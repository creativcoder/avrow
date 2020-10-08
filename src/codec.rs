use crate::error::AvrowErr;
use crate::util::{encode_long, encode_raw_bytes};

use std::io::Write;

// Given a slice of bytes, generates a CRC for it
#[cfg(feature = "snappy")]
pub fn get_crc_uncompressed(pre_comp_buf: &[u8]) -> Result<Vec<u8>, AvrowErr> {
    use byteorder::{BigEndian, WriteBytesExt};
    use crc::crc32;

    let crc_checksum = crc32::checksum_ieee(pre_comp_buf);
    let mut checksum_bytes = Vec::with_capacity(1);
    let _ = checksum_bytes
        .write_u32::<BigEndian>(crc_checksum)
        .map_err(|_| {
            let err: AvrowErr = AvrowErr::CRCGenFailed;
            err
        })?;
    Ok(checksum_bytes)
}

/// Given a uncompressed slice of bytes, returns a compresed Vector of bytes using the snappy codec
#[cfg(feature = "snappy")]
pub(crate) fn compress_snappy(uncompressed_buffer: &[u8]) -> Result<Vec<u8>, AvrowErr> {
    let mut encoder = snap::Encoder::new();
    encoder
        .compress_vec(uncompressed_buffer)
        .map_err(|e| AvrowErr::DecodeFailed(e.into()))
}

#[cfg(feature = "deflate")]
pub fn compress_deflate(uncompressed_buffer: &[u8]) -> Result<Vec<u8>, AvrowErr> {
    use flate2::{write::DeflateEncoder, Compression};

    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder
        .write(uncompressed_buffer)
        .map_err(AvrowErr::EncodeFailed)?;
    encoder.finish().map_err(AvrowErr::EncodeFailed)
}

#[cfg(feature = "zstd")]
pub(crate) fn zstd_compress(level: i32, uncompressed_buffer: &[u8]) -> Result<Vec<u8>, AvrowErr> {
    let comp = zstdd::encode_all(std::io::Cursor::new(uncompressed_buffer), level)
        .map_err(AvrowErr::EncodeFailed)?;
    Ok(comp)
}

#[cfg(feature = "deflate")]
pub fn decompress_deflate(
    compressed_buffer: &[u8],
    uncompressed: &mut Vec<u8>,
) -> Result<(), AvrowErr> {
    use flate2::bufread::DeflateDecoder;
    use std::io::Read;

    let mut decoder = DeflateDecoder::new(compressed_buffer);
    uncompressed.clear();
    decoder
        .read_to_end(uncompressed)
        .map_err(AvrowErr::DecodeFailed)?;
    Ok(())
}

#[cfg(feature = "snappy")]
pub(crate) fn decompress_snappy(
    compressed_buffer: &[u8],
    uncompressed: &mut Vec<u8>,
) -> Result<(), AvrowErr> {
    use byteorder::ByteOrder;

    let data_minus_cksum = &compressed_buffer[..compressed_buffer.len() - 4];
    let decompressed_size =
        snap::decompress_len(data_minus_cksum).map_err(|e| AvrowErr::DecodeFailed(e.into()))?;
    uncompressed.resize(decompressed_size, 0);
    snap::Decoder::new()
        .decompress(data_minus_cksum, &mut uncompressed[..])
        .map_err(|e| AvrowErr::DecodeFailed(e.into()))?;

    let expected =
        byteorder::BigEndian::read_u32(&compressed_buffer[compressed_buffer.len() - 4..]);
    let found = crc::crc32::checksum_ieee(&uncompressed);
    if expected != found {
        return Err(AvrowErr::CRCMismatch { found, expected });
    }
    Ok(())
}

#[cfg(feature = "zstd")]
pub(crate) fn decompress_zstd(
    compressed_buffer: &[u8],
    uncompressed: &mut Vec<u8>,
) -> Result<(), AvrowErr> {
    let mut decoder = zstdd::Decoder::new(compressed_buffer).map_err(AvrowErr::DecodeFailed)?;
    std::io::copy(&mut decoder, uncompressed).map_err(AvrowErr::DecodeFailed)?;
    Ok(())
}

#[cfg(feature = "bzip2")]
pub(crate) fn decompress_bzip2(
    compressed_buffer: &[u8],
    uncompressed: &mut Vec<u8>,
) -> Result<(), AvrowErr> {
    use bzip2::read::BzDecoder;
    let decompressor = BzDecoder::new(compressed_buffer);
    let mut buf = decompressor.into_inner();
    std::io::copy(&mut buf, uncompressed).map_err(AvrowErr::DecodeFailed)?;
    Ok(())
}

#[cfg(feature = "xz")]
pub(crate) fn decompress_xz(
    compressed_buffer: &[u8],
    uncompressed: &mut Vec<u8>,
) -> Result<(), AvrowErr> {
    use xz2::read::XzDecoder;
    let decompressor = XzDecoder::new(compressed_buffer);
    let mut buf = decompressor.into_inner();
    std::io::copy(&mut buf, uncompressed).map_err(AvrowErr::DecodeFailed)?;
    Ok(())
}
/// Defines codecs one can use when writing avro data.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Codec {
    /// The Null codec. When no codec is specified at the time of Writer creation, null is the default.
    Null,
    #[cfg(feature = "deflate")]
    /// The Deflate codec. <br>Uses https://docs.rs/flate2 as the underlying implementation.
    Deflate,
    #[cfg(feature = "snappy")]
    /// The Snappy codec. <br>Uses https://docs.rs/snap as the underlying implementation.
    Snappy,
    #[cfg(feature = "zstd")]
    /// The Zstd codec. <br>Uses https://docs.rs/zstd as the underlying implementation.
    Zstd,
    #[cfg(feature = "bzip2")]
    /// The Bzip2 codec. <br>Uses https://docs.rs/bzip2 as the underlying implementation.
    Bzip2,
    #[cfg(feature = "xz")]
    /// The Xz codec. <br>Uses https://docs.rs/crate/xz2 as the underlying implementation.
    Xz,
}

impl AsRef<str> for Codec {
    fn as_ref(&self) -> &str {
        match self {
            Codec::Null => "null",
            #[cfg(feature = "deflate")]
            Codec::Deflate => "deflate",
            #[cfg(feature = "snappy")]
            Codec::Snappy => "snappy",
            #[cfg(feature = "zstd")]
            Codec::Zstd => "zstd",
            #[cfg(feature = "bzip2")]
            Codec::Bzip2 => "bzip2",
            #[cfg(feature = "xz")]
            Codec::Xz => "xz",
        }
    }
}

// TODO allow all of these to be configurable for setting compression ratio/level
impl Codec {
    pub(crate) fn encode<W: Write>(
        &self,
        block_stream: &mut [u8],
        out_stream: &mut W,
    ) -> Result<(), AvrowErr> {
        match self {
            Codec::Null => {
                // encode size of data in block
                encode_long(block_stream.len() as i64, out_stream)?;
                // encode actual data bytes
                encode_raw_bytes(&block_stream, out_stream)?;
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                let checksum_bytes = get_crc_uncompressed(&block_stream)?;
                let compressed_data = compress_snappy(&block_stream)?;
                encode_long(
                    compressed_data.len() as i64 + crate::config::CRC_CHECKSUM_LEN as i64,
                    out_stream,
                )?;

                out_stream
                    .write(&*compressed_data)
                    .map_err(AvrowErr::EncodeFailed)?;
                out_stream
                    .write(&*checksum_bytes)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            #[cfg(feature = "deflate")]
            Codec::Deflate => {
                let compressed_data = compress_deflate(block_stream)?;
                encode_long(compressed_data.len() as i64, out_stream)?;
                encode_raw_bytes(&*compressed_data, out_stream)?;
            }
            #[cfg(feature = "zstd")]
            Codec::Zstd => {
                let compressed_data = zstd_compress(0, block_stream)?;
                encode_long(compressed_data.len() as i64, out_stream)?;
                encode_raw_bytes(&*compressed_data, out_stream)?;
            }
            #[cfg(feature = "bzip2")]
            Codec::Bzip2 => {
                use bzip2::read::BzEncoder;
                use bzip2::Compression;
                use std::io::Cursor;
                let compressor = BzEncoder::new(Cursor::new(block_stream), Compression::new(5));
                let vec = compressor.into_inner().into_inner();

                encode_long(vec.len() as i64, out_stream)?;
                encode_raw_bytes(&*vec, out_stream)?;
            }
            #[cfg(feature = "xz")]
            Codec::Xz => {
                use std::io::Cursor;
                use xz2::read::XzEncoder;
                let compressor = XzEncoder::new(Cursor::new(block_stream), 6);
                let vec = compressor.into_inner().into_inner();

                encode_long(vec.len() as i64, out_stream)?;
                encode_raw_bytes(&*vec, out_stream)?;
            }
        }
        Ok(())
    }

    pub(crate) fn decode(
        &self,
        compressed: Vec<u8>,
        uncompressed: &mut std::io::Cursor<Vec<u8>>,
    ) -> Result<(), AvrowErr> {
        match self {
            Codec::Null => {
                *uncompressed = std::io::Cursor::new(compressed);
                Ok(())
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => decompress_snappy(&compressed, uncompressed.get_mut()),
            #[cfg(feature = "deflate")]
            Codec::Deflate => decompress_deflate(&compressed, uncompressed.get_mut()),
            #[cfg(feature = "zstd")]
            Codec::Zstd => decompress_zstd(&compressed, uncompressed.get_mut()),
            #[cfg(feature = "bzip2")]
            Codec::Bzip2 => decompress_bzip2(&compressed, uncompressed.get_mut()),
            #[cfg(feature = "xz")]
            Codec::Xz => decompress_xz(&compressed, uncompressed.get_mut()),
        }
    }
}

impl std::convert::TryFrom<&str> for Codec {
    type Error = AvrowErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "null" => Ok(Codec::Null),
            #[cfg(feature = "snappy")]
            "snappy" => Ok(Codec::Snappy),
            #[cfg(feature = "deflate")]
            "deflate" => Ok(Codec::Deflate),
            #[cfg(feature = "zstd")]
            "zstd" => Ok(Codec::Zstd),
            #[cfg(feature = "bzip2")]
            "bzip2" => Ok(Codec::Bzip2),
            #[cfg(feature = "bzip2")]
            "xz" => Ok(Codec::Xz),
            o => Err(AvrowErr::UnsupportedCodec(o.to_string())),
        }
    }
}
