use crate::error::AvrowErr;
use integer_encoding::VarIntReader;
use integer_encoding::VarIntWriter;
use std::io::{Error, ErrorKind, Read, Write};
use std::str;

pub(crate) fn decode_string<R: Read>(reader: &mut R) -> Result<String, AvrowErr> {
    let buf = decode_bytes(reader)?;
    let s = str::from_utf8(&buf).map_err(|_e| {
        let err = Error::new(ErrorKind::InvalidData, "Failed decoding string from bytes");
        AvrowErr::DecodeFailed(err)
    })?;
    Ok(s.to_string())
}

pub(crate) fn decode_bytes<R: Read>(reader: &mut R) -> Result<Vec<u8>, AvrowErr> {
    let len: i64 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
    let mut byte_buf = vec![0u8; len as usize];
    reader
        .read_exact(&mut byte_buf)
        .map_err(AvrowErr::DecodeFailed)?;
    Ok(byte_buf)
}

pub fn encode_long<W: Write>(value: i64, writer: &mut W) -> Result<usize, AvrowErr> {
    writer.write_varint(value).map_err(AvrowErr::EncodeFailed)
}

pub fn encode_raw_bytes<W: Write>(value: &[u8], writer: &mut W) -> Result<(), AvrowErr> {
    writer
        .write(value)
        .map_err(AvrowErr::EncodeFailed)
        .map(|_| ())
}
