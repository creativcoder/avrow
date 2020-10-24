//! The Writer is the primary interface for writing values in avro encoded format.

use crate::codec::Codec;
use crate::config::{DEFAULT_FLUSH_INTERVAL, MAGIC_BYTES, SYNC_MARKER_SIZE};
use crate::error::{AvrowErr, AvrowResult};
use crate::schema::Registry;
use crate::schema::Schema;
use crate::schema::Variant;
use crate::serde_avro;
use crate::util::{encode_long, encode_raw_bytes};
use crate::value::Map;
use crate::value::Value;
use rand::{thread_rng, Rng};
use serde::Serialize;
use std::collections::HashMap;
use std::default::Default;
use std::io::Write;

fn sync_marker() -> [u8; SYNC_MARKER_SIZE] {
    let mut vec = [0u8; SYNC_MARKER_SIZE];
    thread_rng().fill_bytes(&mut vec[..]);
    vec
}

/// Convenient builder struct for configuring and instantiating a Writer.
pub struct WriterBuilder<'a, W> {
    metadata: HashMap<String, Value>,
    codec: Codec,
    schema: Option<&'a Schema>,
    datafile: Option<W>,
    flush_interval: usize,
}

impl<'a, W: Write> WriterBuilder<'a, W> {
    /// Creates a builder instance to construct a Writer.
    pub fn new() -> Self {
        WriterBuilder {
            metadata: Default::default(),
            codec: Codec::Null,
            schema: None,
            datafile: None,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }

    /// Set any custom metadata for the datafile.
    pub fn set_metadata(mut self, k: &str, v: &str) -> Self {
        self.metadata
            .insert(k.to_string(), Value::Bytes(v.as_bytes().to_vec()));
        self
    }

    /// Set one of the available codecs. This requires the respective feature flags to be enabled.
    pub fn set_codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    /// Provide the writer with a reference to the schema file.
    pub fn set_schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the underlying output stream. This can be any type that implements the `Write` trait.
    pub fn set_datafile(mut self, w: W) -> Self {
        self.datafile = Some(w);
        self
    }

    /// Set the flush interval (in bytes) for the internal buffer. It's the amount of bytes post which
    /// the internal buffer is written to the underlying datafile or output stream..
    /// Defaults to [`DEFAULT_FLUSH_INTERVAL`](config/constant.DEFAULT_FLUSH_INTERVAL.html).
    pub fn set_flush_interval(mut self, interval: usize) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Builds the `Writer` instance consuming this builder.
    pub fn build(self) -> AvrowResult<Writer<'a, W>> {
        let mut writer = Writer {
            out_stream: self.datafile.ok_or(AvrowErr::WriterBuildFailed)?,
            schema: self.schema.ok_or(AvrowErr::WriterBuildFailed)?,
            block_stream: Vec::with_capacity(self.flush_interval),
            block_count: 0,
            codec: self.codec,
            sync_marker: sync_marker(),
            flush_interval: self.flush_interval,
        };
        writer.encode_custom_header(self.metadata)?;
        Ok(writer)
    }
}

impl<'a, W: Write> Default for WriterBuilder<'a, W> {
    fn default() -> Self {
        Self::new()
    }
}

/// The Writer is the primary interface for writing values to an avro datafile or a byte container (say a `Vec<u8>`).
/// It takes a reference to the schema for validating the values being written
/// and an output stream `W` which can be any type
/// implementing the [Write](https://doc.rust-lang.org/std/io/trait.Write.html) trait.
pub struct Writer<'a, W> {
    out_stream: W,
    schema: &'a Schema,
    block_stream: Vec<u8>,
    block_count: usize,
    codec: Codec,
    sync_marker: [u8; 16],
    flush_interval: usize,
}

impl<'a, W: Write> Writer<'a, W> {
    /// Creates a new avro `Writer` instance taking a reference to a `Schema`
    /// and a type implementing [`Write`](https://doc.rust-lang.org/std/io/trait.Write.html).
    pub fn new(schema: &'a Schema, out_stream: W) -> AvrowResult<Self> {
        let mut writer = Writer {
            out_stream,
            schema,
            block_stream: Vec::with_capacity(DEFAULT_FLUSH_INTERVAL),
            block_count: 0,
            codec: Codec::Null,
            sync_marker: sync_marker(),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        };
        writer.encode_header()?;
        Ok(writer)
    }

    /// Same as the `new` method, but additionally takes a `Codec` as parameter.
    /// Codecs can be used to compress the encoded data being written in an avro datafile.
    /// Supported codecs as per spec are:
    /// * null (default): No compression is applied.
    /// * [snappy](https://en.wikipedia.org/wiki/Snappy_(compression)) (`--features snappy`)
    /// * [deflate](https://en.wikipedia.org/wiki/DEFLATE) (`--features deflate`)
    /// * [zstd](https://facebook.github.io/zstd/) compression (`--feature zstd`)
    /// * [bzip](http://www.bzip.org/) compression (`--feature bzip`)
    /// * [xz](https://tukaani.org/xz/) compression (`--features xz`)
    pub fn with_codec(schema: &'a Schema, out_stream: W, codec: Codec) -> AvrowResult<Self> {
        let mut writer = Writer {
            out_stream,
            schema,
            block_stream: Vec::with_capacity(DEFAULT_FLUSH_INTERVAL),
            block_count: 0,
            codec,
            sync_marker: sync_marker(),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        };
        writer.encode_header()?;
        Ok(writer)
    }

    /// Appends a value to the buffer.
    /// Before a value gets written, it gets validated with the schema referenced
    /// by this writer.
    ///
    /// # Note:
    /// writes are buffered internally as per the flush interval (for performance) and the underlying
    /// buffer may not reflect values immediately.
    /// Call [`flush`](struct.Writer.html#method.flush) to explicitly write all buffered data.
    /// Alternatively calling [`into_inner`](struct.Writer.html#method.into_inner) on the writer
    /// guarantees that flush will happen and will hand over
    /// the underlying buffer with all data written.
    pub fn write<T: Into<Value>>(&mut self, value: T) -> AvrowResult<()> {
        let val: Value = value.into();
        self.schema.validate(&val)?;

        val.encode(
            &mut self.block_stream,
            &self.schema.variant(),
            &self.schema.cxt,
        )?;
        self.block_count += 1;

        if self.block_stream.len() >= self.flush_interval {
            self.flush()?;
        }

        Ok(())
    }

    /// Appends a native Rust value to the buffer. The value must implement Serde's `Serialize` trait.
    pub fn serialize<T: Serialize>(&mut self, value: T) -> AvrowResult<()> {
        let value = serde_avro::to_value(&value)?;
        self.write(value)?;
        Ok(())
    }

    fn reset_block_buffer(&mut self) {
        self.block_count = 0;
        self.block_stream.clear();
    }

    /// Sync/flush any buffered data to the underlying buffer.
    pub fn flush(&mut self) -> AvrowResult<()> {
        // bail if no data is written or it has already been flushed before
        if self.block_count == 0 {
            return Ok(());
        }
        // encode datum count
        encode_long(self.block_count as i64, &mut self.out_stream)?;
        // encode with codec
        self.codec
            .encode(&mut self.block_stream, &mut self.out_stream)?;
        // Write sync marker
        encode_raw_bytes(&self.sync_marker, &mut self.out_stream)?;
        // Reset block buffer
        self.out_stream.flush().map_err(AvrowErr::EncodeFailed)?;
        self.reset_block_buffer();
        Ok(())
    }

    // Used via WriterBuilder
    fn encode_custom_header(&mut self, mut map: HashMap<String, Value>) -> AvrowResult<()> {
        self.out_stream
            .write(MAGIC_BYTES)
            .map_err(AvrowErr::EncodeFailed)?;
        map.insert("avro.schema".to_string(), self.schema.as_bytes().into());
        let codec_str = self.codec.as_ref().as_bytes();
        map.insert("avro.codec".to_string(), codec_str.into());
        let meta_schema = &Variant::Map {
            values: Box::new(Variant::Bytes),
        };

        Value::Map(map).encode(&mut self.out_stream, meta_schema, &Registry::new())?;
        encode_raw_bytes(&self.sync_marker, &mut self.out_stream)?;
        Ok(())
    }

    fn encode_header(&mut self) -> AvrowResult<()> {
        self.out_stream
            .write(MAGIC_BYTES)
            .map_err(AvrowErr::EncodeFailed)?;
        // encode metadata
        let mut metamap = Map::with_capacity(2);
        metamap.insert("avro.schema".to_string(), self.schema.as_bytes().into());
        let codec_str = self.codec.as_ref().as_bytes();
        metamap.insert("avro.codec".to_string(), codec_str.into());
        let meta_schema = &Variant::Map {
            values: Box::new(Variant::Bytes),
        };

        Value::Map(metamap).encode(&mut self.out_stream, meta_schema, &Registry::new())?;
        encode_raw_bytes(&self.sync_marker, &mut self.out_stream)?;
        Ok(())
    }

    /// Consumes self and yields the inner `Write` instance.
    /// Additionally calls `flush` if no flush has happened before this call.
    pub fn into_inner(mut self) -> AvrowResult<W> {
        self.flush()?;
        Ok(self.out_stream)
    }
}

#[cfg(test)]
mod tests {
    use crate::{from_value, Codec, Reader, Schema, Writer, WriterBuilder};
    use std::io::Cursor;
    use std::str::FromStr;

    #[test]
    fn header_written_on_writer_creation() {
        let schema = Schema::from_str(r##""null""##).unwrap();
        let v = Cursor::new(vec![]);
        let writer = Writer::new(&schema, v).unwrap();
        let buf = writer.into_inner().unwrap().into_inner();
        // writer.
        let slice = &buf[0..4];

        assert_eq!(slice[0], b'O');
        assert_eq!(slice[1], b'b');
        assert_eq!(slice[2], b'j');
        assert_eq!(slice[3], 1);
    }

    #[test]
    fn writer_with_builder() {
        let schema = Schema::from_str(r##""null""##).unwrap();
        let v = vec![];
        let mut writer = WriterBuilder::new()
            .set_codec(Codec::Null)
            .set_schema(&schema)
            .set_datafile(v)
            .set_flush_interval(128_000)
            .build()
            .unwrap();
        writer.serialize(()).unwrap();
        let _v = writer.into_inner().unwrap();

        let reader = Reader::with_schema(_v.as_slice(), &schema).unwrap();
        for i in reader {
            let _: () = from_value(&i).unwrap();
        }
    }

    #[test]
    fn custom_metadata_header() {
        let schema = Schema::from_str(r##""null""##).unwrap();
        let v = vec![];
        let mut writer = WriterBuilder::new()
            .set_codec(Codec::Null)
            .set_schema(&schema)
            .set_datafile(v)
            .set_flush_interval(128_000)
            .set_metadata("hello", "world")
            .build()
            .unwrap();
        writer.serialize(()).unwrap();
        let _v = writer.into_inner().unwrap();

        let reader = Reader::with_schema(_v.as_slice(), &schema).unwrap();
        assert!(reader.meta().contains_key("hello"));
    }
}
