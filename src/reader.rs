use crate::codec::Codec;
use crate::config::DEFAULT_FLUSH_INTERVAL;
use crate::error;
use crate::schema;
use crate::serde_avro;
use crate::util::{decode_bytes, decode_string};
use crate::value;
use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use error::AvrowErr;
use indexmap::IndexMap;
use integer_encoding::VarIntReader;
use schema::Registry;
use schema::Schema;
use schema::Variant;
use serde::Deserialize;
use serde_avro::SerdeReader;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Cursor;
use std::io::Read;
use std::io::{Error, ErrorKind};
use std::str;
use std::str::FromStr;
use value::{FieldValue, Record, Value};

/// Reader is the primary interface for reading data from an avro datafile.
pub struct Reader<R> {
    source: R,
    header: Header,
    // TODO when reading data call resolve schema https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution
    // This is the schema after it has been resolved using both reader and writer schema
    // NOTE: This is a partially resolved schema
    // schema: Option<ResolvedSchema>,
    // TODO this is for experimental purposes, ideally we can just use references
    reader_schema: Option<Schema>,
    block_buffer: Cursor<Vec<u8>>,
    entries_in_block: u64,
}

impl<R> Reader<R>
where
    R: Read,
{
    /// Creates a Reader from an avro encoded readable buffer.
    pub fn new(mut avro_source: R) -> Result<Self, AvrowErr> {
        let header = Header::from_reader(&mut avro_source)?;
        Ok(Reader {
            source: avro_source,
            header,
            reader_schema: None,
            block_buffer: Cursor::new(vec![0u8; DEFAULT_FLUSH_INTERVAL]),
            entries_in_block: 0,
        })
    }

    /// Create a Reader with the given reader schema and a readable buffer.
    pub fn with_schema(mut source: R, reader_schema: Schema) -> Result<Self, AvrowErr> {
        let header = Header::from_reader(&mut source)?;

        Ok(Reader {
            source,
            header,
            reader_schema: Some(reader_schema),
            block_buffer: Cursor::new(vec![0u8; DEFAULT_FLUSH_INTERVAL]),
            entries_in_block: 0,
        })
    }

    // TODO optimize based on benchmarks
    fn next_block(&mut self) -> Result<(), std::io::Error> {
        // if no more bytes to read, read_varint below returns an EOF
        let entries_in_block: i64 = self.source.read_varint()?;
        self.entries_in_block = entries_in_block as u64;

        let block_stream_len: i64 = self.source.read_varint()?;

        let mut compressed_block = vec![0u8; block_stream_len as usize];
        self.source.read_exact(&mut compressed_block)?;

        self.header
            .codec
            .decode(compressed_block, &mut self.block_buffer)
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("Failed decoding block data with codec, {:?}", e),
                )
            })?;

        // Ready for reading from block
        self.block_buffer.set_position(0);

        let mut sync_marker_buf = [0u8; 16];
        let _ = self.source.read_exact(&mut sync_marker_buf);

        if sync_marker_buf != self.header.sync_marker {
            let err = Error::new(
                ErrorKind::Other,
                "Sync marker does not match as expected while reading",
            );
            return Err(err);
        }

        Ok(())
    }

    /// Retrieves a reference to the header metadata map.
    pub fn meta(&self) -> &HashMap<String, Vec<u8>> {
        self.header.metadata()
    }
}

/// `from_value` is the serde API for deserialization of avro encoded data to native Rust types.
pub fn from_value<'de, D: Deserialize<'de>>(
    value: &'de Result<Value, AvrowErr>,
) -> Result<D, AvrowErr> {
    match value {
        Ok(v) => {
            let mut serde_reader = SerdeReader::new(v);
            D::deserialize(&mut serde_reader)
        }
        Err(e) => Err(AvrowErr::UnexpectedAvroValue {
            value: e.to_string(),
        }),
    }
}

impl<'a, 's, R: Read> Iterator for Reader<R> {
    type Item = Result<Value, AvrowErr>;

    fn next(&mut self) -> Option<Self::Item> {
        // invariant: True on start and end of an avro datafile
        if self.entries_in_block == 0 {
            if let Err(e) = self.next_block() {
                // marks the end of the avro datafile
                if let std::io::ErrorKind::UnexpectedEof = e.kind() {
                    return None;
                } else {
                    return Some(Err(AvrowErr::DecodeFailed(e)));
                }
            }
        }

        let writer_schema = &self.header.schema;
        let w_cxt = &writer_schema.cxt;
        let reader_schema = &self.reader_schema;
        let value = if let Some(r_schema) = reader_schema {
            let r_cxt = &r_schema.cxt;
            decode_with_resolution(
                &r_schema.variant,
                &writer_schema.variant,
                &r_cxt,
                &w_cxt,
                &mut self.block_buffer,
            )
        } else {
            // decode without the reader schema
            decode(&writer_schema.variant, &mut self.block_buffer, &w_cxt)
        };

        self.entries_in_block -= 1;

        if let Err(e) = value {
            return Some(Err(e));
        }

        Some(value)
    }
}

// Reads places priority on reader's schema when passing any schema context if a reader schema is provided.
pub(crate) fn decode_with_resolution<R: Read>(
    r_schema: &Variant,
    w_schema: &Variant,
    r_cxt: &Registry,
    w_cxt: &Registry,
    reader: &mut R,
) -> Result<Value, AvrowErr> {
    // LHS: Writer schema, RHS: Reader schema
    let value = match (w_schema, r_schema) {
        (Variant::Null, Variant::Null) => Value::Null,
        (Variant::Boolean, Variant::Boolean) => {
            let mut buf = [0u8; 1];
            reader
                .read_exact(&mut buf)
                .map_err(AvrowErr::DecodeFailed)?;
            match buf {
                [0x00] => Value::Boolean(false),
                [0x01] => Value::Boolean(true),
                _o => {
                    return Err(AvrowErr::DecodeFailed(Error::new(
                        ErrorKind::InvalidData,
                        "expecting a 0x00 or 0x01 as a byte for boolean value",
                    )))
                }
            }
        }
        (Variant::Int, Variant::Int) => {
            Value::Int(reader.read_varint().map_err(AvrowErr::DecodeFailed)?)
        }
        // int is promotable to long, float, or double (we read as int and cast to promotable.)
        (Variant::Int, Variant::Long) => Value::Long(
            reader
                .read_varint::<i32>()
                .map_err(AvrowErr::DecodeFailed)? as i64,
        ),
        (Variant::Int, Variant::Float) => Value::Float(
            reader
                .read_varint::<i32>()
                .map_err(AvrowErr::DecodeFailed)? as f32,
        ),
        (Variant::Int, Variant::Double) => Value::Double(
            reader
                .read_varint::<i32>()
                .map_err(AvrowErr::DecodeFailed)? as f64,
        ),
        (Variant::Long, Variant::Long) => {
            Value::Long(reader.read_varint().map_err(AvrowErr::DecodeFailed)?)
        }
        // long is promotable to float or double
        (Variant::Long, Variant::Float) => Value::Float(
            reader
                .read_varint::<i64>()
                .map_err(AvrowErr::DecodeFailed)? as f32,
        ),
        (Variant::Long, Variant::Double) => Value::Double(
            reader
                .read_varint::<i64>()
                .map_err(AvrowErr::DecodeFailed)? as f64,
        ),
        (Variant::Float, Variant::Float) => Value::Float(
            reader
                .read_f32::<LittleEndian>()
                .map_err(AvrowErr::DecodeFailed)?,
        ),
        (Variant::Double, Variant::Double) => Value::Double(
            reader
                .read_f64::<LittleEndian>()
                .map_err(AvrowErr::DecodeFailed)?,
        ),
        // float is promotable to double
        (Variant::Float, Variant::Double) => Value::Double(
            reader
                .read_f32::<LittleEndian>()
                .map_err(AvrowErr::DecodeFailed)? as f64,
        ),
        (Variant::Bytes, Variant::Bytes) => Value::Bytes(decode_bytes(reader)?),
        // bytes is promotable to string
        (Variant::Bytes, Variant::Str) => {
            let bytes = decode_bytes(reader)?;
            let s = str::from_utf8(&bytes).map_err(|_e| {
                let err = Error::new(ErrorKind::InvalidData, "failed converting bytes to string");
                AvrowErr::DecodeFailed(err)
            })?;

            Value::Str(s.to_string())
        }
        (Variant::Str, Variant::Str) => {
            let buf = decode_bytes(reader)?;
            let s = str::from_utf8(&buf).map_err(|_e| {
                let err = Error::new(ErrorKind::InvalidData, "failed converting bytes to string");
                AvrowErr::DecodeFailed(err)
            })?;
            Value::Str(s.to_string())
        }
        // string is promotable to bytes
        (Variant::Str, Variant::Bytes) => {
            let buf = decode_bytes(reader)?;
            Value::Bytes(buf)
        }
        (Variant::Array { items: w_items }, Variant::Array { items: r_items }) => {
            if w_items == r_items {
                let block_count: i64 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
                let mut v = Vec::with_capacity(block_count as usize);

                for _ in 0..block_count {
                    let decoded =
                        decode_with_resolution(&*r_items, &*w_items, r_cxt, w_cxt, reader)?;
                    v.push(decoded);
                }

                Value::Array(v)
            } else {
                return Err(AvrowErr::ArrayItemsMismatch);
            }
        }
        // Resolution rules
        // if both are records:
        // * The ordering of fields may be different: fields are matched by name. [1]
        // * Schemas for fields with the same name in both records are resolved recursively. [2]
        // * If the writer's record contains a field with a name not present in the reader's record,
        //   the writer's value for that field is ignored. [3]
        // * If the reader's record schema has a field that contains a default value,
        //   and writer's schema does not have a field with the same name,
        //   then the reader should use the default value from its field. [4]
        // * If the reader's record schema has a field with no default value,
        //   and writer's schema does not have a field with the same name, an error is signalled. [5]
        (
            Variant::Record {
                name: writer_name,
                fields: writer_fields,
                ..
            },
            Variant::Record {
                name: reader_name,
                fields: reader_fields,
                ..
            },
        ) => {
            // [1]
            let reader_name = reader_name.fullname();
            let writer_name = writer_name.fullname();
            if writer_name != reader_name {
                return Err(AvrowErr::RecordNameMismatch);
            }

            let mut rec = Record::new(&reader_name);
            for f in reader_fields {
                let reader_fieldname = f.0.as_str();
                let reader_field = f.1;
                // [3]
                if let Some(wf) = writer_fields.get(reader_fieldname) {
                    // [2]
                    let f_decoded =
                        decode_with_resolution(&reader_field.ty, &wf.ty, r_cxt, w_cxt, reader)?;
                    rec.insert(&reader_fieldname, f_decoded)?;
                } else {
                    // [4]
                    let default_field = f.1;
                    if let Some(a) = &default_field.default {
                        rec.insert(&reader_fieldname, a.clone())?;
                    } else {
                        // [5]
                        return Err(AvrowErr::FieldNotFound);
                    }
                }
            }

            return Ok(Value::Record(rec));
        }
        (
            Variant::Enum {
                name: w_name,
                symbols: w_symbols,
                ..
            },
            Variant::Enum {
                name: r_name,
                symbols: r_symbols,
                ..
            },
        ) => {
            if w_name.fullname() != r_name.fullname() {
                return Err(AvrowErr::EnumNameMismatch);
            }

            let idx: i32 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
            let idx = idx as usize;
            if idx >= w_symbols.len() {
                return Err(AvrowErr::InvalidEnumSymbolIdx(
                    idx,
                    format!("{:?}", w_symbols),
                ));
            }

            let symbol = r_symbols.get(idx as usize);
            if let Some(s) = symbol {
                return Ok(Value::Enum(s.to_string()));
            } else {
                return Err(AvrowErr::EnumSymbolNotFound { idx });
            }
        }
        (
            Variant::Fixed {
                name: w_name,
                size: w_size,
            },
            Variant::Fixed {
                name: r_name,
                size: r_size,
            },
        ) => {
            if w_name.fullname() != r_name.fullname() && w_size != r_size {
                return Err(AvrowErr::FixedSchemaNameMismatch);
            } else {
                let mut fixed = vec![0u8; *r_size];
                reader
                    .read_exact(&mut fixed)
                    .map_err(AvrowErr::DecodeFailed)?;
                Value::Fixed(fixed)
            }
        }
        (
            Variant::Map {
                values: writer_values,
            },
            Variant::Map {
                values: reader_values,
            },
        ) => {
            // here equality will be based
            if writer_values == reader_values {
                let block_count: i32 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
                let mut hm = HashMap::new();
                for _ in 0..block_count {
                    let key = decode_string(reader)?;
                    let value = decode(reader_values, reader, r_cxt)?;
                    hm.insert(key, value);
                }
                Value::Map(hm)
            } else {
                return Err(AvrowErr::MapSchemaMismatch);
            }
        }
        (
            Variant::Union {
                variants: writer_variants,
            },
            Variant::Union {
                variants: reader_variants,
            },
        ) => {
            let union_idx: i32 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
            if let Some(writer_schema) = writer_variants.get(union_idx as usize) {
                for i in reader_variants {
                    if i == writer_schema {
                        return decode(i, reader, r_cxt);
                    }
                }
            }

            return Err(AvrowErr::UnionSchemaMismatch);
        }
        /*
         if reader's is a union but writer's is not. The first schema in the reader's union that matches
         the writer's schema is recursively resolved against it. If none match, an error is signalled.
        */
        (
            writer_schema,
            Variant::Union {
                variants: reader_variants,
            },
        ) => {
            for i in reader_variants {
                if i == writer_schema {
                    return decode(i, reader, r_cxt);
                }
            }

            return Err(AvrowErr::WriterNotInReader);
        }
        /*
         if writer's schema is a union, but reader's is not.
         If the reader's schema matches the selected writer's schema,
         it is recursively resolved against it. If they do not match, an error is signalled.
        */
        (
            Variant::Union {
                variants: writer_variants,
            },
            reader_schema,
        ) => {
            // Read the index value in the schema
            let union_idx: i32 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
            let schema = writer_variants.get(union_idx as usize);
            if let Some(s) = schema {
                if s == reader_schema {
                    return decode(reader_schema, reader, r_cxt);
                }
            }
            let writer_schema = format!("writer schema: {:?}", writer_variants);
            let reader_schema = format!("reader schema: {:?}", reader_schema);
            return Err(AvrowErr::SchemaResolutionFailed(
                reader_schema,
                writer_schema,
            ));
        }
        other => {
            return Err(AvrowErr::SchemaResolutionFailed(
                format!("{:?}", other.0),
                format!("{:?}", other.1),
            ))
        }
    };

    Ok(value)
}

pub(crate) fn decode<R: Read>(
    schema: &Variant,
    reader: &mut R,
    r_cxt: &Registry,
) -> Result<Value, AvrowErr> {
    let value = match schema {
        Variant::Null => Value::Null,
        Variant::Boolean => {
            let mut buf = [0u8; 1];
            reader
                .read_exact(&mut buf)
                .map_err(AvrowErr::DecodeFailed)?;
            match buf {
                [0x00] => Value::Boolean(false),
                [0x01] => Value::Boolean(true),
                _ => {
                    return Err(AvrowErr::DecodeFailed(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid boolean value, expected a 0x00 or a 0x01",
                    )))
                }
            }
        }
        Variant::Int => Value::Int(reader.read_varint().map_err(AvrowErr::DecodeFailed)?),
        Variant::Double => Value::Double(
            reader
                .read_f64::<LittleEndian>()
                .map_err(AvrowErr::DecodeFailed)?,
        ),
        Variant::Long => Value::Long(reader.read_varint().map_err(AvrowErr::DecodeFailed)?),
        Variant::Float => Value::Float(
            reader
                .read_f32::<LittleEndian>()
                .map_err(AvrowErr::DecodeFailed)?,
        ),
        Variant::Str => {
            let buf = decode_bytes(reader)?;
            let s = str::from_utf8(&buf).map_err(|_e| {
                let err = Error::new(
                    ErrorKind::InvalidData,
                    "failed converting from bytes to string",
                );
                AvrowErr::DecodeFailed(err)
            })?;
            Value::Str(s.to_string())
        }
        Variant::Array { items } => {
            let block_count: i64 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;

            if block_count == 0 {
                // FIXME do we send an empty array?
                return Ok(Value::Array(Vec::new()));
            }

            let mut it = Vec::with_capacity(block_count as usize);
            for _ in 0..block_count {
                let decoded = decode(&**items, reader, r_cxt)?;
                it.push(decoded);
            }

            Value::Array(it)
        }
        Variant::Bytes => Value::Bytes(decode_bytes(reader)?),
        Variant::Map { values } => {
            let block_count: usize = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
            let mut hm = HashMap::new();
            for _ in 0..block_count {
                let key = decode_string(reader)?;
                let value = decode(values, reader, r_cxt)?;
                hm.insert(key, value);
            }

            Value::Map(hm)
        }
        Variant::Record { name, fields, .. } => {
            let mut v = IndexMap::with_capacity(fields.len());
            for (field_name, field) in fields {
                let field_name = field_name.to_string();
                let field_value = decode(&field.ty, reader, r_cxt)?;
                let field_value = FieldValue::new(field_value);
                v.insert(field_name, field_value);
            }

            let rec = Record {
                name: name.fullname(),
                fields: v,
            };
            Value::Record(rec)
        }
        Variant::Union { variants } => {
            let variant_idx: i64 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
            decode(&variants[variant_idx as usize], reader, r_cxt)?
        }
        Variant::Named(schema_name) => {
            let schema_variant = r_cxt
                .get(schema_name)
                .ok_or(AvrowErr::NamedSchemaNotFound)?;
            decode(schema_variant, reader, r_cxt)?
        }
        a => {
            return Err(AvrowErr::DecodeFailed(Error::new(
                ErrorKind::InvalidData,
                format!("Read failed for schema {:?}", a),
            )))
        }
    };

    Ok(value)
}

/// Header represents the avro datafile header.
#[derive(Debug)]
pub struct Header {
    /// Writer's schema
    pub(crate) schema: Schema,
    /// A Map which stores avro metadata, like `avro.codec` and `avro.schema`.
    /// Additional key values can be added through the
    /// [WriterBuilder](struct.WriterBuilder.html)'s `set_metadata` method.
    pub(crate) metadata: HashMap<String, Vec<u8>>,
    /// A unique 16 byte sequence for file integrity when writing avro data to file.
    pub(crate) sync_marker: [u8; 16],
    /// codec parsed from the datafile
    pub(crate) codec: Codec,
}

fn decode_header_map<R>(reader: &mut R) -> Result<HashMap<String, Vec<u8>>, AvrowErr>
where
    R: Read,
{
    let count: i64 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;
    let count = count as usize;
    let mut map = HashMap::with_capacity(count);

    for _ in 0..count {
        let key = decode_string(reader)?;
        let val = decode_bytes(reader)?;
        map.insert(key, val);
    }

    let _map_end: i64 = reader.read_varint().map_err(AvrowErr::DecodeFailed)?;

    Ok(map)
}

impl Header {
    /// Reads the header from an avro datafile
    pub fn from_reader<R: Read>(reader: &mut R) -> Result<Self, AvrowErr> {
        let mut magic_buf = [0u8; 4];
        reader
            .read_exact(&mut magic_buf[..])
            .map_err(|_| AvrowErr::HeaderDecodeFailed)?;

        if &magic_buf != b"Obj\x01" {
            return Err(AvrowErr::InvalidDataFile);
        }

        let map = decode_header_map(reader)?;

        let mut sync_marker = [0u8; 16];
        let _ = reader
            .read_exact(&mut sync_marker)
            .map_err(|_| AvrowErr::HeaderDecodeFailed)?;

        let schema_bytes = map.get("avro.schema").ok_or(AvrowErr::HeaderDecodeFailed)?;

        let schema = str::from_utf8(schema_bytes)
            .map(Schema::from_str)
            .map_err(|_| AvrowErr::HeaderDecodeFailed)??;

        let codec = if let Some(c) = map.get("avro.codec") {
            match std::str::from_utf8(c) {
                Ok(s) => Codec::try_from(s)?,
                Err(s) => return Err(AvrowErr::UnsupportedCodec(s.to_string())),
            }
        } else {
            Codec::Null
        };

        let header = Header {
            schema,
            metadata: map,
            sync_marker,
            codec,
        };

        Ok(header)
    }

    /// Returns a reference to metadata from avro datafile header
    pub fn metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.metadata
    }

    /// Returns a reference to the writer's schema in this header
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use crate::Reader;
    #[test]
    fn has_required_headers() {
        let data = vec![
            79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 32, 123, 34,
            116, 121, 112, 101, 34, 58, 34, 98, 121, 116, 101, 115, 34, 125, 20, 97, 118, 114, 111,
            46, 99, 111, 100, 101, 99, 14, 100, 101, 102, 108, 97, 116, 101, 0, 145, 85, 112, 15,
            87, 201, 208, 26, 183, 148, 48, 236, 212, 250, 38, 208, 2, 18, 227, 97, 96, 100, 98,
            102, 97, 5, 0, 145, 85, 112, 15, 87, 201, 208, 26, 183, 148, 48, 236, 212, 250, 38,
            208,
        ];

        let reader = Reader::new(data.as_slice()).unwrap();
        assert!(reader.meta().contains_key("avro.codec"));
        assert!(reader.meta().contains_key("avro.schema"));
    }
}
