#![allow(dead_code)]

use avrow::Codec;
use avrow::Schema;
use avrow::{Reader, Writer};
use std::io::Cursor;
use std::str::FromStr;

#[derive(Debug)]
pub(crate) enum Primitive {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
}

impl std::fmt::Display for Primitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Primitive::*;
        let str_repr = match self {
            Null => "null",
            Boolean => "boolean",
            Int => "int",
            Long => "long",
            Float => "float",
            Double => "double",
            Bytes => "bytes",
            String => "string",
        };
        write!(f, "{}", str_repr)
    }
}

pub(crate) fn writer_from_schema<'a>(schema: &'a Schema, codec: Codec) -> Writer<'a, Vec<u8>> {
    let writer = Writer::with_codec(&schema, vec![], codec).unwrap();
    writer
}

pub(crate) fn reader_with_schema<'a>(schema: &Schema, buffer: Vec<u8>) -> Reader<Cursor<Vec<u8>>> {
    let reader = Reader::with_schema(Cursor::new(buffer), schema).unwrap();
    reader
}

#[allow(dead_code)]
pub(crate) fn to_file(path: &str, buffer: &[u8]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .unwrap();
    f.write_all(&buffer).unwrap();
}

pub(crate) struct MockSchema;
impl MockSchema {
    // creates a primitive schema
    pub fn prim(self, ty: &str) -> Schema {
        let schema_str = format!("{{\"type\": \"{}\"}}", ty);
        Schema::from_str(&schema_str).unwrap()
    }

    pub fn record(self) -> Schema {
        Schema::from_str(
            r#"
        {
            "type": "record",
            "name": "LongList",
            "aliases": ["LinkedLongs"],
            "fields" : [
              {"name": "value", "type": "long"},
              {"name": "next", "type": ["null", "LongList"]}
            ]
        }
        "#,
        )
        .unwrap()
    }

    pub fn record_default(self) -> Schema {
        Schema::from_str(
            r#"
        {
            "type": "record",
            "name": "LongList",
            "aliases": ["LinkedLongs"],
            "fields" : [
              {"name": "value", "type": "long"},
              {"name": "next", "type": ["null", "LongList"]},
              {"name": "other", "type":"long", "default": 1}
            ]
        }
        "#,
        )
        .unwrap()
    }
}
