/// Tests for schema resolution
mod common;

use serde::{Deserialize, Serialize};

use avrow::{from_value, Codec, Reader, Schema, Value};
use std::collections::HashMap;
use std::str::FromStr;

use common::{reader_with_schema, writer_from_schema, MockSchema};

#[test]
#[should_panic]
fn null_fails_with_other_primitive_schema() {
    let name = "null";
    let schema = MockSchema.prim(name);
    let mut writer = writer_from_schema(&schema, Codec::Null);
    writer.serialize(()).unwrap();
    writer.flush().unwrap();

    let buf = writer.into_inner().unwrap();

    let reader_schema = MockSchema.prim("boolean");
    let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();

    for i in reader {
        let _ = i.unwrap();
    }
}

#[test]
fn writer_to_reader_promotion_primitives() {
    // int -> long, float, double
    for reader_schema in &["long", "float", "double"] {
        let name = "int";
        let schema = MockSchema.prim(name);
        let mut writer = writer_from_schema(&schema, Codec::Null);
        writer.serialize(1024).unwrap();
        writer.flush().unwrap();

        let buf = writer.into_inner().unwrap();

        let reader_schema = MockSchema.prim(reader_schema);
        let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();
        for i in reader {
            assert!(i.is_ok());
            let _a = i.unwrap();
        }
    }

    // long -> float, double
    for reader_schema in &["float", "double"] {
        let name = "long";
        let schema = MockSchema.prim(name);
        let mut writer = writer_from_schema(&schema, Codec::Null);
        writer.serialize(1024i64).unwrap();
        writer.flush().unwrap();

        let buf = writer.into_inner().unwrap();

        let reader_schema = MockSchema.prim(reader_schema);
        let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();
        for i in reader {
            assert!(i.is_ok());
        }
    }

    // float -> double
    for reader_schema in &["double"] {
        let name = "float";
        let schema = MockSchema.prim(name);
        let mut writer = writer_from_schema(&schema, Codec::Null);
        writer.serialize(1026f32).unwrap();
        writer.flush().unwrap();

        let buf = writer.into_inner().unwrap();

        let reader_schema = MockSchema.prim(reader_schema);
        let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();
        for i in reader {
            assert!(i.is_ok());
        }
    }

    // string -> bytes
    for reader_schema in &["bytes"] {
        let name = "string";
        let schema = MockSchema.prim(name);
        let mut writer = writer_from_schema(&schema, Codec::Null);
        writer.serialize("hello").unwrap();
        writer.flush().unwrap();

        let buf = writer.into_inner().unwrap();

        let reader_schema = MockSchema.prim(reader_schema);
        let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();
        for i in reader {
            assert!(i.is_ok());
            let a = i.unwrap();
            assert_eq!(Value::Bytes(vec![104, 101, 108, 108, 111]), a);
        }
    }

    // bytes -> string
    for reader_schema in &["string"] {
        let name = "bytes";
        let schema = MockSchema.prim(name);
        let mut writer = writer_from_schema(&schema, Codec::Null);
        writer.serialize([104u8, 101, 108, 108, 111]).unwrap();
        writer.flush().unwrap();

        let buf = writer.into_inner().unwrap();

        let reader_schema = MockSchema.prim(reader_schema);
        let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();
        for i in reader {
            assert!(i.is_ok());
            let a = i.unwrap();
            assert_eq!(Value::Str("hello".to_string()), a);
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Foo {
    A,
    B,
    C,
    E,
}

#[test]
#[should_panic]
fn enum_fails_schema_resolution() {
    let schema =
        Schema::from_str(r##"{"type": "enum", "name": "Foo", "symbols": ["A", "B", "C", "D"] }"##)
            .unwrap();
    let mut writer = writer_from_schema(&schema, Codec::Null);
    writer.serialize(Foo::B).unwrap();
    writer.flush().unwrap();

    let buf = writer.into_inner().unwrap();

    // Reading a symbol which does not exist in writer's schema should fail
    let reader_schema =
        Schema::from_str(r##"{"type": "enum", "name": "Foo", "symbols": ["F"] }"##).unwrap();
    let reader = Reader::with_schema(buf.as_slice(), reader_schema).unwrap();

    // let reader = reader_with_schema(reader_schema, name);
    for i in reader {
        i.unwrap();
    }
}

#[test]
#[should_panic]
fn schema_resolution_map() {
    let schema = Schema::from_str(r##"{"type": "map", "values": "string"}"##).unwrap();
    let mut writer = writer_from_schema(&schema, Codec::Null);
    let mut m = HashMap::new();
    m.insert("1", "b");
    writer.serialize(m).unwrap();
    writer.flush().unwrap();

    let buf = writer.into_inner().unwrap();

    // // Reading a symbol which does not exist in writer's schema should fail
    let reader_schema = Schema::from_str(r##"{"type": "map", "values": "int"}"##).unwrap();

    let reader = reader_with_schema(reader_schema, buf);
    for i in reader {
        let _ = i.unwrap();
    }
}

#[derive(Serialize, Deserialize)]
struct LongList {
    value: i64,
    next: Option<Box<LongList>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct LongListDefault {
    value: i64,
    next: Option<Box<LongListDefault>>,
    other: i64,
}

#[test]
fn record_schema_resolution_with_default_value() {
    let schema = MockSchema.record();
    let mut writer = writer_from_schema(&schema, Codec::Null);
    let list = LongList {
        value: 1,
        next: None,
    };

    writer.serialize(list).unwrap();

    let buf = writer.into_inner().unwrap();

    let schema = MockSchema.record_default();
    let reader = reader_with_schema(schema, buf);
    for i in reader {
        let rec: Result<LongListDefault, _> = from_value(&i);
        assert!(rec.is_ok());
    }
}

#[test]
#[cfg(feature = "codec")]
fn writer_is_a_union_but_reader_is_not() {
    let writer_schema = Schema::from_str(r##"["null", "int"]"##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Deflate);
    writer.serialize(()).unwrap();
    writer.serialize(3).unwrap();

    let buf = writer.into_inner().unwrap();

    let schema_str = r##""int""##;
    let reader_schema = Schema::from_str(schema_str).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().unwrap().is_ok());
}

#[test]
fn reader_is_a_union_but_writer_is_not() {
    let writer_schema = Schema::from_str(r##""int""##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Null);
    writer.serialize(3).unwrap();

    let buf = writer.into_inner().unwrap();

    // err
    let reader_schema = Schema::from_str(r##"["null", "string"]"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf.clone());
    assert!(reader.next().unwrap().is_err());

    // ok
    let reader_schema = Schema::from_str(r##"["null", "int"]"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);
    assert!(reader.next().unwrap().is_ok());
}

#[test]
fn both_are_unions_but_different() {
    let writer_schema = Schema::from_str(r##"["null", "int"]"##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Null);
    writer.serialize(3).unwrap();

    let buf = writer.into_inner().unwrap();

    let reader_schema = Schema::from_str(r##"["boolean", "string"]"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);

    // err
    assert!(reader.next().unwrap().is_err());
}

#[test]
fn both_are_map() {
    let writer_schema = Schema::from_str(r##"{"type": "map", "values": "string"}"##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Null);
    let mut map = HashMap::new();
    map.insert("hello", "world");
    writer.serialize(map).unwrap();

    let buf = writer.into_inner().unwrap();

    // let reader_schema =
    //     Schema::from_str(r##"["boolean", {"type":"map", "values": "string"}]"##).unwrap();
    let reader_schema = Schema::from_str(r##"{"type": "map", "values": "string"}"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);
    assert!(reader.next().unwrap().is_ok());
}

#[test]
fn both_are_arrays() {
    let writer_schema = Schema::from_str(r##"{"type": "array", "items": "int"}"##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Null);
    writer.serialize(vec![1, 2, 3]).unwrap();

    let buf = writer.into_inner().unwrap();

    let reader_schema = Schema::from_str(r##"{"type": "array", "items": "int"}"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);
    assert!(reader.next().unwrap().is_ok());
}

#[test]
fn both_are_enums() {
    let writer_schema = Schema::from_str(r##"{"type": "array", "items": "int"}"##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Null);
    writer.serialize(vec![1, 2, 3]).unwrap();

    let buf = writer.into_inner().unwrap();

    let reader_schema = Schema::from_str(r##"{"type": "array", "items": "int"}"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);
    assert!(reader.next().unwrap().is_ok());
}

#[test]
fn null() {
    let writer_schema = Schema::from_str(r##"{"type": "null"}"##).unwrap();
    let mut writer = writer_from_schema(&writer_schema, Codec::Null);
    writer.serialize(()).unwrap();

    let buf = writer.into_inner().unwrap();

    let reader_schema = Schema::from_str(r##"{"type": "null"}"##).unwrap();
    let mut reader = reader_with_schema(reader_schema, buf);
    assert!(reader.next().unwrap().is_ok());
}
