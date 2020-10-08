extern crate pretty_env_logger;
extern crate serde_derive;

mod common;

use avrow::{from_value, Reader, Schema, Codec, Value};
use std::str::FromStr;
use crate::common::{MockSchema, writer_from_schema};
use std::collections::HashMap;


use common::{Primitive};
use serde_derive::{Deserialize, Serialize};

const DATUM_COUNT: usize = 10000;

///////////////////////////////////////////////////////////////////////////////
/// Primitive schema tests
///////////////////////////////////////////////////////////////////////////////

// #[cfg(feature = "codec")]
static PRIMITIVES: [Primitive; 8] = [
    Primitive::Null,
    Primitive::Boolean,
    Primitive::Int,
    Primitive::Long,
    Primitive::Float,
    Primitive::Double,
    Primitive::Bytes,
    Primitive::String,
];

// static PRIMITIVES: [Primitive; 1] = [Primitive::Int];

#[cfg(feature = "codec")]
const CODECS: [Codec; 6] = [
    Codec::Null,
    Codec::Deflate,
    Codec::Snappy,
    Codec::Zstd,
    Codec::Bzip2,
    Codec::Xz,
];

// #[cfg(feature = "bzip2")]
// const CODECS: [Codec; 1] = [Codec::Bzip2];

#[test]
#[cfg(feature = "codec")]
fn read_write_primitive() {
    for codec in CODECS.iter() {
        for primitive in PRIMITIVES.iter() {
            // write
            let name = &format!("{}", primitive);
            let schema = MockSchema.prim(name);
            let mut writer = writer_from_schema(&schema, *codec);
            (0..DATUM_COUNT).for_each(|i| match primitive {
                Primitive::Null => {
                    writer.write(()).unwrap();
                }
                Primitive::Boolean => {
                    writer.write(i % 2 == 0).unwrap();
                }
                Primitive::Int => {
                    writer.write(std::i32::MAX).unwrap();
                }
                Primitive::Long => {
                    writer.write(std::i64::MAX).unwrap();
                }
                Primitive::Float => {
                    writer.write(std::f32::MAX).unwrap();
                }
                Primitive::Double => {
                    writer.write(std::f64::MAX).unwrap();
                }
                Primitive::Bytes => {
                    writer.write(vec![b'a', b'v', b'r', b'o', b'w']).unwrap();
                }
                Primitive::String => {
                    writer.write("avrow").unwrap();
                }
            });

            let buf = writer.into_inner().unwrap();

            // read
            let reader = Reader::with_schema(buf.as_slice(), MockSchema.prim(name)).unwrap();
            for i in reader {
                match primitive {
                    Primitive::Null => {
                        let _: () = from_value(&i).unwrap();
                    }
                    Primitive::Boolean => {
                        let _: bool = from_value(&i).unwrap();
                    }
                    Primitive::Int => {
                        let _: i32 = from_value(&i).unwrap();
                    }
                    Primitive::Long => {
                        let _: i64 = from_value(&i).unwrap();
                    }
                    Primitive::Float => {
                        let _: f32 = from_value(&i).unwrap();
                    }
                    Primitive::Double => {
                        let _: f64 = from_value(&i).unwrap();
                    }
                    Primitive::Bytes => {
                        let _: &[u8] = from_value(&i).unwrap();
                    }
                    Primitive::String => {
                        let _: &str = from_value(&i).unwrap();
                    }
                }
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Complex schema tests
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
struct LongList {
    value: i64,
    next: Option<Box<LongList>>,
}

#[test]
#[cfg(feature = "codec")]
fn io_read_write_self_referential_record() {
    // write
    for codec in CODECS.iter() {
        let schema = r##"
        {
            "type": "record",
            "name": "LongList",
            "aliases": ["LinkedLongs"],
            "fields" : [
              {"name": "value", "type": "long"},
              {"name": "next", "type": ["null", "LongList"]}
            ]
          }
        "##;

        let schema = Schema::from_str(schema).unwrap();
        let mut writer = writer_from_schema(&schema, *codec);
        for _ in 0..1 {
            let value = LongList {
                value: 1i64,
                next: Some(Box::new(LongList {
                    value: 2,
                    next: Some(Box::new(LongList {
                        value: 3,
                        next: None,
                    })),
                })),
            };
            // let value = LongList {
            //     value: 1i64,
            //     next: None,
            // };
            writer.serialize(value).unwrap();
        }

        let buf = writer.into_inner().unwrap();

        // read
        let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
        for i in reader {
            let _: LongList = from_value(&i).unwrap();
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Suit {
    SPADES,
    HEARTS,
    DIAMONDS,
    CLUBS,
}

#[test]
#[cfg(feature = "codec")]
fn enum_read_write() {
    // write
    for codec in CODECS.iter() {
        let schema = r##"
        {
            "type": "enum",
            "name": "Suit",
            "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        }
        "##;

        let schema = Schema::from_str(schema).unwrap();
        let mut writer = writer_from_schema(&schema, *codec);
        for _ in 0..1 {
            let value = Suit::SPADES;
            writer.serialize(value).unwrap();
        }

        let buf = writer.into_inner().unwrap();

        // read
        let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
        for i in reader {
            let _: Suit = from_value(&i).unwrap();
        }
    }
}

#[test]
#[cfg(feature = "codec")]
fn array_read_write() {
    // write
    for codec in CODECS.iter() {
        let schema = r##"
        {"type": "array", "items": "string"}
        "##;

        let schema = Schema::from_str(schema).unwrap();
        let mut writer = writer_from_schema(&schema, *codec);
        for _ in 0..DATUM_COUNT {
            let value = vec!["a", "v", "r", "o", "w"];
            writer.serialize(value).unwrap();
        }

        let buf = writer.into_inner().unwrap();

        // read
        let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
        for i in reader {
            let _: Vec<&str> = from_value(&i).unwrap();
        }
    }
}

#[test]
#[cfg(feature = "codec")]
fn map_read_write() {
    // write
    for codec in CODECS.iter() {
        let schema = r##"
        {"type": "map", "values": "long"}
        "##;

        let schema = Schema::from_str(schema).unwrap();
        let mut writer = writer_from_schema(&schema, *codec);
        for _ in 0..DATUM_COUNT {
            let mut value = HashMap::new();
            value.insert("foo", 1i64);
            value.insert("bar", 2);
            writer.serialize(value).unwrap();
        }

        let buf = writer.into_inner().unwrap();

        // read
        let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
        for i in reader {
            let _: HashMap<String, i64> = from_value(&i).unwrap();
        }
    }
}

#[test]
#[cfg(feature = "codec")]
fn union_read_write() {
    // write
    for codec in CODECS.iter() {
        let schema = r##"
        ["null", "string"]
        "##;

        let schema = Schema::from_str(schema).unwrap();
        let mut writer = writer_from_schema(&schema, *codec);
        for _ in 0..1 {
            writer.serialize(()).unwrap();
            writer.serialize("hello".to_string()).unwrap();
        }

        let buf = writer.into_inner().unwrap();

        // read
        let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
        for i in reader {
            let val = i.as_ref().unwrap();
            match val {
                Value::Null => {
                    let _a: () = from_value(&i).unwrap();
                }
                Value::Str(_) => {
                    let _a: &str = from_value(&i).unwrap();
                }
                _ => unreachable!("should not happen"),
            }
        }
    }
}

#[test]
#[cfg(feature = "codec")]
fn fixed_read_write() {
    // write
    for codec in CODECS.iter() {
        let schema = r##"
        {"type": "fixed", "size": 16, "name": "md5"}
        "##;

        let schema = Schema::from_str(schema).unwrap();
        let mut writer = writer_from_schema(&schema, *codec);
        for _ in 0..1 {
            let value = vec![
                b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd', b'e',
                b'f', b'g',
            ];
            writer.serialize(value.as_slice()).unwrap();
        }

        let buf = writer.into_inner().unwrap();

        // read
        let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
        for i in reader {
            let a: [u8; 16] = from_value(&i).unwrap();
            assert_eq!(a.len(), 16);
        }
    }
}

#[test]
#[cfg(feature = "codec")]
fn bytes_read_write() {
    let schema = Schema::from_str(r##"{"type": "bytes"}"##).unwrap();
    let mut writer = writer_from_schema(&schema, avrow::Codec::Deflate);
    let data = vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8];
    writer.serialize(&data).unwrap();

    let buf = writer.into_inner().unwrap();
    // let mut v: Vec<u8> = vec![];

    let reader = Reader::with_schema(buf.as_slice(), schema).unwrap();
    for i in reader {
        // dbg!(i);
        let b: &[u8] = from_value(&i).unwrap();
        dbg!(b);
    }

    // assert_eq!(v, data);
}

#[test]
#[should_panic]
#[cfg(feature = "codec")]
fn write_invalid_union_data_fails() {
    let schema = Schema::from_str(r##"["int", "float"]"##).unwrap();
    let mut writer = writer_from_schema(&schema, avrow::Codec::Null);
    writer.serialize("string").unwrap();
}

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// struct LongList {
//     value: i64,
//     next: Option<Box<LongList>>,
// }

#[test]
#[cfg(feature = "snappy")]
fn read_deflate_reuse() {
    let schema = Schema::from_str(
        r##"
        {
            "type": "record",
            "name": "LongList",
            "aliases": ["LinkedLongs"],
            "fields" : [
              {"name": "value", "type": "long"},
              {"name": "next", "type": ["null", "LongList"]}
            ]
          }
        "##,
    )
    .unwrap();
    let vec = vec![];
    let mut writer = avrow::Writer::with_codec(&schema, vec, Codec::Snappy).unwrap();
    for _ in 0..100000 {
        let value = LongList {
            value: 1i64,
            next: Some(Box::new(LongList {
                value: 2i64,
                next: Some(Box::new(LongList {
                    value: 3i64,
                    next: Some(Box::new(LongList {
                        value: 4i64,
                        next: Some(Box::new(LongList {
                            value: 5i64,
                            next: None,
                        })),
                    })),
                })),
            })),
        };
        writer.serialize(value).unwrap();
    }
    let vec = writer.into_inner().unwrap();

    let reader = Reader::new(&*vec).unwrap();
    for i in reader {
        let _v: LongList = from_value(&i).unwrap();
    }
}
