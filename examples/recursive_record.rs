use anyhow::Error;
use avrow::{from_value, Codec, Reader, Schema, Writer};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
struct LongList {
    value: i64,
    next: Option<Box<LongList>>,
}

fn main() -> Result<(), Error> {
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

    let schema = Schema::from_str(schema)?;
    let mut writer = Writer::with_codec(&schema, vec![], Codec::Null)?;

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
    writer.serialize(value)?;

    let buf = writer.into_inner()?;

    // read
    let reader = Reader::with_schema(buf.as_slice(), schema)?;
    for i in reader {
        let a: LongList = from_value(&i)?;
        dbg!(a);
    }

    Ok(())
}
