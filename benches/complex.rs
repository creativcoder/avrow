extern crate avrow;
extern crate serde;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate criterion;

use avrow::Codec;
use avrow::Schema;
use avrow::Writer;
use criterion::Criterion;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
struct LongList {
    value: i64,
    next: Option<Box<LongList>>,
}

fn simple_record(c: &mut Criterion) {
    c.bench_function("simple_record", |b| {
        let schema = Schema::from_str(
            r##"{
            "namespace": "atherenergy.vcu_cloud_connect",
            "type": "record",
            "name": "can_raw",
            "fields" : [
                {"name": "one", "type": "int"},
                {"name": "two", "type": "long"},
                {"name": "three", "type": "long"},
                {"name": "four", "type": "int"},
                {"name": "five", "type": "long"}
            ]
        }"##,
        )
        .unwrap();
        let v = vec![];
        let mut writer = Writer::with_codec(&schema, v, Codec::Null).unwrap();
        b.iter(|| {
            for _ in 0..1000 {
                let data = Data {
                    one: 34,
                    two: 334,
                    three: 45765,
                    four: 45643,
                    five: 834,
                };

                writer.serialize(data).unwrap();
            }

            // batch and write data
            writer.flush().unwrap();
        });
    });
}

#[derive(Serialize, Deserialize)]
struct Data {
    one: u32,
    two: u64,
    three: u64,
    four: u32,
    five: u64,
}

fn array_record(c: &mut Criterion) {
    c.bench_function("Array of records", |b| {
        let schema = Schema::from_str(
            r##"{"type": "array", "items": {
            "namespace": "atherenergy.vcu_cloud_connect",
            "type": "record",
            "name": "can_raw",
            "fields" : [
                {"name": "one", "type": "int"},
                {"name": "two", "type": "long"},
                {"name": "three", "type": "long"},
                {"name": "four", "type": "int"},
                {"name": "five", "type": "long"}
            ]
        }}"##,
        )
        .unwrap();
        let mut v = vec![];
        let mut writer = Writer::with_codec(&schema, &mut v, Codec::Null).unwrap();
        b.iter(|| {
            let mut can_array = vec![];
            for _ in 0..1000 {
                let data = Data {
                    one: 34,
                    two: 334,
                    three: 45765,
                    four: 45643,
                    five: 834,
                };

                can_array.push(data);
            }

            // batch and write data
            writer.serialize(can_array).unwrap();
            writer.flush().unwrap();
        });
    });
}

fn nested_recursive_record(c: &mut Criterion) {
    c.bench_function("recursive_nested_record", |b| {
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
        let mut writer = Writer::with_codec(&schema, vec![], Codec::Null).unwrap();

        b.iter(|| {
            for _ in 0..1000 {
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
                writer.serialize(value).unwrap();
            }
        });
        writer.flush().unwrap();
    });
}

criterion_group!(
    benches,
    nested_recursive_record,
    array_record,
    simple_record
);
criterion_main!(benches);
