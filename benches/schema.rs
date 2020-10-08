#[macro_use]
extern crate criterion;
extern crate avrow;

use criterion::criterion_group;
use criterion::Criterion;
use std::str::FromStr;

use avrow::Schema;

fn parse_enum_schema() {
    let _ = Schema::from_str(
        r##"{ "type": "enum",
        "name": "Suit",
        "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
    }"##,
    )
    .unwrap();
}

fn parse_string_schema() {
    let _ = Schema::from_str(r##""string""##).unwrap();
}

fn parse_record_schema(c: &mut Criterion) {
    c.bench_function("parse_record_schema", |b| {
        b.iter(|| {
            let _ = Schema::from_str(
                r##"{
                "namespace": "sensor_data",
                "type": "record",
                "name": "can",
                "fields" : [
                    {"name": "can_id", "type": "int"},
                    {"name": "data", "type": "long"},
                    {"name": "timestamp", "type": "double"},
                    {"name": "seq_num", "type": "int"},
                    {"name": "global_seq", "type": "long"}
                ]
            }"##,
            )
            .unwrap();
        });
    });
}

fn bench_string_schema(c: &mut Criterion) {
    c.bench_function("parse string schema", |b| b.iter(parse_string_schema));
}

fn bench_enum_schema(c: &mut Criterion) {
    c.bench_function("parse enum schema", |b| b.iter(parse_enum_schema));
}

criterion_group!(
    benches,
    bench_string_schema,
    bench_enum_schema,
    parse_record_schema
);
criterion_main!(benches);
