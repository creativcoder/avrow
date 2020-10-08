extern crate avrow;

#[macro_use]
extern crate criterion;

use criterion::Criterion;

use avrow::from_value;
use avrow::Reader;
use avrow::Schema;
use avrow::Writer;
use std::str::FromStr;

fn criterion_benchmark(c: &mut Criterion) {
    // Write benchmarks
    c.bench_function("write_null", |b| {
        let schema = Schema::from_str(r##"{"type": "null" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                writer.write(()).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_boolean", |b| {
        let schema = Schema::from_str(r##"{"type": "boolean" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for i in 0..100_000 {
                writer.write(i % 2 == 0).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_int", |b| {
        let schema = Schema::from_str(r##"{"type": "int" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                writer.write(45).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_long", |b| {
        let schema = Schema::from_str(r##"{"type": "long" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                writer.write(45i64).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_float", |b| {
        let schema = Schema::from_str(r##"{"type": "float" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                writer.write(45.0f32).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_double", |b| {
        let schema = Schema::from_str(r##"{"type": "double" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                writer.write(45.0f64).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_bytes", |b| {
        let schema = Schema::from_str(r##"{"type": "bytes" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                let v = vec![0u8, 1, 2, 3];
                writer.write(v).unwrap();
            }
        });

        writer.flush().unwrap();
    });

    c.bench_function("write_string", |b| {
        let schema = Schema::from_str(r##"{"type": "string" }"##).unwrap();
        let mut out = vec![];
        let mut writer = Writer::new(&schema, &mut out).unwrap();

        b.iter(|| {
            for _ in 0..100_000 {
                writer.write("hello").unwrap();
            }
        });

        writer.flush().unwrap();
    });

    // Read benchmarks
    c.bench_function("avro_read_bytes_from_vec", |b| {
        let avro_data = vec![
            79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 32, 123, 34,
            116, 121, 112, 101, 34, 58, 34, 98, 121, 116, 101, 115, 34, 125, 20, 97, 118, 114, 111,
            46, 99, 111, 100, 101, 99, 8, 110, 117, 108, 108, 0, 149, 158, 112, 231, 150, 73, 245,
            11, 130, 6, 13, 141, 239, 19, 146, 71, 2, 14, 12, 0, 1, 2, 3, 4, 5, 149, 158, 112, 231,
            150, 73, 245, 11, 130, 6, 13, 141, 239, 19, 146, 71,
        ];

        b.iter(|| {
            let reader = Reader::new(avro_data.as_slice()).unwrap();
            for i in reader {
                let _: Vec<u8> = from_value(&i).unwrap();
            }
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
