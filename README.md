<div align="center">
  <img alt="avrow" width="250" src="assets/avrow_logo.png" />

[![github actions](https://github.com/creativcoder/avrow/workflows/Rust/badge.svg)](https://github.com/creativcoder/avrow/actions)
[![crates](https://img.shields.io/crates/v/avrow.svg)](https://crates.io/crates/io-uring)
[![docs.rs](https://docs.rs/avrow/badge.svg)](https://docs.rs/avrow/)
[![license](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/creativcoder/avrow/blob/master/LICENSE-MIT)
[![license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/creativcoder/avrow/blob/master/LICENSE-APACHE)
[![Contributor Covenant](https://img.shields.io/badge/contributor%20covenant-v1.4%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)

  <br />
  <br />

  
### Avrow is a pure Rust implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html) with [Serde](https://github.com/serde-rs/serde) support.
  

  <br />
  <br />

</div>

### Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Getting started](#getting-started)
- [Examples](#examples)
  - [Writing avro data](#writing-avro-data)
  - [Reading avro data](#reading-avro-data)
  - [Writer builder](#writer-customization)
- [Supported Codecs](#supported-codecs)
- [Using the avrow-cli tool](#using-avrow-cli-tool)
- [Benchmarks](#benchmarks)
- [Todo](#todo)
- [Changelog](#changelog)
- [Contributions](#contributions)
- [Support](#support)
- [MSRV](#msrv)
- [License](#license)

## Overview

Avrow is a pure Rust implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html): a row based data serialization system. The Avro data serialization format finds its use quite a lot in big data streaming systems such as [Kafka](https://kafka.apache.org/) and [Spark](https://spark.apache.org/).
Within avro's context, an avro encoded file or byte stream is called a "data file".
To write data in avro encoded format, one needs a schema which is provided in json format. Here's an example of an avro schema represented in json:

```json
{
  "type": "record",
  "name": "LongList",
  "aliases": ["LinkedLongs"],
  "fields" : [
    {"name": "value", "type": "long"},
    {"name": "next", "type": ["null", "LongList"]}
  ]
}
```
The above schema is of type record with fields and represents a linked list of 64-bit integers. In most implementations, this schema is then fed to a `Writer` instance along with a buffer to write encoded data to. One can then call one
of the `write` methods on the writer to write data. One distinguishing aspect of avro is that the schema for the encoded data is written on the header of the data file. This means that for reading data you don't need to provide a schema to a `Reader` instance. The spec also allows providing a reader schema to filter data when reading.

The Avro specification provides two kinds of encoding:
* Binary encoding - Efficent and takes less space on disk.
* JSON encoding - When you want a readable version of avro encoded data. Also used for debugging purposes.

This crate implements only the binary encoding as that's the format practically used for performance and storage reasons.

## Features.

* Full support for recursive self-referential schemas with Serde serialization/deserialization.
* All compressions codecs (`deflate`, `bzip2`, `snappy`, `xz`, `zstd`) supported as per spec.
* Simple and intuitive API - As the underlying structures in use are `Read` and `Write` types, avrow tries to mimic the same APIs as Rust's standard library APIs for minimal learning overhead. Writing avro values is simply calling `write` or `serialize` (with serde) and reading avro values is simply using iterators.
* Less bloat / Lightweight - Compile times in Rust are costly. Avrow tries to use minimal third-party crates. Compression codec and schema fingerprinting support are feature gated by default. To use them, compile with respective feature flags (e.g. `--features zstd`).
* Schema evolution - One can configure the avrow `Reader` with a reader schema and only read data relevant to their use case.
* Schema's in avrow supports querying their canonical form and have fingerprinting (`rabin64`, `sha256`, `md5`) support.

**Note**: This is not a complete spec implemention and remaining features being implemented are listed under [Todo](#todo) section.

## Getting started:

Add avrow as a dependency to `Cargo.toml`:

```toml
[dependencies]
avrow = "0.1"
```

## Examples:

### Writing avro data

```rust

use anyhow::Error;
use avrow::{Schema, Writer};
use std::str::FromStr;

fn main() -> Result<(), Error> {
    // Create schema from json
    let schema = Schema::from_str(r##"{"type":"string"}"##)?;
    // or from a path
    let schema2 = Schema::from_path("./string_schema.avsc")?;
    // Create an output stream
    let stream = Vec::new();
    // Create a writer
    let writer = Writer::new(&schema, stream.as_slice())?;
    // Write your data!
    let res = writer.write("Hey")?;
    // or using serialize method for serde derived types.
    let res = writer.serialize("there!")?;

    Ok(())
}

```
For simple and native Rust types, avrow provides a `From` impl for Avro value types. For compound or user defined types (structs, enums), one can use the `serialize` method which relies on serde. Alternatively, one can construct `avrow::Value` instances which is a more verbose way to write avro values and should be a last resort.

### Reading avro data

```rust
fn main() -> Result<(), Error> {
    let schema = Schema::from_str(r##""null""##);
    let data = vec![
        79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101,
        109, 97, 32, 123, 34, 116, 121, 112, 101, 34, 58, 34, 98, 121, 116,
        101, 115, 34, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101,
        99, 14, 100, 101, 102, 108, 97, 116, 101, 0, 145, 85, 112, 15, 87,
        201, 208, 26, 183, 148, 48, 236, 212, 250, 38, 208, 2, 18, 227, 97,
        96, 100, 98, 102, 97, 5, 0, 145, 85, 112, 15, 87, 201, 208, 26,
        183, 148, 48, 236, 212, 250, 38, 208,
    ];
    // Create a Reader
    let reader = Reader::with_schema(v.as_slice(), schema)?;
    for i in reader {
        dbg!(&i);
    }

    Ok(())
}

```

A more involved self-referential recursive schema example:

```rust
use anyhow::Error;
use avrow::{from_value, Codec, Reader, Schema, Writer};
use serde::{Deserialize, Serialize};

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

    // Calling into_inner performs flush internally. Alternatively, one can call flush explicitly.
    let buf = writer.into_inner()?;

    // read
    let reader = Reader::with_schema(buf.as_slice(), schema)?;
    for i in reader {
        let a: LongList = from_value(&i)?;
        dbg!(a);
    }

    Ok(())
}

```

An example of writing a json object with a confirming schema. The json object maps to an `avrow::Record` type.

```rust
use anyhow::Error;
use avrow::{from_value, Reader, Record, Schema, Writer};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
struct Mentees {
    id: i32,
    username: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RustMentors {
    name: String,
    github_handle: String,
    active: bool,
    mentees: Mentees,
}

fn main() -> Result<(), Error> {
    let schema = Schema::from_str(
        r##"
            {
            "name": "rust_mentors",
            "type": "record",
            "fields": [
                {
                "name": "name",
                "type": "string"
                },
                {
                "name": "github_handle",
                "type": "string"
                },
                {
                "name": "active",
                "type": "boolean"
                },
                {
                    "name":"mentees",
                    "type": {
                        "name":"mentees",
                        "type": "record",
                        "fields": [
                            {"name":"id", "type": "int"},
                            {"name":"username", "type": "string"}
                        ]
                    }
                }
            ]
            }
"##,
    )?;

    let json_data = serde_json::from_str(
        r##"
    { "name": "bob",
        "github_handle":"ghbob",
        "active": true,
        "mentees":{"id":1, "username":"alice"} }"##,
    )?;
    let rec = Record::from_json(json_data, &schema)?;
    let mut writer = crate::Writer::new(&schema, vec![])?;
    writer.write(rec)?;

    let avro_data = writer.into_inner()?;
    let reader = crate::Reader::from(avro_data.as_slice())?;
    for value in reader {
        let mentors: RustMentors = from_value(&value)?;
        dbg!(mentors);
    }
    Ok(())
}

```

### Writer customization

If you want to have more control over the parameters of `Writer`, consider using `WriterBuilder` as shown below:

```rust

use anyhow::Error;
use avrow::{Codec, Reader, Schema, WriterBuilder};

fn main() -> Result<(), Error> {
    let schema = Schema::from_str(r##""null""##)?;
    let v = vec![];
    let mut writer = WriterBuilder::new()
        .set_codec(Codec::Null)
        .set_schema(&schema)
        .set_datafile(v)
        // set any custom metadata in the header
        .set_metadata("hello", "world")
        // set after how many bytes, the writer should flush
        .set_flush_interval(128_000)
        .build()
        .unwrap();
    writer.serialize(())?;
    let v = writer.into_inner()?;

    let reader = Reader::with_schema(v.as_slice(), schema)?;
    for i in reader {
        dbg!(i?);
    }

    Ok(())
}
```

Refer to [examples](./examples) for more code examples.

## Supported Codecs

In order to facilitate efficient encoding, avro spec also defines compression codecs to use when serializing data.

Avrow supports all compression codecs as per spec:

- Null - The default is no codec.
- [Deflate](https://en.wikipedia.org/wiki/DEFLATE)
- [Snappy](https://github.com/google/snappy)
- [Zstd](https://facebook.github.io/zstd/)
- [Bzip2](https://www.sourceware.org/bzip2/)
- [Xz](https://linux.die.net/man/1/xz)

These are feature-gated behind their respective flags. Check `Cargo.toml` `features` section for more details.

## Using avrow-cli tool:

Quite often you will need a quick way to examine avro file for debugging purposes. 
For that, this repository also comes with the [`avrow-cli`](./avrow-cli) tool (av)
by which one can examine avro datafiles from the command line.

See [avrow-cli](avrow-cli/) repository for more details.

Installing avrow-cli:

```
cd avrow-cli
cargo install avrow-cli
```

Using avrow-cli (binary name is `av`):

```bash
av read -d data.avro
```

The `read` subcommand will print all rows in `data.avro` to standard out in debug format.

### Rust native types to Avro value mapping (via Serde)

Primitives
---

| Rust native types (primitive types) | Avro (`Value`) |
| ----------------------------------- | -------------- |
| `(), Option::None`                  | `null`         |
| `bool`                              | `boolean`      |
| `i8, u8, i16, u16, i32, u32`        | `int`          |
| `i64, u64`                          | `long`         |
| `f32`                               | `float`        |
| `f64`                               | `double`       |
| `&[u8], Vec<u8>`                    | `bytes`        |
| `&str, String`                      | `string`       |
---
Complex

| Rust native types (complex types)                    | Avro     |
| ---------------------------------------------------- | -------- |
| `struct Foo {..}`                                    | `record` |
| `enum Foo {A,B}` (variants cannot have data in them) | `enum`   |
| `Vec<T> where T: Into<Value>`                        | `array`  |
| `HashMap<String, T> where T: Into<Value>`            | `map`    |
| `T where T: Into<Value>`                             | `union`  |
| `Vec<u8>` : Length equal to size defined in schema   | `fixed`  |

<br>

## Todo

* [Logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types) support.
* Sorted reads.
* Single object encoding.
* Schema Registry as a trait - would allow avrow to read from and write to remote schema registries.
* AsyncRead + AsyncWrite Reader and Writers.
* Avro protocol message and RPC support. 
* Benchmarks and optimizations.

## Changelog

Please see the [CHANGELOG](CHANGELOG.md) for a release history.

## Contributions

All kinds of contributions are welcome.

Head over to [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## Support

<a href="https://www.buymeacoffee.com/creativcoder" target="_blank"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee" style="height: 41px !important;width: 174px !important;box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;-webkit-box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;" ></a>

[![ko-fi](https://www.ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/P5P71YZ0L)

## MSRV

Avrow works on stable Rust, starting 1.37+.
It does not use any nightly features.

## License

Dual licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
