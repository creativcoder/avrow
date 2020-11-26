//! Avrow is a pure Rust implementation of the [Apache Avro specification](https://avro.apache.org/docs/current/spec.html).
//!
//! Please refer to the [README](https://github.com/creativcoder/avrow/blob/main/README.md) for an overview.
//! For more details on the spec, head over to the [FAQ](https://cwiki.apache.org/confluence/display/AVRO/FAQ).
//!
//! ## Using the library
//!
//! Add avrow to your `Cargo.toml`:
//!```toml
//! [dependencies]
//! avrow = "0.2.1"
//!```
//! ## A hello world example of reading and writing avro data files

//!```rust
//! use avrow::{Reader, Schema, Writer, from_value};
//! use std::str::FromStr;
//! use anyhow::Error;
//!
//! fn main() -> Result<(), Error> {
//!     // Writing data
//!
//!     // Create a schema
//!     let schema = Schema::from_str(r##""null""##)?;
//!     // Create writer using schema and provide a buffer to write to
//!     let mut writer = Writer::new(&schema, vec![])?;
//!     // Write data using write
//!     writer.write(())?;
//!     // or serialize via serde
//!     writer.serialize(())?;
//!     // retrieve the underlying buffer using the into_inner method.
//!     let buf = writer.into_inner()?;
//!
//!     // Reading data
//!
//!     // Create Reader by providing a Read wrapped version of `buf`
//!     let reader = Reader::new(buf.as_slice())?;
//!     // Use iterator for reading data in an idiomatic manner.
//!     for i in reader {
//!         // reading values can fail due to decoding errors, so the return value of iterator is a Option<Result<Value>>
//!         // it allows one to examine the failure reason on the underlying avro reader.
//!         dbg!(&i);
//!         // This value can be converted to a native Rust type using from_value method from the serde impl.
//!         let _: () = from_value(&i)?;
//!     }
//!
//!     Ok(())
//! }

//!```

#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/creativcoder/avrow/main/assets/avrow_logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/creativcoder/avrow/main/assets/avrow_logo.png"
)]
#![deny(missing_docs)]
#![recursion_limit = "1024"]
#![deny(unused_must_use)]
#![deny(rust_2018_idioms)]
#![deny(warnings)]

mod codec;
pub mod config;
mod error;
mod reader;
mod schema;
mod serde_avro;
mod util;
mod value;
mod writer;

pub use codec::Codec;
pub use error::AvrowErr;
pub use reader::from_value;
pub use reader::Header;
pub use reader::Reader;
pub use schema::Schema;
pub use serde_avro::to_value;
pub use value::Record;
pub use value::Value;
pub use writer::Writer;
pub use writer::WriterBuilder;
