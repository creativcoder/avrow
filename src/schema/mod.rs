//! Contains routines for parsing and validating an Avro schema.
//! Schemas in avro are written as JSON and can be provided as .avsc files
//! to a Writer or a Reader.

pub mod common;
#[cfg(test)]
mod tests;
use crate::error::AvrowErr;
pub use common::Order;
mod canonical;
pub mod parser;
pub(crate) use parser::Registry;

use crate::error::AvrowResult;
use crate::value::Value;
use canonical::normalize_schema;
use canonical::CanonicalSchema;
use common::{Field, Name};
use indexmap::IndexMap;
use serde_json::{self, Value as JsonValue};
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Variant {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    Str,
    Record {
        name: Name,
        aliases: Option<Vec<String>>,
        fields: IndexMap<String, Field>,
    },
    Fixed {
        name: Name,
        size: usize,
    },
    Enum {
        name: Name,
        aliases: Option<Vec<String>>,
        symbols: Vec<String>,
    },
    Map {
        values: Box<Variant>,
    },
    Array {
        items: Box<Variant>,
    },
    Union {
        variants: Vec<Variant>,
    },
    Named(String),
}

/// Represents the avro schema used to write encoded avro data.
#[derive(Debug)]
pub struct Schema {
    // TODO can remove this if not needed
    inner: JsonValue,
    // Schema context that has a lookup table to resolve named schema references
    pub(crate) cxt: Registry,
    // typed and stripped version of schema used internally.
    pub(crate) variant: Variant,
    // canonical form of schema. This is used for equality.
    pub(crate) canonical: CanonicalSchema,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl std::str::FromStr for Schema {
    type Err = AvrowErr;
    /// Parse an avro schema from a JSON string
    /// One can use Rust's raw string syntax (r##""##) to pass schema.
    fn from_str(schema: &str) -> Result<Self, Self::Err> {
        let schema_json =
            serde_json::from_str(schema).map_err(|e| AvrowErr::SchemaParseErr(e.into()))?;
        Schema::parse_imp(schema_json)
    }
}

impl Schema {
    /// Parses an avro schema from a JSON schema in a file.
    /// Alternatively, one can use the [`FromStr`](https://doc.rust-lang.org/std/str/trait.FromStr.html)
    /// impl to create the Schema from a JSON string:
    /// ```
    /// use std::str::FromStr;
    /// use avrow::Schema;
    ///
    /// let schema = Schema::from_str(r##""null""##).unwrap();
    /// ```
    pub fn from_path<P: AsRef<Path> + Debug>(path: P) -> AvrowResult<Self> {
        let schema_file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(AvrowErr::SchemaParseErr)?;
        let value =
            serde_json::from_reader(schema_file).map_err(|e| AvrowErr::SchemaParseErr(e.into()))?;
        Schema::parse_imp(value)
    }

    fn parse_imp(schema_json: JsonValue) -> AvrowResult<Self> {
        let mut parser = Registry::new();
        let pcf = CanonicalSchema(normalize_schema(&schema_json)?);
        // TODO see if we can use canonical form to parse variant
        let variant = parser.parse_schema(&schema_json, None)?;
        Ok(Schema {
            inner: schema_json,
            cxt: parser,
            variant,
            canonical: pcf,
        })
    }

    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        format!("{}", self.inner).into_bytes()
    }

    pub(crate) fn variant(&self) -> &Variant {
        &self.variant
    }

    #[inline(always)]
    pub(crate) fn validate(&self, value: &Value) -> AvrowResult<()> {
        self.variant.validate(value, &self.cxt)
    }

    /// Returns the canonical form of an Avro schema.
    /// Example:
    /// ```rust
    /// use avrow::Schema;
    /// use std::str::FromStr;
    ///
    /// let schema = Schema::from_str(r##"
    ///     {
    ///         "type": "record",
    ///         "name": "LongList",
    ///         "aliases": ["LinkedLongs"],
    ///         "fields" : [
    ///             {"name": "value", "type": "long"},
    ///             {"name": "next", "type": ["null", "LongList"]
    ///         }]
    ///     }
    /// "##).unwrap();
    ///
    /// let canonical = schema.canonical_form();
    /// ```
    pub fn canonical_form(&self) -> &CanonicalSchema {
        &self.canonical
    }
}

impl Variant {
    pub fn validate(&self, value: &Value, cxt: &Registry) -> AvrowResult<()> {
        let variant = self;
        match (value, variant) {
            (Value::Null, Variant::Null)
            | (Value::Boolean(_), Variant::Boolean)
            | (Value::Int(_), Variant::Int)
            // long is promotable to float or double
            | (Value::Long(_), Variant::Long)
            | (Value::Long(_), Variant::Float)
            | (Value::Long(_), Variant::Double)
            // int is promotable to long, float or double
            | (Value::Int(_), Variant::Long)
            | (Value::Int(_), Variant::Float)
            | (Value::Int(_), Variant::Double)
            | (Value::Float(_), Variant::Float)
            // float is promotable to double
            | (Value::Float(_), Variant::Double)
            | (Value::Double(_), Variant::Double)
            | (Value::Str(_), Variant::Str)
            // string is promotable to bytes
            | (Value::Str(_), Variant::Bytes)
            // bytes is promotable to string
            | (Value::Bytes(_), Variant::Str)
            | (Value::Bytes(_), Variant::Bytes) => {},
            (Value::Fixed(v), Variant::Fixed { size, .. })
            | (Value::Bytes(v), Variant::Fixed { size, .. }) => {
                if v.len() != *size {
                    return Err(AvrowErr::FixedValueLenMismatch {
                        found: v.len(),
                        expected: *size,
                    });
                }
            }
            (Value::Record(rec), Variant::Record { ref fields, .. }) => {
                for (fname, fvalue) in &rec.fields {
                    if let Some(ftype) = fields.get(fname) {
                        ftype.ty.validate(&fvalue.value, cxt)?;
                    } else {
                        return Err(AvrowErr::RecordFieldMissing);
                    }
                }
            }
            (Value::Map(hmap), Variant::Map { values }) => {
                return if let Some(v) = hmap.values().next() {
                    values.validate(v, cxt)
                } else {
                    Err(AvrowErr::EmptyMap)
                }
            }
            (Value::Enum(sym), Variant::Enum { symbols, .. }) if symbols.contains(sym) => {
                return Ok(())
            }
            (Value::Array(item), Variant::Array { items }) => {
                return if let Some(v) = item.first() {
                    items.validate(v, cxt)
                } else {
                    Err(AvrowErr::EmptyArray)
                }
            }
            (v, Variant::Named(name)) => {
                if let Some(schema) = cxt.get(&name) {
                    if schema.validate(v, cxt).is_ok() {
                        return Ok(());
                    }
                }
                return Err(AvrowErr::NamedSchemaNotFoundForValue)
            }
            // Value `a` can be any of the above schemas + any named schema in the schema registry
            (a, Variant::Union { variants }) => {
                for s in variants.iter() {
                    if s.validate(a, cxt).is_ok() {
                        return Ok(());
                    }
                }

                return Err(AvrowErr::NotFoundInUnion)
            }

            (v, s) => {
                return Err(AvrowErr::SchemaDataValidationFailed(
                    format!("{:?}", v),
                    format!("{:?}", s),
                ))
            }
        }

        Ok(())
    }

    fn get_named_mut(&mut self) -> Option<&mut Name> {
        match self {
            Variant::Record { name, .. }
            | Variant::Fixed { name, .. }
            | Variant::Enum { name, .. } => Some(name),
            _ => None,
        }
    }
}
