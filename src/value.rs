//! Represents the types that

use crate::error::AvrowErr;
use crate::schema;
use crate::schema::common::validate_name;
use crate::schema::parser::parse_default;
use crate::schema::Registry;
use crate::util::{encode_long, encode_raw_bytes};
use crate::Schema;
use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use indexmap::IndexMap;
use integer_encoding::VarIntWriter;
use schema::Order;
use schema::Variant;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::io::Write;

// Convenient type alias for map initialzation.
pub type Map = HashMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FieldValue {
    pub(crate) value: Value,
    #[serde(skip_serializing)]
    order: schema::Order,
}

impl FieldValue {
    pub(crate) fn new(value: Value) -> Self {
        FieldValue {
            value,
            order: Order::Ascending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
/// The [Record](https://avro.apache.org/docs/current/spec.html#schema_record) avro type.
/// Avro records translates to a struct in Rust. Any struct that implements serde's
/// Serializable trait can be converted to an avro record.
pub struct Record {
    pub(crate) name: String,
    pub(crate) fields: IndexMap<String, FieldValue>,
}

impl Record {
    /// Creates a new avro record type with the given name.
    pub fn new(name: &str) -> Self {
        Record {
            fields: IndexMap::new(),
            name: name.to_string(),
        }
    }

    /// Adds a field to the record.
    pub fn insert<T: Into<Value>>(&mut self, field_name: &str, ty: T) -> Result<(), AvrowErr> {
        validate_name(0, field_name)?;
        self.fields
            .insert(field_name.to_string(), FieldValue::new(ty.into()));
        Ok(())
    }

    /// Sets the ordering of the field in the record.
    pub fn set_field_order(&mut self, field_name: &str, order: Order) -> Result<(), AvrowErr> {
        let a = self
            .fields
            .get_mut(field_name)
            .ok_or(AvrowErr::FieldNotFound)?;
        a.order = order;
        Ok(())
    }

    /// Creates a record from a [BTreeMap](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html) by consuming it.
    /// The values in `BTreeMap` must implement `Into<Value>`. The `name` provided must match with the name in the record
    /// schema being provided to the writer.
    pub fn from_btree<K: Into<String> + Ord + Display, V: Into<Value>>(
        name: &str,
        btree: BTreeMap<K, V>,
    ) -> Result<Self, AvrowErr> {
        let mut record = Record::new(name);
        for (k, v) in btree {
            let field_value = FieldValue {
                value: v.into(),
                order: Order::Ascending,
            };
            record.fields.insert(k.to_string(), field_value);
        }

        Ok(record)
    }

    /// Creates a record from a JSON object (serde_json::Value). A confirming record schema must be provided.
    pub fn from_json(
        json: serde_json::Map<String, serde_json::Value>,
        schema: &Schema,
    ) -> Result<Value, AvrowErr> {
        if let Variant::Record {
            name,
            fields: record_schema_fields,
            ..
        } = &schema.variant
        {
            let mut values = IndexMap::with_capacity(record_schema_fields.len());
            'fields: for (k, v) in record_schema_fields {
                if let Some(default_value) = json.get(k) {
                    if let Variant::Union { variants } = &v.ty {
                        for var in variants {
                            if let Ok(v) = parse_default(&default_value, &var) {
                                values.insert(k.to_string(), FieldValue::new(v));
                                continue 'fields;
                            }
                        }
                        return Err(AvrowErr::FailedDefaultUnion);
                    } else {
                        let parsed_value = parse_default(&default_value, &v.ty)?;
                        values.insert(k.to_string(), FieldValue::new(parsed_value));
                    }
                } else if let Some(v) = &v.default {
                    values.insert(k.to_string(), FieldValue::new(v.clone()));
                } else {
                    return Err(AvrowErr::FieldNotFound);
                }
            }

            Ok(Value::Record(crate::value::Record {
                fields: values,
                name: name.fullname(),
            }))
        } else {
            Err(AvrowErr::ExpectedJsonObject)
        }
    }
}

// TODO: Avro sort order
// impl PartialOrd for Value {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         match (self, other) {
//             (Value::Null, Value::Null) => Some(Ordering::Equal),
//             (Value::Boolean(self_v), Value::Boolean(other_v)) => {
//                 if self_v == other_v {
//                     return Some(Ordering::Equal);
//                 }
//                 if *self_v == false && *other_v {
//                     Some(Ordering::Less)
//                 } else {
//                     Some(Ordering::Greater)
//                 }
//             }
//             (Value::Int(self_v), Value::Int(other_v)) => Some(self_v.cmp(other_v)),
//             (Value::Long(self_v), Value::Long(other_v)) => Some(self_v.cmp(other_v)),
//             (Value::Float(self_v), Value::Float(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Double(self_v), Value::Double(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Bytes(self_v), Value::Bytes(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Byte(self_v), Value::Byte(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Fixed(self_v), Value::Fixed(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Str(self_v), Value::Str(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Array(self_v), Value::Array(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Enum(self_v), Value::Enum(other_v)) => self_v.partial_cmp(other_v),
//             (Value::Record(_self_v), Value::Record(_other_v)) => todo!(),
//             _ => todo!(),
//         }
//     }
// }

/// Represents an Avro value
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Value {
    /// A null value.
    Null,
    /// An i32 integer value.
    Int(i32),
    /// An i64 long value.
    Long(i64),
    /// A boolean value.
    Boolean(bool),
    /// A f32 float value.
    Float(f32),
    /// A f64 float value.
    Double(f64),
    /// A Record value (BTreeMap<String, Value>).
    Record(Record),
    /// A Fixed value.
    Fixed(Vec<u8>),
    /// A Map value.
    Map(Map),
    /// A sequence of u8 bytes.
    Bytes(Vec<u8>),
    /// Rust strings map directly to avro strings
    Str(String),
    /// A union is a sequence of unique `Value`s
    Union(Box<Value>),
    /// An enumeration. Unlike Rust enums, enums in avro don't support data within their variants.
    Enum(String),
    /// An array of `Value`s
    Array(Vec<Value>),
    /// auxiliary u8 helper for serde. Not an avro value.
    Byte(u8),
}

impl Value {
    pub(crate) fn encode<W: Write>(
        &self,
        writer: &mut W,
        schema: &Variant,
        cxt: &Registry,
    ) -> Result<(), AvrowErr> {
        match (self, schema) {
            (Value::Null, Variant::Null) => {}
            (Value::Boolean(b), Variant::Boolean) => writer
                .write_all(&[*b as u8])
                .map_err(AvrowErr::EncodeFailed)?,
            (Value::Int(i), Variant::Int) => {
                writer.write_varint(*i).map_err(AvrowErr::EncodeFailed)?;
            }
            // int is promotable to long, float or double ---
            (Value::Int(i), Variant::Long) => {
                writer
                    .write_varint(*i as i64)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            (Value::Int(i), Variant::Float) => {
                writer
                    .write_f32::<LittleEndian>(*i as f32)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            (Value::Int(i), Variant::Double) => {
                writer
                    .write_f64::<LittleEndian>(*i as f64)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            // ---
            (Value::Long(l), Variant::Long) => {
                writer.write_varint(*l).map_err(AvrowErr::EncodeFailed)?;
            }
            (Value::Long(l), Variant::Float) => {
                writer
                    .write_f32::<LittleEndian>(*l as f32)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            (Value::Long(l), Variant::Double) => {
                writer
                    .write_f64::<LittleEndian>(*l as f64)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            (Value::Float(f), Variant::Float) => {
                writer
                    .write_f32::<LittleEndian>(*f)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            // float is promotable to double ---
            (Value::Float(f), Variant::Double) => {
                writer
                    .write_f64::<LittleEndian>(*f as f64)
                    .map_err(AvrowErr::EncodeFailed)?;
            } // ---
            (Value::Double(d), Variant::Double) => {
                writer
                    .write_f64::<LittleEndian>(*d)
                    .map_err(AvrowErr::EncodeFailed)?;
            }
            (ref value, Variant::Named(name)) => {
                if let Some(schema) = cxt.get(name) {
                    value.encode(writer, schema, cxt)?;
                }
            }
            // Match with union happens first than more specific match arms
            (ref value, Variant::Union { variants, .. }) => {
                let (union_idx, schema) = resolve_union(&value, &variants, cxt)?;
                let union_idx = union_idx as i32;
                writer
                    .write_varint(union_idx)
                    .map_err(AvrowErr::EncodeFailed)?;
                value.encode(writer, &schema, cxt)?
            }
            (Value::Record(ref record), Variant::Record { fields, .. }) => {
                for (f_name, f_value) in &record.fields {
                    let field_type = fields.get(f_name);
                    if let Some(field_ty) = field_type {
                        f_value.value.encode(writer, &field_ty.ty, cxt)?;
                    }
                }
            }
            (Value::Map(hmap), Variant::Map { values }) => {
                // number of keys/value (start of a block)
                encode_long(hmap.keys().len() as i64, writer)?;
                for (k, v) in hmap.iter() {
                    encode_long(k.len() as i64, writer)?;
                    encode_raw_bytes(&*k.as_bytes(), writer)?;
                    v.encode(writer, values, cxt)?;
                }
                // marks end of block
                encode_long(0, writer)?;
            }
            (Value::Fixed(ref v), Variant::Fixed { .. }) => {
                writer.write_all(&*v).map_err(AvrowErr::EncodeFailed)?;
            }
            (Value::Str(s), Variant::Str) => {
                encode_long(s.len() as i64, writer)?;
                encode_raw_bytes(&*s.as_bytes(), writer)?;
            }
            // string is promotable to bytes ---
            (Value::Str(s), Variant::Bytes) => {
                encode_long(s.len() as i64, writer)?;
                encode_raw_bytes(&*s.as_bytes(), writer)?;
            } // --
            (Value::Bytes(b), Variant::Bytes) => {
                encode_long(b.len() as i64, writer)?;
                encode_raw_bytes(&*b, writer)?;
            }
            // bytes is promotable to string ---
            (Value::Bytes(b), Variant::Str) => {
                encode_long(b.len() as i64, writer)?;
                encode_raw_bytes(&*b, writer)?;
            } // ---
            (Value::Bytes(b), Variant::Fixed { size: _size, .. }) => {
                encode_raw_bytes(&*b, writer)?;
            }
            (Value::Enum(ref sym), Variant::Enum { symbols, .. }) => {
                if let Some(idx) = symbols.iter().position(|r| r == sym) {
                    writer
                        .write_varint(idx as i32)
                        .map_err(AvrowErr::EncodeFailed)?;
                } else {
                    return Err(AvrowErr::SchemaDataMismatch);
                }
            }
            (
                Value::Array(ref values),
                Variant::Array {
                    items: items_schema,
                },
            ) => {
                let array_items_count = Value::from(values.len() as i64);
                array_items_count.encode(writer, &Variant::Long, cxt)?;

                for i in values {
                    i.encode(writer, items_schema, cxt)?;
                }
                Value::from(0i64).encode(writer, &Variant::Long, cxt)?;
            }
            // case where serde serializes a Vec<u8> to a Array of Byte
            // FIXME:figure out a better way for this?
            (Value::Array(ref values), Variant::Bytes) => {
                let mut v = Vec::with_capacity(values.len());
                for i in values {
                    if let Value::Byte(b) = i {
                        v.push(*b);
                    }
                }
                encode_long(values.len() as i64, writer)?;
                encode_raw_bytes(&*v, writer)?;
            }
            _ => return Err(AvrowErr::SchemaDataMismatch),
        };
        Ok(())
    }
}

// Given a value, returns the index and the variant of the union
fn resolve_union<'a>(
    value: &Value,
    union_variants: &'a [Variant],
    cxt: &'a Registry,
) -> Result<(usize, &'a Variant), AvrowErr> {
    for (idx, variant) in union_variants.iter().enumerate() {
        match (value, variant) {
            (Value::Null, Variant::Null)
            | (Value::Boolean(_), Variant::Boolean)
            | (Value::Int(_), Variant::Int)
            | (Value::Long(_), Variant::Long)
            | (Value::Float(_), Variant::Float)
            | (Value::Double(_), Variant::Double)
            | (Value::Bytes(_), Variant::Bytes)
            | (Value::Str(_), Variant::Str)
            | (Value::Map(_), Variant::Map { .. })
            | (Value::Array(_), Variant::Array { .. })
            | (Value::Fixed(_), Variant::Fixed { .. })
            | (Value::Enum(_), Variant::Enum { .. })
            | (Value::Record(_), Variant::Record { .. }) => return Ok((idx, variant)),
            (Value::Array(v), Variant::Fixed { size, .. }) => {
                if v.len() == *size {
                    return Ok((idx, variant));
                }
                return Err(AvrowErr::FixedValueLenMismatch {
                    found: v.len(),
                    expected: *size,
                });
            }
            (Value::Union(_), _) => return Err(AvrowErr::NoImmediateUnion),
            (Value::Record(_), Variant::Named(name)) => {
                if let Some(schema) = cxt.get(&name) {
                    return Ok((idx, schema));
                } else {
                    return Err(AvrowErr::SchemaNotFoundInUnion);
                }
            }
            (Value::Enum(_), Variant::Named(name)) => {
                if let Some(schema) = cxt.get(&name) {
                    return Ok((idx, schema));
                } else {
                    return Err(AvrowErr::SchemaNotFoundInUnion);
                }
            }
            (Value::Fixed(_), Variant::Named(name)) => {
                if let Some(schema) = cxt.get(&name) {
                    return Ok((idx, schema));
                } else {
                    return Err(AvrowErr::SchemaNotFoundInUnion);
                }
            }
            _a => {}
        }
    }

    Err(AvrowErr::SchemaNotFoundInUnion)
}

///////////////////////////////////////////////////////////////////////////////
/// From impls for Value
///////////////////////////////////////////////////////////////////////////////

impl From<()> for Value {
    fn from(_v: ()) -> Value {
        Value::Null
    }
}

impl From<String> for Value {
    fn from(v: String) -> Value {
        Value::Str(v)
    }
}

impl<T: Into<Value>> From<HashMap<String, T>> for Value {
    fn from(v: HashMap<String, T>) -> Value {
        let mut map = HashMap::with_capacity(v.len());
        for (k, v) in v.into_iter() {
            map.insert(k, v.into());
        }
        Value::Map(map)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Value {
        Value::Boolean(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Value {
        Value::Bytes(value)
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(value: &'a [u8]) -> Value {
        Value::Bytes(value.to_vec())
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Int(value)
    }
}

impl From<isize> for Value {
    fn from(value: isize) -> Value {
        Value::Int(value as i32)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Value {
        Value::Int(value as i32)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(values: Vec<T>) -> Value {
        let mut new_vec = vec![];
        for i in values {
            new_vec.push(i.into());
        }
        Value::Array(new_vec)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Value {
        Value::Long(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Value {
        Value::Long(value as i64)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Value {
        Value::Float(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Value {
        Value::Double(value)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(value: &'a str) -> Value {
        Value::Str(value.to_string())
    }
}

#[macro_export]
/// Convenient macro to create a avro fixed value
macro_rules! fixed {
    ($vec:tt) => {
        avrow::Value::Fixed($vec)
    };
}

///////////////////////////////////////////////////////////////////////////////
/// Value -> Rust value
///////////////////////////////////////////////////////////////////////////////

impl Value {
    /// Try to retrieve an avro null
    pub fn as_null(&self) -> Result<(), AvrowErr> {
        if let Value::Null = self {
            Ok(())
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro boolean
    pub fn as_boolean(&self) -> Result<&bool, AvrowErr> {
        if let Value::Boolean(b) = self {
            Ok(b)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro int
    pub fn as_int(&self) -> Result<&i32, AvrowErr> {
        if let Value::Int(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro long
    pub fn as_long(&self) -> Result<&i64, AvrowErr> {
        if let Value::Long(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro float
    pub fn as_float(&self) -> Result<&f32, AvrowErr> {
        if let Value::Float(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro double
    pub fn as_double(&self) -> Result<&f64, AvrowErr> {
        if let Value::Double(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro bytes
    pub fn as_bytes(&self) -> Result<&[u8], AvrowErr> {
        if let Value::Bytes(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro string
    pub fn as_string(&self) -> Result<&str, AvrowErr> {
        if let Value::Str(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro record
    pub fn as_record(&self) -> Result<&Record, AvrowErr> {
        if let Value::Record(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve the variant of the enum as a string
    pub fn as_enum(&self) -> Result<&str, AvrowErr> {
        if let Value::Enum(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro array
    pub fn as_array(&self) -> Result<&[Value], AvrowErr> {
        if let Value::Array(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro map
    pub fn as_map(&self) -> Result<&HashMap<String, Value>, AvrowErr> {
        if let Value::Map(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro union
    pub fn as_union(&self) -> Result<&Value, AvrowErr> {
        if let Value::Union(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
    /// Try to retrieve an avro fixed
    pub fn as_fixed(&self) -> Result<&[u8], AvrowErr> {
        if let Value::Fixed(v) = self {
            Ok(v)
        } else {
            Err(AvrowErr::ExpectedVariantNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Record;
    use crate::from_value;
    use crate::Schema;
    use crate::Value;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;
    use std::str::FromStr;

    #[test]
    fn record_from_btree() {
        let mut rec = BTreeMap::new();
        rec.insert("foo", "bar");
        let _r = Record::from_btree("test", rec).unwrap();
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct SomeRecord {
        one: Vec<u8>,
        two: Vec<u8>,
    }

    #[test]
    fn named_schema_resolves() {
        let schema = r##"
            {
                "type": "record",
                "name": "SomeRecord",
                "aliases": ["MyRecord"],
                "fields" : [
                {"name": "one", "type":{"type": "fixed", "size": 5, "name": "md5"}},
                {"name": "two", "type":"md5"}
                ]
            }
            "##;

        let schema = crate::Schema::from_str(schema).unwrap();
        let mut writer = crate::Writer::with_codec(&schema, vec![], crate::Codec::Null).unwrap();

        let value = SomeRecord {
            one: vec![0u8, 1, 2, 3, 4],
            two: vec![0u8, 1, 2, 3, 4],
        };

        writer.serialize(&value).unwrap();

        let output = writer.into_inner().unwrap();
        let reader = crate::Reader::new(output.as_slice()).unwrap();
        for i in reader {
            let r: SomeRecord = from_value(&i).unwrap();
            assert_eq!(r, value);
        }
    }

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
    #[test]
    fn record_from_json() {
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
        )
        .unwrap();

        let json = serde_json::from_str(
            r##"
        { "name": "bob",
          "github_handle":"ghbob",
          "active": true,
          "mentees":{"id":1, "username":"alice"} }"##,
        )
        .unwrap();
        let rec = super::Record::from_json(json, &schema).unwrap();
        let mut writer = crate::Writer::new(&schema, vec![]).unwrap();
        writer.write(rec).unwrap();
        let avro_data = writer.into_inner().unwrap();
        let reader = crate::Reader::new(avro_data.as_slice()).unwrap();
        for value in reader {
            let _mentors: RustMentors = from_value(&value).unwrap();
        }
    }

    #[test]
    fn record_has_fields_with_default() {
        let schema_str = r##"
        {
            "namespace": "sensor.data",
            "type": "record",
            "name": "common",
            "fields" : [
                {"name": "data", "type": ["null", "string"], "default": null}
            ]
        }
"##;

        let sample_data = r#"{
            "data": null
        }"#;

        let serde_json = serde_json::from_str(sample_data).unwrap();
        let schema = Schema::from_str(schema_str).unwrap();
        let rec = Record::from_json(serde_json, &schema).unwrap();
        let field = &rec.as_record().unwrap().fields["data"];
        assert_eq!(field.value, Value::Null);
    }
}
