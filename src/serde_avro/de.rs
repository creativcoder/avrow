use super::de_impl::{ArrayDeserializer, ByteSeqDeserializer, MapDeserializer, StructReader};
use crate::error::AvrowErr;

use crate::value::Value;

use serde::de::IntoDeserializer;
use serde::de::{self, Visitor};
use serde::forward_to_deserialize_any;

pub(crate) struct SerdeReader<'de> {
    pub(crate) inner: &'de Value,
}

impl<'de> SerdeReader<'de> {
    pub(crate) fn new(inner: &'de Value) -> Self {
        SerdeReader { inner }
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut SerdeReader<'de> {
    type Error = AvrowErr;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.inner {
            Value::Null => visitor.visit_unit(),
            Value::Boolean(v) => visitor.visit_bool(*v),
            Value::Int(v) => visitor.visit_i32(*v),
            Value::Long(v) => visitor.visit_i64(*v),
            Value::Float(v) => visitor.visit_f32(*v),
            Value::Double(v) => visitor.visit_f64(*v),
            Value::Str(ref v) => visitor.visit_borrowed_str(v),
            Value::Bytes(ref bytes) => visitor.visit_borrowed_bytes(&bytes),
            Value::Array(items) => visitor.visit_seq(ArrayDeserializer::new(&items)),
            Value::Enum(s) => visitor.visit_enum(s.as_str().into_deserializer()),
            _ => Err(AvrowErr::Unsupported),
        }
    }

    forward_to_deserialize_any! {
        unit bool u8 i8 i16 i32 i64 u16 u32 u64 f32 f64 str bytes byte_buf string ignored_any enum
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.inner {
            Value::Array(ref items) => visitor.visit_seq(ArrayDeserializer::new(items)),
            // TODO figure out the correct byte stram to use
            Value::Bytes(buf) | Value::Fixed(buf) => {
                let byte_seq_deser = ByteSeqDeserializer { input: buf.iter() };
                visitor.visit_seq(byte_seq_deser)
            }
            Value::Union(v) => match v.as_ref() {
                Value::Array(ref items) => visitor.visit_seq(ArrayDeserializer::new(items)),
                _ => Err(AvrowErr::Unsupported),
            },
            _ => Err(AvrowErr::Unsupported),
        }
    }

    // avro bytes
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // for struct field
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.inner {
            Value::Map(m) => {
                let map_de = MapDeserializer {
                    keys: m.keys(),
                    values: m.values(),
                };
                visitor.visit_map(map_de)
            }
            v => Err(AvrowErr::UnexpectedAvroValue {
                value: format!("{:?}", v),
            }),
        }
    }

    fn deserialize_struct<V>(
        self,
        _a: &'static str,
        _b: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.inner {
            Value::Record(ref r) => visitor.visit_map(StructReader::new(r.fields.iter())),
            Value::Union(ref inner) => match **inner {
                Value::Record(ref rec) => visitor.visit_map(StructReader::new(rec.fields.iter())),
                _ => Err(de::Error::custom("Union variant not a record/struct")),
            },
            _ => Err(de::Error::custom("Must be a record/struct")),
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Not yet supported types
    ///////////////////////////////////////////////////////////////////////////

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // TODO it is not clear to what avro schema can a tuple map to
        Err(AvrowErr::Unsupported)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(AvrowErr::Unsupported)
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(AvrowErr::Unsupported)
    }
}
