use super::de::SerdeReader;
use crate::error::AvrowErr;
use crate::value::FieldValue;
use crate::Value;
use indexmap::map::Iter as MapIter;
use serde::de;
use serde::de::DeserializeSeed;
use serde::de::Visitor;
use serde::forward_to_deserialize_any;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::slice::Iter;

pub(crate) struct StructReader<'de> {
    input: MapIter<'de, String, FieldValue>,
    value: Option<&'de FieldValue>,
}

impl<'de> StructReader<'de> {
    pub fn new(input: MapIter<'de, String, FieldValue>) -> Self {
        StructReader { input, value: None }
    }
}

impl<'de> de::MapAccess<'de> for StructReader<'de> {
    type Error = AvrowErr;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => {
                let (ref field, ref value) = item;
                self.value = Some(value);
                seed.deserialize(StrDeserializer { input: &field })
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let a = self.value.take();
        if let Some(a) = a {
            match &a.value {
                Value::Null => seed.deserialize(NullDeserializer),
                value => seed.deserialize(&mut SerdeReader { inner: &value }),
            }
        } else {
            Err(de::Error::custom("Unexpected call to next_value_seed."))
        }
    }
}

pub(crate) struct ArrayDeserializer<'de> {
    input: Iter<'de, Value>,
}

impl<'de> ArrayDeserializer<'de> {
    pub fn new(input: &'de [Value]) -> Self {
        Self {
            input: input.iter(),
        }
    }
}

impl<'de> de::SeqAccess<'de> for ArrayDeserializer<'de> {
    type Error = AvrowErr;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => seed.deserialize(&mut SerdeReader::new(item)).map(Some),
            None => Ok(None),
        }
    }
}

pub(crate) struct ByteSeqDeserializer<'de> {
    pub(crate) input: Iter<'de, u8>,
}

impl<'de> de::SeqAccess<'de> for ByteSeqDeserializer<'de> {
    type Error = AvrowErr;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.input.next() {
            Some(item) => seed.deserialize(ByteDeserializer { byte: item }).map(Some),
            None => Ok(None),
        }
    }
}

pub(crate) struct ByteDeserializer<'de> {
    pub(crate) byte: &'de u8,
}

impl<'de> de::Deserializer<'de> for ByteDeserializer<'de> {
    type Error = AvrowErr;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(*self.byte)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

pub(crate) struct MapDeserializer<'de> {
    pub(crate) keys: Keys<'de, String, Value>,
    pub(crate) values: Values<'de, String, Value>,
}

impl<'de> de::MapAccess<'de> for MapDeserializer<'de> {
    type Error = AvrowErr;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.keys.next() {
            Some(key) => seed.deserialize(StrDeserializer { input: key }).map(Some),
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.values.next() {
            Some(value) => seed.deserialize(&mut SerdeReader::new(value)),
            None => Err(Self::Error::Message(
                "Unexpected call to next_value_seed".to_string(),
            )),
        }
    }
}

pub(crate) struct StrDeserializer<'de> {
    input: &'de str,
}

impl<'de> de::Deserializer<'de> for StrDeserializer<'de> {
    type Error = AvrowErr;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(&self.input)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

pub(crate) struct NullDeserializer;

impl<'de> de::Deserializer<'de> for NullDeserializer {
    type Error = AvrowErr;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_none()
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}
