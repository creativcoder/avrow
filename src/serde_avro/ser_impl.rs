use super::SerdeWriter;
use crate::error::AvrowErr;
use crate::value::FieldValue;
use crate::value::Record;
use crate::Value;
use serde::Serialize;
use std::collections::HashMap;

pub struct MapSerializer {
    map: HashMap<String, Value>,
}

impl MapSerializer {
    pub fn new(len: Option<usize>) -> Self {
        let map = match len {
            Some(len) => HashMap::with_capacity(len),
            None => HashMap::new(),
        };

        MapSerializer { map }
    }
}

impl serde::ser::SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error>
    where
        K: Serialize,
        V: Serialize,
    {
        let key = key.serialize(&mut SerdeWriter)?;
        if let Value::Str(s) = key {
            let value = value.serialize(&mut SerdeWriter)?;
            self.map.insert(s, value);
            Ok(())
        } else {
            Err(AvrowErr::ExpectedString)
        }
    }

    fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Map(self.map))
    }
}

//////////////////////////////////////////////////////////////////////////////
/// Rust structs to avro record
//////////////////////////////////////////////////////////////////////////////
pub struct StructSerializer {
    name: String,
    fields: indexmap::IndexMap<String, FieldValue>,
}

impl StructSerializer {
    pub fn new(name: &str, len: usize) -> StructSerializer {
        StructSerializer {
            name: name.to_string(),
            fields: indexmap::IndexMap::with_capacity(len),
        }
    }
}

impl serde::ser::SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_field<T: ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.fields.insert(
            name.to_owned(),
            FieldValue::new(value.serialize(&mut SerdeWriter)?),
        );
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let record = Record {
            name: self.name,
            fields: self.fields,
        };
        Ok(Value::Record(record))
    }
}

//////////////////////////////////////////////////////////////////////////////
/// Sequences
//////////////////////////////////////////////////////////////////////////////

pub struct SeqSerializer {
    items: Vec<Value>,
}

impl SeqSerializer {
    pub fn new(len: Option<usize>) -> SeqSerializer {
        let items = match len {
            Some(len) => Vec::with_capacity(len),
            None => Vec::new(),
        };

        SeqSerializer { items }
    }
}

// Helper function to extract a Vec<u8> from a Vec<Value>
// This should only be called by the caller who knows that the items
// in the Vec a Value::Byte(u8).
// NOTE: Does collect on an into_iter() allocate a new vec?
fn as_byte_vec(a: Vec<Value>) -> Vec<u8> {
    a.into_iter()
        .map(|v| {
            if let Value::Byte(b) = v {
                b
            } else {
                unreachable!("Expecting a byte value in the Vec")
            }
        })
        .collect()
}

impl<'a> serde::ser::SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let v = value.serialize(&mut SerdeWriter)?;
        self.items.push(v);
        Ok(())
    }

    // If the items in vec are of Value::Byte(u8) then return a byte array.
    // FIXME: maybe implement Serialize directly for Vec<u8> to avoid this way.
    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.items.first() {
            Some(Value::Byte(_)) => Ok(Value::Bytes(as_byte_vec(self.items))),
            _ => Ok(Value::Array(self.items)),
        }
    }
}

//////////////////////////////////////////////////////////////////////////////
/// Tuples: avro bytes, fixed
//////////////////////////////////////////////////////////////////////////////

impl<'a> serde::ser::SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let v = value.serialize(&mut SerdeWriter)?;
        self.items.push(v);
        Ok(())
    }

    // If the items in vec are of Value::Byte(u8) then return a byte array.
    // FIXME: maybe implement Serialize directly for Vec<u8> to avoid this way.
    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.items.first() {
            Some(Value::Byte(_)) => Ok(Value::Bytes(as_byte_vec(self.items))),
            Some(Value::Fixed(_)) => Ok(Value::Fixed(as_byte_vec(self.items))),
            _ => Ok(Value::Array(self.items)),
        }
    }
}
