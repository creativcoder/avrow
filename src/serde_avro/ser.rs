use super::ser_impl::{MapSerializer, SeqSerializer, StructSerializer};
use crate::error::AvrowErr;
use crate::value::Value;
use serde::ser::{self, Serialize};

pub struct SerdeWriter;

/// `to_value` is the serde API for serialization of Rust types to an [avrow::Value](enum.Value.html)
pub fn to_value<T>(value: &T) -> Result<Value, AvrowErr>
where
    T: Serialize,
{
    let mut serializer = SerdeWriter;
    value.serialize(&mut serializer)
}

impl<'b> ser::Serializer for &'b mut SerdeWriter {
    type Ok = Value;
    type Error = AvrowErr;
    type SerializeSeq = SeqSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = Unsupported;
    type SerializeTupleVariant = Unsupported;
    type SerializeStructVariant = Unsupported;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Byte(v as u8))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v as i32))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v as i32))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Long(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        // using the auxiliary avro value
        Ok(Value::Byte(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v as i32))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Int(v as i32))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Long(v as i64))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Float(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Double(v))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Str(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Str(v.to_owned()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        // todo: identify call path to this
        Ok(Value::Bytes(v.to_owned()))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Ok(value.serialize(&mut SerdeWriter)?)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Enum(variant.to_string()))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SeqSerializer::new(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapSerializer::new(len))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(StructSerializer::new(name, len))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(_len))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        unimplemented!("Avro does not support Rust tuple structs");
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        // TODO Is there a way we can map union type to some valid avro type
        Err(AvrowErr::Message(
            "Tuple type is not currently supported as per avro spec".to_string(),
        ))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!("Avro enums does not support struct variants in enum")
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        unimplemented!("Avro does not support newtype struct variants in enums");
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Unsupported types in avro
///////////////////////////////////////////////////////////////////////////////

pub struct Unsupported;

// struct enum variant
impl ser::SerializeStructVariant for Unsupported {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_field<T: ?Sized>(&mut self, _: &'static str, _: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        unimplemented!("Avro enums does not support data in its variant")
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Avro enums does not support data in its variant")
    }
}

// tuple enum variant
impl ser::SerializeTupleVariant for Unsupported {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_field<T: ?Sized>(&mut self, _: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        unimplemented!("Avro enums does not support Rust tuple variants in enums")
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Avro enums does not support Rust tuple variant in enums")
    }
}

// TODO maybe we can map it by looking at the schema
impl ser::SerializeTupleStruct for Unsupported {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        unimplemented!("Avro enums does not support Rust tuple struct")
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Avro enums does not support Rust tuple struct")
    }
}

impl<'a> ser::SerializeTuple for Unsupported {
    type Ok = Value;
    type Error = AvrowErr;

    fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        unimplemented!("Avro enums does not support Rust tuples")
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!("Avro enums does not support Rust tuples")
    }
}
