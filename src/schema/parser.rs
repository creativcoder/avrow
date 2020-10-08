use super::common::{Field, Name, Order};
use super::Variant;
use crate::error::io_err;
use crate::error::AvrowErr;
use crate::error::AvrowResult;
use crate::schema::common::validate_name;
use crate::value::FieldValue;
use crate::value::Value;
use indexmap::IndexMap;
use serde_json::{Map, Value as JsonValue};
use std::borrow::ToOwned;
use std::collections::HashMap;

// Wraps a { name -> schema } lookup table to aid parsing named references in complex schemas
// During parsing, the value for each key may get updated as a schema discovers
// more information about the schema during parsing.
#[derive(Debug, Clone)]
pub(crate) struct Registry {
    // TODO: use a reference to Variant?
    cxt: HashMap<String, Variant>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            cxt: HashMap::new(),
        }
    }

    pub(crate) fn get<'a>(&'a self, name: &str) -> Option<&'a Variant> {
        self.cxt.get(name)
    }

    pub(crate) fn parse_schema(
        &mut self,
        value: &JsonValue,
        enclosing_namespace: Option<&str>,
    ) -> Result<Variant, AvrowErr> {
        match value {
            // Parse a complex schema
            JsonValue::Object(ref schema) => self.parse_object(schema, enclosing_namespace),
            // Parse a primitive schema, could also be a named schema reference
            JsonValue::String(ref schema) => self.parse_primitive(&schema, enclosing_namespace),
            // Parse a union schema
            JsonValue::Array(ref schema) => self.parse_union(schema, enclosing_namespace),
            _ => Err(AvrowErr::UnknownSchema),
        }
    }

    fn parse_union(
        &mut self,
        schema: &[JsonValue],
        enclosing_namespace: Option<&str>,
    ) -> Result<Variant, AvrowErr> {
        let mut union_schema = vec![];
        for s in schema {
            let parsed_schema = self.parse_schema(s, enclosing_namespace)?;
            match parsed_schema {
                Variant::Union { .. } => {
                    return Err(AvrowErr::DuplicateSchemaInUnion);
                }
                _ => {
                    if union_schema.contains(&parsed_schema) {
                        return Err(AvrowErr::DuplicateSchemaInUnion);
                    } else {
                        union_schema.push(parsed_schema);
                    }
                }
            }
        }
        Ok(Variant::Union {
            variants: union_schema,
        })
    }

    fn get_fullname(&self, name: &str, enclosing_namespace: Option<&str>) -> String {
        if let Some(namespace) = enclosing_namespace {
            format!("{}.{}", namespace, name)
        } else {
            name.to_string()
        }
    }

    /// Parse a `serde_json::Value` representing a primitive Avro type into a `Schema`.
    fn parse_primitive(
        &mut self,
        schema: &str,
        enclosing_namespace: Option<&str>,
    ) -> Result<Variant, AvrowErr> {
        match schema {
            "null" => Ok(Variant::Null),
            "boolean" => Ok(Variant::Boolean),
            "int" => Ok(Variant::Int),
            "long" => Ok(Variant::Long),
            "double" => Ok(Variant::Double),
            "float" => Ok(Variant::Float),
            "bytes" => Ok(Variant::Bytes),
            "string" => Ok(Variant::Str),
            other if !other.is_empty() => {
                let name = self.get_fullname(other, enclosing_namespace);
                if self.cxt.contains_key(&name) {
                    Ok(Variant::Named(name))
                } else {
                    Err(AvrowErr::SchemaParseErr(io_err(&format!(
                        "named schema `{}` must be defined before use",
                        other
                    ))))
                }
            }
            _ => Err(AvrowErr::InvalidPrimitiveSchema),
        }
    }

    fn parse_record_fields(
        &mut self,
        fields: &[serde_json::Value],
        enclosing_namespace: Option<&str>,
    ) -> Result<IndexMap<String, Field>, AvrowErr> {
        let mut fields_parsed = IndexMap::with_capacity(fields.len());
        for field_obj in fields {
            match field_obj {
                JsonValue::Object(o) => {
                    let name = o
                        .get("name")
                        .and_then(|a| a.as_str())
                        .ok_or(AvrowErr::RecordNameNotFound)?;

                    let ty: &JsonValue = o.get("type").ok_or(AvrowErr::RecordTypeNotFound)?;
                    let mut ty = self.parse_schema(ty, enclosing_namespace)?;

                    // if ty is named use enclosing namespace to construct the fullname
                    if let Some(name) = ty.get_named_mut() {
                        // if parsed type has its own namespace
                        if name.namespace().is_none() {
                            if let Some(namespace) = enclosing_namespace {
                                name.set_namespace(namespace)?;
                            }
                        }
                    }

                    let default = if let Some(v) = o.get("default") {
                        Some(parse_default(v, &ty)?)
                    } else {
                        None
                    };

                    let order = if let Some(order) = o.get("order") {
                        parse_field_order(order)?
                    } else {
                        Order::Ascending
                    };

                    let aliases = parse_aliases(o.get("aliases"));

                    fields_parsed.insert(
                        name.to_string(),
                        Field::new(name, ty, default, order, aliases)?,
                    );
                }
                _ => return Err(AvrowErr::InvalidRecordFieldType),
            }
        }

        Ok(fields_parsed)
    }

    fn parse_object(
        &mut self,
        value: &Map<String, JsonValue>,
        enclosing_namespace: Option<&str>,
    ) -> Result<Variant, AvrowErr> {
        match value.get("type") {
            Some(&JsonValue::String(ref s)) if s == "record" => {
                let rec_name = Name::from_json(value, enclosing_namespace)?;

                // Insert a named reference to support recursive schema definitions.
                self.cxt
                    .insert(rec_name.to_string(), Variant::Named(rec_name.to_string()));

                let fields = if let Some(JsonValue::Array(ref fields_vec)) = value.get("fields") {
                    fields_vec
                } else {
                    return Err(AvrowErr::ExpectedFieldsJsonArray);
                };

                let fields = self.parse_record_fields(fields, {
                    if rec_name.namespace().is_some() {
                        // Most tightly enclosing namespace, which is this namespace
                        rec_name.namespace()
                    } else {
                        enclosing_namespace
                    }
                })?;

                let aliases = parse_aliases(value.get("aliases"));

                let rec = Variant::Record {
                    name: rec_name.clone(),
                    aliases,
                    fields,
                };

                let rec_for_registry = rec.clone();
                let rec_name = rec_name.to_string();

                // if a record schema is being redefined throw an error.
                if let Some(Variant::Named(_)) = self.cxt.get(&rec_name) {
                    self.cxt.insert(rec_name, rec_for_registry);
                } else {
                    return Err(AvrowErr::DuplicateSchema);
                }

                Ok(rec)
            }
            Some(&JsonValue::String(ref s)) if s == "enum" => {
                let name = Name::from_json(value, enclosing_namespace)?;
                let aliases = parse_aliases(value.get("aliases"));
                let mut symbols = vec![];

                if let Some(v) = value.get("symbols") {
                    match v {
                        JsonValue::Array(sym) => {
                            // let mut symbols = Vec::with_capacity(sym.len());
                            for v in sym {
                                let symbol = v.as_str().ok_or(AvrowErr::EnumSymbolParseErr)?;
                                validate_name(0, symbol)?;
                                symbols.push(symbol.to_string());
                            }
                        }
                        other => {
                            return Err(AvrowErr::EnumParseErr(format!("{:?}", other)));
                        }
                    }
                } else {
                    return Err(AvrowErr::EnumSymbolsMissing);
                }

                let name_str = name.fullname();

                let enum_schema = Variant::Enum {
                    name,
                    aliases,
                    symbols,
                };

                self.cxt.insert(name_str, enum_schema.clone());

                Ok(enum_schema)
            }
            Some(&JsonValue::String(ref s)) if s == "array" => {
                let item_missing_err = AvrowErr::SchemaParseErr(io_err(
                    "Array schema must have `items` field defined",
                ));
                let items_schema = value.get("items").ok_or(item_missing_err)?;
                let parsed_items = self.parse_schema(items_schema, enclosing_namespace)?;
                Ok(Variant::Array {
                    items: Box::new(parsed_items),
                })
            }
            Some(&JsonValue::String(ref s)) if s == "map" => {
                let item_missing_err =
                    AvrowErr::SchemaParseErr(io_err("Map schema must have `values` field defined"));
                let items_schema = value.get("values").ok_or(item_missing_err)?;
                let parsed_items = self.parse_schema(items_schema, enclosing_namespace)?;
                Ok(Variant::Map {
                    values: Box::new(parsed_items),
                })
            }
            Some(&JsonValue::String(ref s)) if s == "fixed" => {
                let name = Name::from_json(value, enclosing_namespace)?;
                let size = value.get("size").ok_or(AvrowErr::FixedSizeNotFound)?;
                let name_str = name.fullname();

                let fixed_schema = Variant::Fixed {
                    name,
                    size: size.as_u64().ok_or(AvrowErr::FixedSizeNotNumber)? as usize, // clamp to usize
                };

                self.cxt.insert(name_str, fixed_schema.clone());

                Ok(fixed_schema)
            }
            Some(JsonValue::String(ref s)) if s == "null" => Ok(Variant::Null),
            Some(JsonValue::String(ref s)) if s == "boolean" => Ok(Variant::Boolean),
            Some(JsonValue::String(ref s)) if s == "int" => Ok(Variant::Int),
            Some(JsonValue::String(ref s)) if s == "long" => Ok(Variant::Long),
            Some(JsonValue::String(ref s)) if s == "float" => Ok(Variant::Float),
            Some(JsonValue::String(ref s)) if s == "double" => Ok(Variant::Double),
            Some(JsonValue::String(ref s)) if s == "bytes" => Ok(Variant::Bytes),
            Some(JsonValue::String(ref s)) if s == "string" => Ok(Variant::Str),
            _other => Err(AvrowErr::SchemaParseFailed),
        }
    }
}

// TODO add support if needed
// fn parse_doc(value: Option<&JsonValue>) -> Option<String> {
//     if let Some(JsonValue::String(s)) = value {
//         Some(s.to_string())
//     } else {
//         None
//     }
// }

// Parses the `order` of a field, defaults to `ascending` order
pub(crate) fn parse_field_order(order: &JsonValue) -> AvrowResult<Order> {
    match *order {
        JsonValue::String(ref s) => match &**s {
            "ascending" => Ok(Order::Ascending),
            "descending" => Ok(Order::Descending),
            "ignore" => Ok(Order::Ignore),
            _ => Err(AvrowErr::UnknownFieldOrdering),
        },
        _ => Err(AvrowErr::InvalidFieldOrdering),
    }
}

// Parses aliases of a field
fn parse_aliases(aliases: Option<&JsonValue>) -> Option<Vec<String>> {
    match aliases {
        Some(JsonValue::Array(ref aliases)) => {
            let mut alias_parsed = Vec::with_capacity(aliases.len());
            for a in aliases {
                let a = a.as_str().map(ToOwned::to_owned)?;
                alias_parsed.push(a);
            }
            Some(alias_parsed)
        }
        _ => None,
    }
}

pub(crate) fn parse_default(
    default_value: &JsonValue,
    schema_variant: &Variant,
) -> Result<Value, AvrowErr> {
    match (default_value, schema_variant) {
        (d, Variant::Union { variants }) => {
            let first_variant = variants.first().ok_or(AvrowErr::FailedDefaultUnion)?;
            parse_default(d, first_variant)
        }
        (JsonValue::Null, Variant::Null) => Ok(Value::Null),
        (JsonValue::Bool(v), Variant::Boolean) => Ok(Value::Boolean(*v)),
        (JsonValue::Number(n), Variant::Int) => Ok(Value::Int(n.as_i64().unwrap() as i32)),
        (JsonValue::Number(n), Variant::Long) => Ok(Value::Long(n.as_i64().unwrap())),
        (JsonValue::Number(n), Variant::Float) => Ok(Value::Float(n.as_f64().unwrap() as f32)),
        (JsonValue::Number(n), Variant::Double) => Ok(Value::Double(n.as_f64().unwrap() as f64)),
        (JsonValue::String(n), Variant::Bytes) => Ok(Value::Bytes(n.as_bytes().to_vec())),
        (JsonValue::String(n), Variant::Str) => Ok(Value::Str(n.clone())),
        (JsonValue::Object(v), Variant::Record { name, fields, .. }) => {
            let mut values = IndexMap::with_capacity(v.len());

            for (k, v) in v {
                let parsed_value =
                    parse_default(v, &fields.get(k).ok_or(AvrowErr::DefaultValueParse)?.ty)?;
                values.insert(k.to_string(), FieldValue::new(parsed_value));
            }

            Ok(Value::Record(crate::value::Record {
                fields: values,
                name: name.to_string(),
            }))
        }
        (JsonValue::String(n), Variant::Enum { symbols, .. }) => {
            if symbols.contains(n) {
                Ok(Value::Str(n.clone()))
            } else {
                Err(AvrowErr::EnumSymbolNotPresent)
            }
        }
        (JsonValue::Array(arr), Variant::Array { items }) => {
            let mut default_arr_items: Vec<Value> = Vec::with_capacity(arr.len());
            for v in arr {
                let parsed_default = parse_default(v, items);
                default_arr_items.push(parsed_default?);
            }

            Ok(Value::Array(default_arr_items))
        }
        (
            JsonValue::Object(map),
            Variant::Map {
                values: values_schema,
            },
        ) => {
            let mut values = std::collections::HashMap::with_capacity(map.len());
            for (k, v) in map {
                let parsed_value = parse_default(v, values_schema)?;
                values.insert(k.to_string(), parsed_value);
            }

            Ok(Value::Map(values))
        }

        (JsonValue::String(n), Variant::Fixed { .. }) => Ok(Value::Fixed(n.as_bytes().to_vec())),
        (_d, _s) => Err(AvrowErr::DefaultValueParse),
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::common::Order;
    use crate::schema::Field;
    use crate::schema::Name;
    use crate::schema::Variant;
    use crate::Schema;
    use crate::Value;
    use indexmap::IndexMap;
    use std::str::FromStr;
    #[test]
    fn schema_parse_default_values() {
        let schema = Schema::from_str(
            r##"{
                "type": "record",
                "name": "Can",
                "doc":"Represents a can data",
                "namespace": "com.avrow",
                "aliases": ["my_linked_list"],
                "fields" : [
                    {
                        "name": "next",
                        "type": ["null", "Can"]
                    },
                    {
                        "name": "value",
                        "type": "long",
                        "default": 1,
                        "aliases": ["data"],
                        "order": "descending",
                        "doc": "This field holds the value of the linked list"
                    }
                ]
            }"##,
        )
        .unwrap();

        let mut fields = IndexMap::new();
        let f1 = Field::new(
            "value",
            Variant::Long,
            Some(Value::Long(1)),
            Order::Ascending,
            None,
        )
        .unwrap();
        let f2 = Field::new(
            "next",
            Variant::Union {
                variants: vec![Variant::Null, Variant::Named("com.avrow.Can".to_string())],
            },
            None,
            Order::Ascending,
            None,
        )
        .unwrap();
        fields.insert("value".to_string(), f1);
        fields.insert("next".to_string(), f2);

        let mut name = Name::new("Can").unwrap();
        name.set_namespace("com.avrow").unwrap();

        let s = Variant::Record {
            name,
            aliases: Some(vec!["my_linked_list".to_string()]),
            fields,
        };

        assert_eq!(&s, schema.variant());
    }

    #[test]
    fn nested_record_fields_parses_properly_with_fullnames() {
        let schema = Schema::from_str(r##"{
            "name": "longlist",
            "namespace": "com.some",
            "type":"record",
            "fields": [
                {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": 4, "namespace": "com.bar"}
                },
                {"name": "inner_rec", "type": {"type": "record", "name": "inner_rec", "fields": [
                    {
                        "name": "test",
                        "type": {"type": "fixed", "name":"hello", "size":5}
                    }
                ]}}
            ]
        }"##).unwrap();

        assert!(schema.cxt.cxt.contains_key("com.bar.magic"));
        assert!(schema.cxt.cxt.contains_key("com.some.hello"));
        assert!(schema.cxt.cxt.contains_key("com.some.longlist"));
        assert!(schema.cxt.cxt.contains_key("com.some.inner_rec"));
    }
}
