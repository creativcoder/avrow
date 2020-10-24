use super::common::{Field, Name, Order};
use super::{Schema, Variant};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::str::FromStr;

fn primitive_schema_objects() -> HashMap<&'static str, Variant> {
    let mut s = HashMap::new();
    s.insert(r##"{ "type": "null" }"##, Variant::Null);
    s.insert(r##"{ "type": "boolean" }"##, Variant::Boolean);
    s.insert(r##"{ "type": "int" }"##, Variant::Int);
    s.insert(r##"{ "type": "long" }"##, Variant::Long);
    s.insert(r##"{ "type": "float" }"##, Variant::Float);
    s.insert(r##"{ "type": "double" }"##, Variant::Double);
    s.insert(r##"{ "type": "bytes" }"##, Variant::Bytes);
    s.insert(r##"{ "type": "string" }"##, Variant::Str);
    s
}

fn primitive_schema_canonical() -> HashMap<&'static str, Variant> {
    let mut s = HashMap::new();
    s.insert(r##""null""##, Variant::Null);
    s.insert(r##""boolean""##, Variant::Boolean);
    s.insert(r##""int""##, Variant::Int);
    s.insert(r##""long""##, Variant::Long);
    s.insert(r##""float""##, Variant::Float);
    s.insert(r##""double""##, Variant::Double);
    s.insert(r##""bytes""##, Variant::Bytes);
    s.insert(r##""string""##, Variant::Str);
    s
}

#[test]
fn parse_primitives_as_json_objects() {
    for (s, v) in primitive_schema_objects() {
        let schema = Schema::from_str(s).unwrap();
        assert_eq!(schema.variant, v);
    }
}

#[test]
fn parse_primitives_as_defined_types() {
    for (s, v) in primitive_schema_canonical() {
        let schema = Schema::from_str(s).unwrap();
        assert_eq!(schema.variant, v);
    }
}

#[test]
fn parse_record() {
    let record_schema = Schema::from_str(
        r##"{
        "type": "record",
        "name": "LongOrNull",
        "namespace":"com.test",
        "aliases": ["MaybeLong"],
        "fields" : [
            {"name": "value", "type": "long"},
            {"name": "other", "type": ["null", "LongOrNull"]}
        ]
    }"##,
    )
    .unwrap();

    let union_variants = vec![
        Variant::Null,
        Variant::Named("com.test.LongOrNull".to_string()),
    ];

    let mut fields_map = IndexMap::new();
    fields_map.insert(
        "value".to_string(),
        Field::new("value", Variant::Long, None, Order::Ascending, None).unwrap(),
    );
    fields_map.insert(
        "other".to_string(),
        Field::new(
            "other",
            Variant::Union {
                variants: union_variants,
            },
            None,
            Order::Ascending,
            None,
        )
        .unwrap(),
    );

    let mut name = Name::new("LongOrNull").unwrap();
    name.set_namespace("com.test").unwrap();

    assert_eq!(
        record_schema.variant,
        Variant::Record {
            name,
            aliases: Some(vec!["MaybeLong".to_string()]),
            fields: fields_map,
        }
    );
}

#[test]
fn parse_fixed() {
    let fixed_schema =
        Schema::from_str(r##"{"type": "fixed", "size": 16, "name": "md5"}"##).unwrap();
    assert_eq!(
        fixed_schema.variant,
        Variant::Fixed {
            name: Name::new("md5").unwrap(),
            size: 16
        }
    );
}

#[test]
fn parse_enum() {
    let json = r##"{ 
        "type": "enum",
        "name": "Suit",
        "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
    }"##;
    let enum_schema = Schema::from_str(json).unwrap();
    let name = Name::new("Suit").unwrap();
    let mut symbols = vec![];
    symbols.push("SPADES".to_owned());
    symbols.push("HEARTS".to_owned());
    symbols.push("DIAMONDS".to_owned());
    symbols.push("CLUBS".to_owned());

    assert_eq!(
        enum_schema.variant,
        Variant::Enum {
            name,
            aliases: None,
            symbols
        }
    );
}

#[test]
fn parse_array() {
    let json = r##"{"type": "array", "items": "string"}"##;
    let array_schema = Schema::from_str(json).unwrap();
    assert_eq!(
        array_schema.variant,
        Variant::Array {
            items: Box::new(Variant::Str)
        }
    );
}

#[test]
fn parse_map() {
    let map_schema = Schema::from_str(r##"{"type": "map", "values": "long"}"##).unwrap();
    assert_eq!(
        map_schema.variant,
        Variant::Map {
            values: Box::new(Variant::Long)
        }
    );
}

///////////////////////////////////////////////////////////////////////////////
/// Union
///////////////////////////////////////////////////////////////////////////////

#[test]
fn parse_simple_union() {
    let union_schema = Schema::from_str(r##"["null", "string"]"##).unwrap();
    assert_eq!(
        union_schema.variant,
        Variant::Union {
            variants: vec![Variant::Null, Variant::Str]
        }
    );
}

#[test]
#[should_panic]
fn parse_union_duplicate_primitive_fails() {
    let mut results = vec![];
    for i in primitive_schema_canonical() {
        let json = &format!("[{}, {}]", i.0, i.0);
        results.push(Schema::from_str(json).is_err());
    }

    assert!(results.iter().any(|a| !(*a)));
}

#[test]
fn parse_union_with_different_named_type_but_same_schema_succeeds() {
    let union_schema = Schema::from_str(
        r##"[
    {
        "type":"record",
        "name": "record_one",
        "fields" : [
            {"name": "value", "type": "long"}
        ]
    },
    {
        "type":"record",
        "name": "record_two",
        "fields" : [
            {"name": "value", "type": "long"}
        ]
    }]"##,
    );

    assert!(union_schema.is_ok());
}

#[test]
fn parse_union_with_same_named_type_fails() {
    let union_schema = Schema::from_str(
        r##"[
    {
        "type":"record",
        "name": "record_one",
        "fields" : [
            {"name": "value", "type": "long"}
        ]
    },
    {
        "type":"record",
        "name": "record_one",
        "fields" : [
            {"name": "value", "type": "long"}
        ]
    }]"##,
    );

    assert!(union_schema.is_err());
}

#[test]
fn parse_union_field_invalid_default_values() {
    let default_valued_schema = Schema::from_str(
        r##"
    {
        "name": "Company",
        "type": "record",
        "fields": [
            {
                "name": "emp_name",
                "type": "string",
                "doc": "employee name"
            },
            {      
                "name": "bonus",
                "type": ["null", "long"],
                "default": null,
                "doc": "bonus received on a yearly basis"
            },
            {
                "name": "subordinates",
                "type": ["null", {"type": "map", "values": "string"}],
                "default": {"foo":"bar"},
                "doc": "map of subordinates Name and Designation"
            },
            {
                "name": "departments",
                "type":["null", {"type":"array", "items":"string" }],
                "default": ["Sam", "Bob"],
                "doc": "Departments under the employee"
            }
        ]
    }
    "##,
    );

    assert!(default_valued_schema.is_err());
}

#[test]
fn parse_default_values_record() {
    let default_valued_schema = Schema::from_str(
        r##"
    {
        "name": "Company",
        "type": "record",
        "namespace": "com.test.avrow",
        "fields": [
            {      
                "name": "bonus",
                "type": ["null", "long"],
                "default": null,
                "doc": "bonus received on a yearly basis"
            }
        ]
    }
    "##,
    );

    assert!(default_valued_schema.is_ok());
}

#[test]
#[should_panic(expected = "DuplicateSchema")]
fn fails_on_duplicate_schema() {
    let schema = r##"{
        "type": "record",
        "namespace": "test.avro.training",
        "name": "SomeMessage",
        "fields": [{
            "name": "is_error",
            "type": "boolean",
            "default": false
        }, {
            "name": "outcome",
            "type": [{
                "type": "record",
                "name": "SomeMessage",
                "fields": []
            }, {
                "type": "record",
                "name": "ErrorRecord",
                "fields": [{
                    "name": "errors",
                    "type": {
                        "type": "map",
                        "values": "string"
                    },
                    "doc": "doc"
                }]
            }]
        }]
    }"##;

    Schema::from_str(schema).unwrap();
}

#[test]
#[should_panic]
fn parse_immediate_unions_fails() {
    let default_valued_schema = Schema::from_str(
        r##"
    ["null", "string", ["null", "int"]]"##,
    );

    assert!(default_valued_schema.is_ok());
}

#[test]
fn parse_simple_default_values_record() {
    let _default_valued_schema = Schema::from_str(
        r##"
    {
        "name": "com.school.Student",
        "type": "record",
        "fields": [
            {
                    "name": "departments",
                    "type":[{"type":"array", "items":"string" }, "null"],
                    "default": ["Computer Science", "Finearts"],
                    "doc": "Departments of a student"
                }
            ]
        }
    "##,
    )
    .unwrap();
}

#[test]
fn parse_default_record_value_in_union() {
    let schema = Schema::from_str(
        r##"
    {
        "name": "com.big.data.avro.schema.Employee",
        "type": "record",
        "fields": [
            {
                    "name": "departments",
                    "type":[
                        {"type":"record",
                        "name": "dept_name",
                        "fields":[{"name":"id","type": "string"}, {"name":"foo", "type": "null"}] }],
                    "default": {"id": "foo", "foo": null}
                }
            ]
        }
    "##,
    )
    .unwrap();

    if let Variant::Record { fields, .. } = schema.variant {
        match &fields["departments"].default {
            Some(crate::Value::Record(r)) => {
                assert!(r.fields.contains_key("id"));
                assert_eq!(
                    r.fields["id"],
                    crate::value::FieldValue::new(crate::Value::Str("foo".to_string()))
                );
            }
            _ => panic!("should be a record"),
        }
    }
}

#[test]
#[should_panic(expected = "must be defined before use")]
fn named_schema_must_be_defined_before_being_used() {
    let _schema = Schema::from_str(
        r##"{
        "type": "record",
        "name": "LongList",
        "aliases": ["LinkedLongs"],
        "fields" : [
          {"name": "value", "type": "long"},
          {"name": "next", "type": ["null", "OtherList"]}
        ]
      }"##,
    )
    .unwrap();
}

#[test]
fn test_two_instance_schema_equality() {
    let raw_schema = r#"
        {
        "type": "record",
        "name": "User",
        "doc": "Hi there.",
        "fields": [
            {"name": "likes_pizza", "type": "boolean", "default": false},
            {"name": "aa-i32",
            "type": {"type": "array", "items": {"type": "array", "items": "int"}},
            "default": [[0], [12, -1]]}
        ]
        }
    "#;

    let schema = Schema::from_str(raw_schema).unwrap();
    let schema2 = Schema::from_str(raw_schema).unwrap();
    assert_eq!(schema, schema2);
}

#[test]
#[should_panic(expected = "DuplicateField")]
fn duplicate_field_name_in_record_fails() {
    let raw_schema = r#"
        {
        "type": "record",
        "name": "Person",
        "doc": "Hi there.",
        "fields": [
            {"name": "id", "type": "string", "default": "dsf8e8"},
            {"name": "id", "type": "int", "default": 56}
        ]
        }
    "#;

    Schema::from_str(raw_schema).unwrap();
}
