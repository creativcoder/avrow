// This module contains definition of types that are common across a subset of
// avro Schema implementation.

use crate::error::AvrowErr;
use crate::schema::Variant;
use crate::value::Value;
use serde_json::Value as JsonValue;
use std::fmt::{self, Display};
use std::str::FromStr;

///////////////////////////////////////////////////////////////////////////////
/// Name implementation for named types: record, fixed, enum
///////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_name(idx: usize, name: &str) -> Result<(), AvrowErr> {
    if name.contains('.')
        || (name.starts_with(|a: char| a.is_ascii_digit()) && idx == 0)
        || name.is_empty()
        || !name.chars().any(|a| a.is_ascii_alphanumeric() || a == '_')
    {
        Err(AvrowErr::InvalidName)
    } else {
        Ok(())
    }
}

// Follows the grammer: <empty> | <name>[(<dot><name>)*]
pub(crate) fn validate_namespace(s: &str) -> Result<(), AvrowErr> {
    let split = s.split('.');
    for (i, n) in split.enumerate() {
        let _ = validate_name(i, n).map_err(|_| AvrowErr::InvalidNamespace)?;
    }
    Ok(())
}

/// Represents the `fullname` attribute
/// of a named avro type i.e, Record, Fixed and Enum.
#[derive(Debug, Clone, Eq, PartialOrd, Ord)]
pub struct Name {
    pub(crate) name: String,
    pub(crate) namespace: Option<String>,
}

impl Name {
    // Creates a new name with validation. This will extract the namespace if a dot is present in `name`
    // Any further calls to set_namespace, will be a noop if the name already contains a dot.
    pub(crate) fn new(name: &str) -> Result<Self, AvrowErr> {
        let mut namespace = None;
        let name = if name.contains('.') {
            // should not have multiple dots and dots in end or start
            let _ = validate_namespace(name)?;
            // strip namespace
            let idx = name.rfind('.').unwrap(); // we check for ., so it's okay
            namespace = Some(name[..idx].to_string());
            let name = &name[idx + 1..];
            validate_name(0, name)?;
            name
        } else {
            validate_name(0, name)?;
            name
        };

        Ok(Self {
            name: name.to_string(),
            namespace,
        })
    }

    pub(crate) fn from_json(
        json: &serde_json::map::Map<String, JsonValue>,
        enclosing_namespace: Option<&str>,
    ) -> Result<Self, AvrowErr> {
        let mut name = if let Some(JsonValue::String(ref s)) = json.get("name") {
            Name::new(s)
        } else {
            return Err(AvrowErr::NameParseFailed);
        }?;

        // As per spec, If the name field has a dot, that is a fullname. any namespace provided is ignored.
        // If no namespace was extracted from the name itself (i.e., name did not contain a dot)
        // we then see if we have the namespace field on the json itself
        // otherwise we use the enclosing namespace if that is a Some(namespace)
        if name.namespace.is_none() {
            if let Some(namespace) = json.get("namespace") {
                if let JsonValue::String(s) = namespace {
                    validate_namespace(s)?;
                    name.set_namespace(s)?;
                }
            } else if let Some(a) = enclosing_namespace {
                validate_namespace(a)?;
                name.set_namespace(a)?;
            }
        }

        Ok(name)
    }

    pub(crate) fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    // receives a mutable json and parses a Name and removes namespace. Used for canonicalization.
    pub(crate) fn from_json_mut(
        json: &mut serde_json::map::Map<String, JsonValue>,
        enclosing_namespace: Option<&str>,
    ) -> Result<Self, AvrowErr> {
        let mut name = if let Some(JsonValue::String(ref s)) = json.get("name") {
            Name::new(s)
        } else {
            return Err(AvrowErr::NameParseFailed);
        }?;

        if name.namespace.is_none() {
            if let Some(namespace) = json.get("namespace") {
                if let JsonValue::String(s) = namespace {
                    validate_namespace(s)?;
                    name.set_namespace(s)?;
                    json.remove("namespace");
                }
            } else if let Some(a) = enclosing_namespace {
                validate_namespace(a)?;
                name.set_namespace(a)?;
            }
        }

        Ok(name)
    }

    pub(crate) fn set_namespace(&mut self, namespace: &str) -> Result<(), AvrowErr> {
        // empty string is a null namespace
        if namespace.is_empty() {
            return Ok(());
        }

        validate_namespace(namespace)?;
        // If a namespace was already extracted when constructing name (name had a dot)
        // then this is a noop
        if self.namespace.is_none() {
            let _ = validate_namespace(namespace)?;
            self.namespace = Some(namespace.to_string());
        }
        Ok(())
    }

    // TODO according to Rust convention, item path separators are :: instead of .
    // should we add a configurable separator?
    pub(crate) fn fullname(&self) -> String {
        if let Some(n) = &self.namespace {
            if n.is_empty() {
                // According to spec, it's fine to put "" as a namespace, which becomes a null namespace
                self.name.to_string()
            } else {
                format!("{}.{}", n, self.name)
            }
        } else {
            self.name.to_string()
        }
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref namespace) = self.namespace {
            write!(f, "{}.{}", namespace, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl FromStr for Name {
    type Err = AvrowErr;

    fn from_str(s: &str) -> Result<Self, AvrowErr> {
        Name::new(s)
    }
}

impl std::convert::TryFrom<&str> for Name {
    type Error = AvrowErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Name::new(value)
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        self.fullname() == other.fullname()
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Ordering for record fields
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Clone)]
pub enum Order {
    Ascending,
    Descending,
    Ignore,
}

impl FromStr for Order {
    type Err = AvrowErr;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ascending" => Ok(Order::Ascending),
            "descending" => Ok(Order::Descending),
            "ignore" => Ok(Order::Ignore),
            _ => Err(AvrowErr::UnknownFieldOrdering),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Record field definition.
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Field {
    pub(crate) name: String,
    pub(crate) ty: Variant,
    pub(crate) default: Option<Value>,
    pub(crate) order: Order,
    pub(crate) aliases: Option<Vec<String>>,
}

// TODO do we also use order for equality?
impl std::cmp::PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.ty == other.ty
    }
}

impl Field {
    pub(crate) fn new(
        name: &str,
        ty: Variant,
        default: Option<Value>,
        order: Order,
        aliases: Option<Vec<String>>,
    ) -> Result<Self, AvrowErr> {
        // According to spec, field names also must adhere to a valid nane.
        validate_name(0, name)?;
        Ok(Field {
            name: name.to_string(),
            ty,
            default,
            order,
            aliases,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validate_namespace;
    use super::Name;

    #[test]
    #[should_panic(expected = "InvalidName")]
    fn name_starts_with_number() {
        Name::new("2org.apache.avro").unwrap();
    }

    #[test]
    #[should_panic(expected = "InvalidNamespace")]
    fn invalid_namespace() {
        let mut name = Name::new("org.apache.avro").unwrap();
        name.set_namespace("23").unwrap();
    }

    #[test]
    fn name_with_seperate_namespace() {
        let mut name = Name::new("hello").unwrap();
        let _ = name.set_namespace("org.foo");
        assert_eq!("org.foo.hello", name.fullname().to_string());
    }

    #[test]
    fn name_contains_dots() {
        let name = Name::new("org.apache.avro").unwrap();
        assert_eq!("avro", name.name.to_string());
        assert_eq!("org.apache.avro", name.fullname().to_string());
    }

    #[test]
    fn fullname_with_empty_namespace() {
        let mut name = Name::new("org.apache.avro").unwrap();
        name.set_namespace("").unwrap();
        assert_eq!("org.apache.avro", name.fullname());
    }

    #[test]
    fn multiple_dots_invalid() {
        let a = "some.namespace..foo";
        assert!(validate_namespace(a).is_err());
    }

    #[test]
    fn name_has_dot_and_namespace_present() {
        let json_str = r##"
            {
            "name":"my.longlist",
            "namespace":"com.some",
            "type":"record"
            }
        "##;
        let json: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let name = Name::from_json(json.as_object().unwrap(), None).unwrap();
        assert_eq!(name.name, "longlist");
        assert_eq!(name.namespace, Some("my".to_string()));
        assert_eq!(name.fullname(), "my.longlist");
    }

    #[test]
    fn name_no_dot_and_namespace_present() {
        let json_str = r##"
            {
            "name":"longlist",
            "namespace":"com.some",
            "type":"record"
            }
        "##;
        let json: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let name = Name::from_json(json.as_object().unwrap(), None).unwrap();
        assert_eq!(name.name, "longlist");
        assert_eq!(name.namespace, Some("com.some".to_string()));
        assert_eq!(name.fullname(), "com.some.longlist");
    }
}
