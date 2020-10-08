use crate::schema::Name;
use crate::serde_avro::AvrowErr;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::cmp::PartialEq;

// wrap overflow of 0xc15d213aa4d7a795
const EMPTY: i64 = -4513414715797952619;

static FP_TABLE: once_cell::sync::Lazy<[i64; 256]> = {
    use once_cell::sync::Lazy;
    Lazy::new(|| {
        let mut fp_table: [i64; 256] = [0; 256];
        for i in 0..256 {
            let mut fp = i;
            for _ in 0..8 {
                fp = (fp as u64 >> 1) as i64 ^ (EMPTY & -(fp & 1));
            }
            fp_table[i as usize] = fp;
        }
        fp_table
    })
};

// relevant fields and in order fields according to spec
const RELEVANT_FIELDS: [&str; 7] = [
    "name", "type", "fields", "symbols", "items", "values", "size",
];
/// Represents canonical form of an avro schema. This representation removes irrelevant fields
/// such as docs and aliases in the schema.
/// Fingerprinting methods are available on this instance.
#[derive(Debug, PartialEq)]
pub struct CanonicalSchema(pub(crate) JsonValue);

impl std::fmt::Display for CanonicalSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = serde_json::to_string_pretty(&self.0);
        write!(f, "{}", c.map_err(|_| std::fmt::Error)?)
    }
}

impl CanonicalSchema {
    #[cfg(feature = "sha2")]
    pub fn sha256(&self) -> Vec<u8> {
        use shatwo::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.0.to_string());
        let result = hasher.finalize();
        result.to_vec()
    }

    #[cfg(feature = "md5")]
    pub fn md5(&self) -> Vec<u8> {
        let v = mdfive::compute(self.0.to_string().as_bytes());
        v.to_vec()
    }

    pub fn rabin64(&self) -> i64 {
        let buf = self.0.to_string();
        let buf = buf.as_bytes();
        let mut fp: i64 = EMPTY;

        buf.iter().for_each(|b| {
            let idx = ((fp ^ *b as i64) & 0xff) as usize;
            fp = (fp as u64 >> 8) as i64 ^ FP_TABLE[idx];
        });

        fp
    }
}

// TODO unescape \uXXXX
// pub fn normalize_unescape(s: &str) -> &str {
//     s
// }

// [FULLNAMES] - traverse the `type` field and replace names with fullnames
pub fn normalize_name(
    json_map: &mut serde_json::map::Map<String, JsonValue>,
    enclosing_namespace: Option<&str>,
) -> Result<(), AvrowErr> {
    let name = Name::from_json_mut(json_map, enclosing_namespace)?;

    json_map["name"] = json!(name.fullname());

    if let Some(JsonValue::Array(fields)) = json_map.get_mut("fields") {
        for f in fields.iter_mut() {
            if let JsonValue::Object(ref mut o) = f {
                if let Some(JsonValue::Object(ref mut o)) = o.get_mut("type") {
                    if o.contains_key("name") {
                        normalize_name(o, name.namespace())?;
                    }
                }
            }
        }
    }

    Ok(())
}

// [STRIP]
pub fn normalize_strip(
    schema: &mut serde_json::map::Map<String, JsonValue>,
) -> Result<(), AvrowErr> {
    if schema.contains_key("doc") {
        schema.remove("doc").ok_or(AvrowErr::ParsingCanonicalForm)?;
    }
    if schema.contains_key("aliases") {
        schema
            .remove("aliases")
            .ok_or(AvrowErr::ParsingCanonicalForm)?;
    }

    Ok(())
}

type JsonMap = serde_json::map::Map<String, JsonValue>;

pub fn order_fields(json: &JsonMap) -> Result<JsonMap, AvrowErr> {
    let mut ordered = JsonMap::new();

    for field in RELEVANT_FIELDS.iter() {
        if let Some(value) = json.get(*field) {
            match value {
                JsonValue::Object(m) => {
                    ordered.insert(field.to_string(), json!(order_fields(m)?));
                }
                JsonValue::Array(a) => {
                    let mut obj_arr = vec![];
                    for field in a {
                        match field {
                            JsonValue::Object(m) => {
                                obj_arr.push(json!(order_fields(m)?));
                            }
                            _ => {
                                obj_arr.push(field.clone());
                            }
                        }
                    }

                    ordered.insert(field.to_string(), json!(obj_arr));
                }
                _ => {
                    ordered.insert(field.to_string(), value.clone());
                }
            }
        }
    }

    Ok(ordered)
}

// The following steps in parsing canonical form are handled by serde so we rely on that.
// [INTEGERS] - serde will not parse a string with a zero prefixed integer.
// [WHITESPACE] - serde also eliminates whitespace.
// [STRINGS] - TODO in `normalize_unescape`
// For rest of the steps, we implement them as below
pub(crate) fn normalize_schema(json_schema: &JsonValue) -> Result<JsonValue, AvrowErr> {
    match json_schema {
        // Normalize a complex schema
        JsonValue::Object(ref scm) => {
            // [PRIMITIVES]
            if let Some(JsonValue::String(s)) = scm.get("type") {
                match s.as_ref() {
                    "record" | "enum" | "array" | "maps" | "union" | "fixed" => {}
                    _ => {
                        return Ok(json!(s));
                    }
                }
            }

            let mut schema = scm.clone();
            // [FULLNAMES]
            if schema.contains_key("name") {
                normalize_name(&mut schema, None)?;
            }
            // [ORDER]
            let mut schema = order_fields(&schema)?;
            // [STRIP]
            normalize_strip(&mut schema)?;
            Ok(json!(schema))
        }
        // [PRIMITIVES]
        // Normalize a primitive schema
        a @ JsonValue::String(_) => Ok(json!(a)),
        // Normalize a union schema
        JsonValue::Array(v) => {
            let mut variants = Vec::with_capacity(v.len());
            for i in v {
                let normalized = normalize_schema(i)?;
                variants.push(normalized);
            }
            Ok(json!(v))
        }
        _other => Err(AvrowErr::UnknownSchema),
    }
}

#[cfg(test)]
mod tests {
    use crate::Schema;
    use std::str::FromStr;
    #[test]
    fn canonical_primitives() {
        let schema_str = r##"{"type": "null"}"##;
        let _ = Schema::from_str(schema_str).unwrap();
    }

    #[test]
    #[cfg(feature = "fingerprint")]
    fn canonical_schema_sha256_fingerprint() {
        let header_schema = r##"{"type": "record", "name": "org.apache.avro.file.Header",
            "fields" : [
            {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
            {"name": "meta", "type": {"type": "map", "values": "bytes"}},
            {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
            ]
        }"##;
        let schema = Schema::from_str(header_schema).unwrap();
        let canonical = schema.canonical_form();

        let expected = "809bed56cf47c84e221ad8b13e28a66ed9cd6b1498a43bad9aa0c868205e";
        let found = canonical.sha256();
        let mut fingerprint_str = String::new();
        for i in found {
            let a = format!("{:x}", i);
            fingerprint_str.push_str(&a);
        }

        assert_eq!(expected, fingerprint_str);
    }

    #[test]
    #[cfg(feature = "fingerprint")]
    fn schema_rabin_fingerprint() {
        let schema = r##""null""##;
        let expected = "0x63dd24e7cc258f8a";
        let schema = Schema::from_str(schema).unwrap();
        let canonical = schema.canonical_form();
        let actual = format!("0x{:x}", canonical.rabin64());
        assert_eq!(expected, actual);
    }

    #[test]
    #[cfg(feature = "fingerprint")]
    fn schema_md5_fingerprint() {
        let schema = r##""null""##;
        let expected = "9b41ef67651c18488a8b8bb67c75699";
        let schema = Schema::from_str(schema).unwrap();
        let canonical = schema.canonical_form();
        let actual = canonical.md5();
        let mut fingerprint_str = String::new();
        for i in actual {
            let a = format!("{:x}", i);
            fingerprint_str.push_str(&a);
        }
        assert_eq!(expected, fingerprint_str);
    }
}
