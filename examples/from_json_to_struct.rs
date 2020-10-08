use anyhow::Error;
use avrow::{from_value, Reader, Record, Schema, Writer};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
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

fn main() -> Result<(), Error> {
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
    )?;

    let json_data = serde_json::from_str(
        r##"
    { "name": "bob",
        "github_handle":"ghbob",
        "active": true,
        "mentees":{"id":1, "username":"alice"} }"##,
    )?;
    let rec = Record::from_json(json_data, &schema)?;
    let mut writer = crate::Writer::new(&schema, vec![])?;
    writer.write(rec)?;

    let avro_data = writer.into_inner()?;
    let reader = Reader::new(avro_data.as_slice())?;
    for value in reader {
        let mentors: RustMentors = from_value(&value)?;
        dbg!(mentors);
    }
    Ok(())
}
