use anyhow::Error;
use avrow::Schema;
use std::str::FromStr;

fn main() -> Result<(), Error> {
    let schema = Schema::from_str(
        r##"
         {
             "type": "record",
             "name": "LongList",
             "aliases": ["LinkedLongs"],
             "fields" : [
                 {"name": "value", "type": "long"},
                 {"name": "next", "type": ["null", "LongList"]
             }]
         }
     "##,
    )
    .unwrap();
    println!("{}", schema.canonical_form());
    // get the rabin fingerprint of the canonical form.
    dbg!(schema.canonical_form().rabin64());
    Ok(())
}
