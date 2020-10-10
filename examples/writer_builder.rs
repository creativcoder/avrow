use anyhow::Error;
use avrow::{Codec, Reader, Schema, WriterBuilder};
use std::str::FromStr;

fn main() -> Result<(), Error> {
    let schema = Schema::from_str(r##""null""##)?;
    let v = vec![];
    let mut writer = WriterBuilder::new()
        .set_codec(Codec::Null)
        .set_schema(&schema)
        .set_datafile(v)
        .set_flush_interval(128_000)
        .build()?;
    writer.serialize(())?;
    let v = writer.into_inner()?;

    let reader = Reader::with_schema(v.as_slice(), &schema)?;
    for i in reader {
        dbg!(i?);
    }

    Ok(())
}
