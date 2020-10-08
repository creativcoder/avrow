// A hello world example of reading and writing avro data files

use anyhow::Error;
use avrow::from_value;
use avrow::Reader;
use avrow::Schema;
use avrow::Writer;
use std::str::FromStr;

use std::io::Cursor;

fn main() -> Result<(), Error> {
    // Writing data

    // Create a schema
    let schema = Schema::from_str(r##""null""##)?;
    // Create writer using schema and provide a buffer (implements Read) to write to
    let mut writer = Writer::new(&schema, vec![])?;
    // Write the data using write and creating a Value manually.
    writer.write(())?;
    // or the more convenient and intuitive serialize method that takes native Rust types.
    writer.serialize(())?;
    // retrieve the underlying buffer using the buffer method.
    // TODO buffer is not intuive when using a file. into_inner is much better here.
    let buf = writer.into_inner()?;

    // Reading data

    // Create Reader by providing a Read wrapped version of `buf`
    let reader = Reader::new(Cursor::new(buf))?;
    // Use iterator for reading data in an idiomatic manner.
    for i in reader {
        // reading values can fail due to decoding errors, so the return value of iterator is a Option<Result<Value>>
        // it allows one to examine the failure reason on the underlying avro reader.
        dbg!(&i);
        // This value can be converted to a native Rust type using `from_value` method that uses serde underneath.
        let _val: () = from_value(&i)?;
    }

    Ok(())
}
