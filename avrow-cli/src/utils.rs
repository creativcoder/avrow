use anyhow::Context;
use anyhow::Result;
use std::path::Path;

// Open an avro datafile for reading avro data
pub(crate) fn read_datafile<P: AsRef<Path>>(path: P) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| "Could not read datafile")
}
