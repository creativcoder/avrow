use crate::read_datafile;
use anyhow::{anyhow, Context};
use avrow::{Header, Reader};
use std::io::Read;
use std::path::PathBuf;
use std::str;

pub fn metadata(datafile: &PathBuf) -> Result<(), anyhow::Error> {
    let mut avro_datafile = read_datafile(datafile)?;
    let header = Header::from_reader(&mut avro_datafile)?;
    for (k, v) in header.metadata() {
        print!("{}\t", k);
        println!(
            "{}",
            str::from_utf8(v).expect("Invalid UTF-8 in avro header")
        );
    }
    Ok(())
}

pub fn read(datafile: &PathBuf) -> Result<(), anyhow::Error> {
    let mut avro_datafile = read_datafile(datafile)?;
    let reader = Reader::new(&mut avro_datafile)?;
    // TODO: remove irrelevant fields
    for i in reader {
        println!("{:?}", i?);
    }

    Ok(())
}

pub fn bytes(datafile: &PathBuf) -> Result<(), anyhow::Error> {
    let mut avro_datafile = read_datafile(datafile)?;
    let mut v = vec![];

    avro_datafile
        .read_to_end(&mut v)
        .with_context(|| "Failed to read datafile")?;

    println!("{:?}", v);
    Ok(())
}

pub fn schema(datafile: &PathBuf) -> Result<(), anyhow::Error> {
    let mut avro_datafile = read_datafile(datafile)?;
    let header = Header::from_reader(&mut avro_datafile)?;
    // TODO print human readable schema
    println!("{}", header.schema());
    Ok(())
}

pub fn fingerprint(datafile: &PathBuf, fingerprint: &str) -> Result<(), anyhow::Error> {
    let mut avro_datafile = read_datafile(datafile)?;
    let header = Header::from_reader(&mut avro_datafile)?;
    match fingerprint.as_ref() {
        "rabin64" => {
            println!("0x{:x}", header.schema().canonical_form().rabin64());
        }
        "sha256" => {
            let mut fingerprint_str = String::new();
            let sha256 = header.schema().canonical_form().sha256();
            for i in sha256 {
                let a = format!("{:x}", i);
                fingerprint_str.push_str(&a);
            }

            println!("{}", fingerprint_str);
        }
        "md5" => {
            let mut fingerprint_str = String::new();
            let md5 = header.schema().canonical_form().md5();
            for i in md5 {
                let a = format!("{:x}", i);
                fingerprint_str.push_str(&a);
            }

            println!("{}", fingerprint_str);
        }
        other => return Err(anyhow!("invalid or unsupported fingerprint: {}", other)),
    }
    Ok(())
}

pub fn canonical(datafile: &PathBuf) -> Result<(), anyhow::Error> {
    let mut avro_datafile = read_datafile(datafile)?;
    let header = Header::from_reader(&mut avro_datafile)?;
    println!("{}", header.schema().canonical_form());
    Ok(())
}
