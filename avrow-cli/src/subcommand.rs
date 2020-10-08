use crate::read_datafile;
use anyhow::{anyhow, Context};
use argh::FromArgs;
use avrow::{Header, Reader};
use std::io::Read;
use std::path::PathBuf;
use std::str;

#[derive(FromArgs, PartialEq, Debug)]
/// Get metadata information of the avro datafile.
#[argh(subcommand, name = "getmeta")]
pub struct GetMeta {
    /// datafile as input
    #[argh(option, short = 'd')]
    datafile: PathBuf,
}

impl GetMeta {
    pub fn getmeta(&self) -> Result<(), anyhow::Error> {
        let mut avro_datafile = read_datafile(&self.datafile)?;
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
}

#[derive(FromArgs, PartialEq, Debug)]
/// Prints data from datafile in debug format.
#[argh(subcommand, name = "read")]
pub struct ReadData {
    /// datafile as input
    #[argh(option, short = 'd')]
    datafile: PathBuf,
}
impl ReadData {
    pub fn read_data(&self) -> Result<(), anyhow::Error> {
        let mut avro_datafile = read_datafile(&self.datafile)?;
        let reader = Reader::new(&mut avro_datafile)?;
        // TODO: remove irrelevant fields
        for i in reader {
            println!("{:#?}", i?);
        }

        Ok(())
    }
}

#[derive(FromArgs, PartialEq, Debug)]
/// Dumps the avro datafile as bytes for debugging purposes
#[argh(subcommand, name = "tobytes")]
pub struct ToBytes {
    /// datafile as input
    #[argh(option, short = 'd')]
    datafile: PathBuf,
}

impl ToBytes {
    pub fn tobytes(&self) -> Result<(), anyhow::Error> {
        let mut avro_datafile = read_datafile(&self.datafile)?;
        let mut v = vec![];

        avro_datafile
            .read_to_end(&mut v)
            .with_context(|| "Failed to read data file in memory")?;

        println!("{:?}", v);
        Ok(())
    }
}

#[derive(FromArgs, PartialEq, Debug)]
/// Prints the writer's schema encoded in the provided datafile.
#[argh(subcommand, name = "getschema")]
pub struct GetSchema {
    /// datafile as input
    #[argh(option, short = 'd')]
    datafile: PathBuf,
}

impl GetSchema{
    pub fn getschema(&self) -> Result<(), anyhow::Error> {
        let mut avro_datafile = read_datafile(&self.datafile)?;
        let header = Header::from_reader(&mut avro_datafile)?;
        // TODO print human readable schema
        dbg!(header.schema());
        Ok(())
    }
}

#[derive(FromArgs, PartialEq, Debug)]
/// Prints fingerprint of the canonical form of writer's schema.
#[argh(subcommand, name = "fingerprint")]
pub struct Fingerprint {
    /// datafile as input
    #[argh(option, short = 'd')]
    datafile: String,
    /// the fingerprinting algorithm (rabin64 (default), sha256, md5)
    #[argh(option, short = 'f')]
    fingerprint: String,
}
impl Fingerprint {
    pub fn fingerprint(&self) -> Result<(), anyhow::Error> {
        let mut avro_datafile = read_datafile(&self.datafile)?;
        let header = Header::from_reader(&mut avro_datafile)?;
        match self.fingerprint.as_ref() {
            "rabin64" => {
                println!("0x{:x}", header.schema().canonical_form().rabin64());
            },
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
            other => return Err(anyhow!("invalid or unsupported fingerprint: {}", other))
        }
        Ok(())
    }
}

#[derive(FromArgs, PartialEq, Debug)]
/// Prints the canonical form of writer's schema encoded in the provided datafile.
#[argh(subcommand, name = "canonical")]
pub struct Canonical {
        /// datafile as input
    #[argh(option, short = 'd')]
    datafile: String,
}

impl Canonical {
    pub fn canonical(&self) -> Result<(), anyhow::Error> {
        let mut avro_datafile = read_datafile(&self.datafile)?;
        let header = Header::from_reader(&mut avro_datafile)?;
        println!("{}", header.schema().canonical_form());
        Ok(())
    }
}
