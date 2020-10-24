//! avrow-cli is a command line tool to examine and analyze avro data files.
//!
//! Usage: `av -d <avrodatafile> read`
//!
//! The above command prints the data contained in the <avrodatafile> in a readable format.
//!

mod subcommand;
mod utils;

use std::path::PathBuf;
use structopt::StructOpt;
use subcommand::{bytes, canonical, fingerprint, metadata, read, schema};
use utils::read_datafile;

#[derive(StructOpt, Debug)]
#[structopt(about = "Command line tool for examining avro datafiles")]
enum AvrowCli {
    #[structopt(
        name = "metadata",
        about = "Get metadata information of the avro datafile"
    )]
    Metadata {
        #[structopt(short)]
        datafile: PathBuf,
    },
    #[structopt(
        name = "schema",
        about = "Prints the writer's schema encoded in the avro datafile"
    )]
    Schema {
        #[structopt(short)]
        datafile: PathBuf,
    },
    #[structopt(
        about = "Prints fingerprint of the canonical form of writer's schema in the avro datafile."
    )]
    Fingerprint {
        #[structopt(short)]
        datafile: PathBuf,
        #[structopt(short)]
        fingerprint: String,
    },
    #[structopt(
        about = "Prints the canonical form of writer's schema encoded in the avro datafile."
    )]
    Canonical {
        #[structopt(short)]
        datafile: PathBuf,
    },
    #[structopt(about = "Prints data in the avro datafile in debug format")]
    Read {
        #[structopt(short)]
        datafile: PathBuf,
    },
    #[structopt(
        name = "bytes",
        about = "Dumps the avro datafile as bytes for debugging purposes"
    )]
    Bytes {
        #[structopt(short)]
        datafile: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    use AvrowCli::*;
    let opt = AvrowCli::from_args();
    match opt {
        Metadata { datafile } => metadata(&datafile)?,
        Schema { datafile } => schema(&datafile)?,
        Canonical { datafile } => canonical(&datafile)?,
        Read { datafile } => read(&datafile)?,
        Bytes { datafile } => bytes(&datafile)?,
        Fingerprint {
            datafile,
            fingerprint: fp,
        } => fingerprint(&datafile, &fp)?,
    }

    Ok(())
}
