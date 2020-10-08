//! avrow-cli is a command line tool to examine and analyze avro data files.
//!
//! Usage: avrow-cli -i <avrodatafile> tojson // This prints the data contained in the <avrodatafile> in a readable format.

mod subcommand;
mod utils;

use argh::FromArgs;
use utils::read_datafile;

use subcommand::{Canonical, Fingerprint, GetMeta, GetSchema, ToBytes, ReadData};

#[derive(Debug, FromArgs)]
/// av: command line tool for examining avro datafiles.
struct AvrowCli {
    #[argh(subcommand)]
    subcommand: SubCommand,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCommand {
    GetMeta(GetMeta),
    GetSchema(GetSchema),
    Read(ReadData),
    ToBytes(ToBytes),
    Fingerprint(Fingerprint),
    Canonical(Canonical),
}

fn main() -> anyhow::Result<()> {
    let flags: AvrowCli = argh::from_env();
    match flags.subcommand {
        SubCommand::GetMeta(cmd) => cmd.getmeta()?,
        SubCommand::Read(cmd) => cmd.read_data()?,
        SubCommand::ToBytes(cmd) => cmd.tobytes()?,
        SubCommand::GetSchema(cmd) => cmd.getschema()?,
        SubCommand::Fingerprint(cmd) => cmd.fingerprint()?,
        SubCommand::Canonical(cmd) => cmd.canonical()?
    }

    Ok(())
}
