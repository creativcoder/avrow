
## Avrow-cli - command line tool to examine avro files [WIP]

Inspired from avro-tools.jar

## Install

```
cargo install --path .
```
This will install the binary as `av`.

### Following subcommands are the supported as of now.

```
avrow-cli 0.1.0
Command line tool for examining avro datafiles

USAGE:
    av <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    bytes          Dumps the avro datafile as bytes for debugging purposes
    canonical      Prints the canonical form of writer's schema encoded in the avro datafile.
    fingerprint    Prints fingerprint of the canonical form of writer's schema in the avro datafile.
    help           Prints this message or the help of the given subcommand(s)
    metadata       Get metadata information of the avro datafile
    read           Prints data in the avro datafile in debug format
    schema         Prints the writer's schema encoded in the avro datafile
```
