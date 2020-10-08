
## Avrow-cli - command line tool to examine avro files [WIP]

Inspired from avro-tools.jar

### Following subcommands are the supported as of now.

```
Usage: target/debug/av <command> [<args>]

av: command line tool for examining avro datafiles.

Options:
  --help            display usage information

Commands:
  getmeta           Get metadata information of the avro datafile.
  getschema         Prints the writer's schema encoded in the provided datafile.
  read              Prints data from datafile as human readable value
  tobytes           Dumps the avro datafile as bytes for debugging purposes
  fingerprint       Prints fingerprint of the canonical form of writer's schema.
  canonical         Prints the canonical form of writer's schema encoded in the
                    provided datafile.
canonical
```

Usage:

```bash
av read -d ./data.avro 
```
