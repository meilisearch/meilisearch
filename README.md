# raptor-rs
Raptor, the new RISE

## Usage

First you need to generate the index files.

```bash
$ cargo build --release
$ time ./target/release/raptor-cli index csv --stop-words stop-words.txt the-csv-file.csv
```

The `stop-words.txt` file here is a simple file that contains one stop word by line.

Once the command finished indexing you will have 3 files that compose the index:
- The `xxx.map` represent the fst map.
- The `xxx.idx` represent the doc indexes matching the words in the map.
- The `xxx.sst` is a file that contains all the fields and the values asociated with it, it is passed to the internal RocksDB.

Now you can easily use `raptor server console` or `raptor serve http` with the name of the dump. (e.g. relaxed-colden).

```bash
$ cargo build --release --default-features --features serve-console
$ ./target/release/raptor-cli serve console --stop-words stop-words.txt relaxed-colden
```

Note: If you have performance issues run the searcher in release mode (i.e. `--release`).
