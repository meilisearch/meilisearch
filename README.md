# raptor-rs
Raptor, the new RISE

## Usage

First you need to generate the index files.

```bash
$ cargo build --release --bin raptor-indexer
$ time ./target/release/raptor-indexer products.json_lines
```

Once the command finished indexing you will have 3 files that compose the index:
- The `xxx.map` represent the fst map.
- The `xxx.idx` represent the doc indexes matching the words in the map.
- The `xxx.sst` is a file that contains all the fields and the values asociated with it, it is passed to the internal RocksDB.

Now you can easily use `raptor-search` or `raptor-http` with only the prefix name of the files. (e.g. relaxed-colden).

```bash
$ cargo run --bin raptor-search -- relaxed-colden
$ cargo run --bin raptor-http -- relaxed-colden
```

Note: If you have performance issues run the searcher in release mode (i.e. `--release`).
