# MeiliDB

[![Build Status](https://travis-ci.org/Kerollmops/MeiliDB.svg?branch=master)](https://travis-ci.org/Kerollmops/MeiliDB)
[![dependency status](https://deps.rs/repo/github/Kerollmops/MeiliDB/status.svg)](https://deps.rs/repo/github/Kerollmops/MeiliDB)
[![License](https://img.shields.io/github/license/Kerollmops/MeiliDB.svg)](https://github.com/Kerollmops/MeiliDB)
[![Rust 1.31+](https://img.shields.io/badge/rust-1.31+-lightgray.svg)](
https://www.rust-lang.org)

A _full-text search database_ using a key-value store internally.

It uses [RocksDB](https://github.com/facebook/rocksdb) as the internal key-value store. The key-value store allows us to handle updates and queries with small memory and CPU overheads.

You can [read the deep dive](deep-dive.md) if you want more information on the engine, it describes the whole process of generating updates and handling queries.

We will be proud if you submit issues and pull requests. You can help to grow this project and start contributing by checking [issues tagged "good-first-issue"](https://github.com/Kerollmops/MeiliDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). It is a good start!

The project is only a library yet. It means that there is no binary provided yet. To get started, you can check the examples wich are made to work with the data located in the `misc/` folder.

MeiliDB will be a binary in a near future so you will be able to use it as a database out-of-the-box. We should be able to query it using a [to-be-defined](https://github.com/Kerollmops/MeiliDB/issues/38) protocol. This is our current goal, [see the milestones](https://github.com/Kerollmops/MeiliDB/milestones). In the end, the binary will be a bunch of network protocols and wrappers around the library - which will also be published on [crates.io](https://crates.io). Both the binary and the library will follow the same update cycle.



## Performances

With a database composed of _100 353_ documents with _352_ attributes each and _90_ of them indexed.
So nearly _9 million_ fields indexed for _35 million_ stored we can handle more than _1.2k req/sec_ on an Intel i7-7700 (8) @ 4.2GHz.

Requests are made using [wrk](https://github.com/wg/wrk) and scripted to generate real users queries.

```
Running 10s test @ http://localhost:2230
  2 threads and 12 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    18.86ms   49.39ms 614.89ms   95.23%
    Req/Sec   620.41     59.53   790.00     65.00%
  12359 requests in 10.00s, 3.26MB read
Requests/sec:   1235.54
Transfer/sec:    334.22KB
```

### Notes

The default Rust allocator has recently been [changed to use the system allocator](https://github.com/rust-lang/rust/pull/51241/).
We have seen much better performances when [using jemalloc as the global allocator](https://github.com/alexcrichton/jemallocator#documentation).

## Usage and examples

MeiliDB runs with an index like most search engines.
So to test the library you can create one by indexing a simple csv file.

```bash
cargo run --release --example create-database -- test.mdb misc/kaggle.csv --schema schema-example.toml --stop-words misc/fr.stopwords.txt
```

Once the command is executed, the index should be in the `test.mdb` folder. You are now able to run the `query-database` example and play with MeiliDB.

```bash
cargo run --release --example query-database -- test.mdb -n 10 id title
```

