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

_These information are outdated (October 2018) and will be updated soon_

We made some tests on remote machines and found that MeiliDB easily handles a dataset of near 280k products, on a $5/month server with a single vCPU and 1GB of RAM, running the same index, with a simple query:

- near 190 concurrent users with an average response time of 90ms
- 150 concurrent users with an average response time of 70ms
- 100 concurrent users with an average response time of 45ms

Servers were located in Amsterdam and tests were made between two different locations.

### Notes

The default Rust allocator has recently been [changed to use the system allocator](https://github.com/rust-lang/rust/pull/51241/).
We have seen much better performances when [using jemalloc as the global allocator](https://github.com/alexcrichton/jemallocator#documentation).

## Usage and examples

MeiliDB runs with an index like most search engines.
So to test the library you can create one by indexing a simple csv file.

```bash
cargo run --release --example create-database -- test.mdb misc/kaggle.csv
```

Once the command is executed, the index should be in the `test.mdb` folder.

You are now able to run the `query-database` example and play with MeiliDB.

```bash
cargo run --release --example query-database -- test.mdb
```

