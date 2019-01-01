# MeiliDB

[![Build Status](https://travis-ci.org/Kerollmops/MeiliDB.svg?branch=master)](https://travis-ci.org/Kerollmops/MeiliDB)
[![dependency status](https://deps.rs/repo/github/Kerollmops/MeiliDB/status.svg)](https://deps.rs/repo/github/Kerollmops/MeiliDB)
[![License](https://img.shields.io/github/license/Kerollmops/MeiliDB.svg)](https://github.com/Kerollmops/MeiliDB)
[![Rust 1.31+](https://img.shields.io/badge/rust-1.31+-lightgray.svg)](
https://www.rust-lang.org)

A _full-text search database_ using a key-value store internally.

It uses [RocksDB](https://github.com/facebook/rocksdb) like a classic database, to store documents and internal data. The key-value store power allow us to handle updates and queries with small memory and CPU overheads.

You can [read the deep dive](deep-dive.md) if you want more informations on the engine, it describes the whole process of generating updates and handling queries.

We will be proud if you send pull requests to help us grow this project, you can start with [issues tagged "good-first-issue"](https://github.com/Kerollmops/MeiliDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) to start !

At the moment this is a library only, this means that binaries are not part of this repository but since I'm still nice I have made some examples for you in the `examples/` folder that works with the data located in the `misc/` folder.

In a near future MeiliDB we be a binary like any database: updated and queried using some kind of protocol. It is the final goal, [see the milestones](https://github.com/Kerollmops/MeiliDB/milestones). MeiliDB will just be a bunch of network and protocols functions wrapping the library which itself will be published to https://crates.io, following the same update cycle.



## Performances

_these informations have been made with a version dated of october 2018, we must update them_

We made some tests on remote machines and found that we can handle with a dataset of near 280k products, on a server that cost 5$/month with 1vCPU and 1GB of ram and on the same index and with a simple query:

- near 190 users with an average response time of 90ms
- 150 users with an average response time of 70ms
- 100 users with an average response time of 45ms

Network is mesured, servers are located in amsterdam and tests are made between two different datacenters.

### Notes

The default Rust allocator has recently been [changed to use the system allocator](https://github.com/rust-lang/rust/pull/51241/).
We have seen much better performances when [using jemalloc as the global allocator](https://github.com/alexcrichton/jemallocator#documentation).



## Usage and examples

MeiliDB work with an index like most of the search engines.
So to test the library you can create one by indexing a simple csv file.

```bash
cargo run --release --example create-database -- test.mdb misc/kaggle.csv
```

Once the command finished indexing the database should have been saved under the `test.mdb` folder.

Now you can easily run the `query-database` example to check what is stored in it.

```bash
cargo run --release --example query-database -- test.mdb
```

