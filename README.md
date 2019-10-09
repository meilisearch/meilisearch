# MeiliDB

[![Build Status](https://dev.azure.com/thomas0884/thomas/_apis/build/status/meilisearch.MeiliDB?branchName=master)](https://dev.azure.com/thomas0884/thomas/_build/latest?definitionId=1&branchName=master)
[![dependency status](https://deps.rs/repo/github/Kerollmops/MeiliDB/status.svg)](https://deps.rs/repo/github/Kerollmops/MeiliDB)
[![License](https://img.shields.io/github/license/Kerollmops/MeiliDB.svg)](https://github.com/Kerollmops/MeiliDB)
[![Rust 1.31+](https://img.shields.io/badge/rust-1.31+-lightgray.svg)](
https://www.rust-lang.org)

A _full-text search database_ based on the fast [LMDB key-value store](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database).

## Features

- Provides [6 default ranking criteria](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/criterion/mod.rs#L14-L19) used to [bucket sort](https://en.wikipedia.org/wiki/Bucket_sort) documents
- Accepts [custom criteria](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/criterion/mod.rs#L24-L33) and can apply them in any custom order
- Support [ranged queries](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/query_builder.rs#L255-L260), useful for paginating results
- Can [distinct](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/query_builder.rs#L241-L246) and [filter](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/query_builder.rs#L223-L235) returned documents based on context defined rules
- Can store complete documents or only [user schema specified fields](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-schema/src/lib.rs#L265-L279)
- The [default tokenizer](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-tokenizer/src/lib.rs) can index latin and kanji based languages
- Returns [the matching text areas](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/lib.rs#L66-L88), useful to highlight matched words in results
- Accepts query time search config like the [searchable attributes](https://github.com/Kerollmops/new-meilidb/blob/dea7e28a45dde897f97742bdd33fcf75d5673502/meilidb-core/src/query_builder.rs#L248-L252)
- Supports run time indexing (incremental indexing)



It uses [RocksDB](https://github.com/facebook/rocksdb) as the internal key-value store. The key-value store allows us to handle updates and queries with small memory and CPU overheads. The whole ranking system is [data oriented](https://github.com/meilisearch/MeiliDB/issues/82) and provides great performances.

You can [read the deep dive](deep-dive.md) if you want more information on the engine, it describes the whole process of generating updates and handling queries or you can take a look at the [typos and ranking rules](typos-ranking-rules.md) if you want to know the default rules used to sort the documents.

We will be proud if you submit issues and pull requests. You can help to grow this project and start contributing by checking [issues tagged "good-first-issue"](https://github.com/meilisearch/MeiliDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). It is a good start!

The project is only a library yet. It means that there is no binary provided yet. To get started, you can check the examples wich are made to work with the data located in the `misc/` folder.

MeiliDB will be a binary in a near future so you will be able to use it as a database out-of-the-box. We should be able to query it using a [to-be-defined](https://github.com/meilisearch/MeiliDB/issues/38) protocol. This is our current goal, [see the milestones](https://github.com/meilisearch/MeiliDB/milestones). In the end, the binary will be a bunch of network protocols and wrappers around the library - which will also be published on [crates.io](https://crates.io). Both the binary and the library will follow the same update cycle.



## Performances

With a database composed of _100 353_ documents with _352_ attributes each and _3_ of them indexed.
So more than _300 000_ fields indexed for _35 million_ stored we can handle more than _2.8k req/sec_ with an average response time of _9 ms_ on an Intel i7-7700 (8) @ 4.2GHz.

Requests are made using [wrk](https://github.com/wg/wrk) and scripted to simulate real users queries.

```
Running 10s test @ http://localhost:2230
  2 threads and 25 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     9.52ms    7.61ms  99.25ms   84.58%
    Req/Sec     1.41k   119.11     1.78k    64.50%
  28080 requests in 10.01s, 7.42MB read
Requests/sec:   2806.46
Transfer/sec:    759.17KB
```

### Notes

The default Rust allocator has recently been [changed to use the system allocator](https://github.com/rust-lang/rust/pull/51241/).
We have seen much better performances when [using jemalloc as the global allocator](https://github.com/alexcrichton/jemallocator#documentation).

## Usage and examples

Currently MeiliDB do not provide an http server but you can run these two examples to try it out.

It creates an index named _movies_ and insert _19 700_ (in batches of _1000_) movies into it.

```bash
cargo run --release --example from_file -- \
    index example.mdb datasets/movies/data.csv \
    --schema datasets/movies/schema.toml \
    --update-group-size 1000
```

Once this is done, you can query this database using the second binary example.

```bash
cargo run --release --example from_file -- \
    search example.mdb
    --number 4 \
    --filter '!adult' \
    id popularity adult original_title
```
