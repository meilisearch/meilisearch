# MeiliDB

A _full-text search database_ using a key-value store internally.

It uses [RocksDB](https://github.com/facebook/rocksdb) as a built-in database, to store documents and internal data. The key-value store power allow us to handle updates and queries with small memory and CPU overheads.

You can [read the deep dive](deep-dive.md) if you want more information on the engine, it describes the whole process of generating updates and handling queries.

We will be proud if you submit pull requests. It will help to help to grow this project, you can start contributing by checking [issues tagged "good-first-issue"](https://github.com/Kerollmops/MeiliDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). It a good start!

At the moment this project is only a library. It means that it's not prividing yet any binaries. To get started, we provided  some examples in the `examples/` folder that are made to work with the data located in the `misc/` folder.

In a near future MeiliDB, we will provide a binary to execute this project as database, so you will be able to update and query it using a protocol. This will be our final goal, [see the milestones](https://github.com/Kerollmops/MeiliDB/milestones). At the end, MeiliDB will be a bunch of network protocols, and wrappers. We will publish the entire project on https://crates.io, following our usual update cycle.



## Performances

_these information are outdated  (October 2018) It will be updated soon_

We made some tests on remote machines and found that MeiliDB easily handles a dataset of near 280k products, on a $5/month server with a single vCPU and 1GB of RAM, running the same index, with a simple query:

- **near 190 concurrent users with an average response time of 90ms**
- 150 concurrent users with an average response time of 70ms
- 100 concurrent users with an average response time of 45ms

Servers were located in Amsterdam and tests were made between two different locations.

## Usage and examples

MeiliDB runs with an index like most search engines.
So to test the library you can create one by indexing a simple csv file.

```bash
cargo run --release --example create-database -- test.mdb misc/kaggle.csv
```

Once the command is executed, the index should be in the `test.mdb` folder.

You are now able to run the `query-database` example, to play with MeiliDB.

```bash
cargo run --release --example query-database -- test.mdb
```

