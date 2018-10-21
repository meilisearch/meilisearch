# pentium

A search engine based on the [blog posts serie](https://blog.algolia.com/inside-the-algolia-engine-part-1-indexing-vs-search/) of the great Algolia company.

If you want to be involved in the project you can [read the deep dive](deep-dive.md).

This is a library, this means that binary are not part of this repository
but since I'm still nice I have made some examples for you in the `examples/` folder.



## Performances

We made some tests on remote machines and found that we can handle, on a server that cost 5$/month with 1vCPU and 1GB of ram and on the same index and with a simple query:

- near 190 users with an average response time of 90ms
- 150 users with an average response time of 70ms
- 100 users with an average response time of 45ms

Network is mesured, servers are located in amsterdam and tests are made between two different datacenters.



## Usage and examples

Pentium work with an index like most of the search engines.
So to test the library you can create one by indexing a simple csv file.

```bash
cargo build --release --example csv-indexer
time ./target/release/examples/csv-indexer --stop-words misc/en.stopwords.txt misc/kaggle.csv
```

The `en.stopwords.txt` file here is a simple file that contains one stop word by line (e.g. or, and).

Once the command finished indexing you will have 3 files that compose the index:
- The `xxx.map` represent the fst map.
- The `xxx.idx` represent the doc indexes matching the words in the map.
- The `xxx.sst` is a file that contains all the fields and the values asociated with it, it is passed to the internal RocksDB.

Now you can easily run the `serve-console` or `serve-http` examples with the name of the dump. (e.g. relaxed-colden).

```bash
cargo build --release --example serve-console
./target/release/examples/serve-console relaxed-colden
```

