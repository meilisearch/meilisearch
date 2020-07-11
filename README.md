# milli
A prototype of concurrent indexing, only contains postings ids

## Introduction

This engine is a prototype, do not use it in production.
This is one of the most advanced search engine I have worked on.
It currently only supports the proximity criterion.

### Compile all the binaries

```bash
cargo build --release --bins
```

## Indexing

It can index mass documents in no much time, I already achieved to index:
 - 109m songs (song and artist name) in 21min and take 29GB on disk.
 - 12m cities (name, timezone and country ID) in 3min13s and take 3.3GB on disk.

All of that on a 39$/month machine with 4cores.

### Index your documents

You can feed the engine with your CSV data:

```bash
./target/release/indexer --db my-data.mmdb ../my-data.csv
```

## Querying

The engine is designed to handle very frequent words like any other word frequency.
This is why you can search for "asia dubai" (the most common timezone) in the countries datasets in no time (59ms) even with 12m documents.

We haven't modified the algorithm to handle queries that are scattered over multiple attributes, this is an open issue (#4).

### Exposing a website to request the database

Once you've indexed the dataset you will be able to access it with your brwoser.

```bash
./target/release/serve -l 0.0.0.0:8700 --db my-data.mmdb
```

## Gaps

There is many ways to make the engine search for too long and consume too much CPU.
This can for example be achieved by querying the engine for "the best of the do" on the songs and subreddits datasets.

There is plenty of way to improve the algorithms and there is and will be new issues explaining potential improvements.
