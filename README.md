# MeiliSearch

[![Build Status](https://github.com/meilisearch/MeiliSearch/workflows/Cargo%20test/badge.svg)](https://github.com/meilisearch/MeiliSearch/actions)
[![dependency status](https://deps.rs/repo/github/meilisearch/MeiliSearch/status.svg)](https://deps.rs/repo/github/meilisearch/MeiliSearch)
[![License](https://img.shields.io/badge/license-MIT-informational)](https://github.com/meilisearch/MeiliSearch/blob/master/LICENSE)

⚡ Ultra relevant and instant full-text search API 🔍

MeiliSearch is a powerful, fast, open-source, easy to use, and deploy search engine. The search and indexation are fully customizable and handles features like typo-tolerance, filters, and synonyms.
For more [details about those features, go to our documentation](https://docs.meilisearch.com/).

[![crates.io demo gif](misc/crates-io-demo.gif)](https://crates.meilisearch.com)
> Meili helps the Rust community find crates on [crates.meilisearch.com](https://crates.meilisearch.com)

## Features
* Search as-you-type experience (answers < 50ms)
* Full-text search
* Typo tolerant (understands typos and spelling mistakes)
* Supports Kanji
* Supports Synonym
* Easy to install, deploy, and maintain
* Whole documents returned
* Highly customizable
* RESTfull API

## Quick Start

### Deploy the Server

#### Run it using Docker

```bash
docker run -it -p 7700:7700 --rm getmeili/meilisearch
```

#### Installation using APT

```bash
echo "deb [trusted=yes] https://apt.fury.io/meilisearch/ /" > /etc/apt/sources.list.d/fury.list
apt update && apt install meilisearch-http
meilisearch
```

#### Download the binary

```bash
curl -L https://install.meilisearch.com | sh
./meilisearch
```

#### Run it on heroku

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/meilisearch/MeiliSearch)

#### Compile and run it from sources

If you have the Rust toolchain already installed, you can compile from the source

```bash
git clone https://github.com/meilisearch/MeiliSearch.git
cd MeiliSearch
cargo run --release
```

### Create an Index and Upload Some Documents

We provide a movie dataset that you can use for testing purposes.

```bash
curl -L 'https://bit.ly/2PAcw9l' -o movies.json
```

MeiliSearch can serve multiple indexes, with different kinds of documents,
therefore, it is required to create the index before sending documents to it.

```bash
curl -i -X POST 'http://127.0.0.1:7700/indexes' --data '{ "name": "Movies", "uid": "movies" }'
```

Now that the server knows about our brand new index, we can send it data.
We provided you a small dataset that is available in the `datasets/` directory.

```bash
curl -i -X POST 'http://127.0.0.1:7700/indexes/movies/documents' \
  --header 'content-type: application/json' \
  --data-binary @movies.json
```

### Search for Documents

#### In command line

The search engine is now aware of our documents and can serve those via our HTTP server again.
The [`jq` command-line tool](https://stedolan.github.io/jq/) can significantly help you read the server responses.

```bash
curl 'http://127.0.0.1:7700/indexes/movies/search?q=botman+robin&limit=2' | jq
```

```json
{
  "hits": [
    {
      "id": "415",
      "title": "Batman & Robin",
      "poster": "https://image.tmdb.org/t/p/w1280/79AYCcxw3kSKbhGpx1LiqaCAbwo.jpg",
      "overview": "Along with crime-fighting partner Robin and new recruit Batgirl...",
      "release_date": "1997-06-20",
    },
    {
      "id": "411736",
      "title": "Batman: Return of the Caped Crusaders",
      "poster": "https://image.tmdb.org/t/p/w1280/GW3IyMW5Xgl0cgCN8wu96IlNpD.jpg",
      "overview": "Adam West and Burt Ward returns to their iconic roles of Batman and Robin...",
      "release_date": "2016-10-08",
    }
  ],
  "offset": 0,
  "limit": 2,
  "processingTimeMs": 1,
  "query": "botman robin"
}
```

#### With the Web Interface

MeiliSearch provides a simple web interface containing a search bar in order to quickly test the instant search experience with a given set of documents.

This web interface is available in your browser at the root of the server. The default URL is [http://127.0.0.1:7700](http://127.0.0.1:7700).

### Documentation

Now, that you have a running MeiliSearch, you can learn more and tune your search engine using [the documentation](https://docs.meilisearch.com).

## How it works

MeiliSearch uses [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) as the internal key-value store. The key-value store allows us to handle updates and queries with small memory and CPU overheads. The whole ranking system is [data oriented](https://github.com/meilisearch/MeiliSearch/issues/82) and provides great performances.

You can [read the deep dive](deep-dive.md) if you want more information on the engine; it describes the whole process of generating updates and handling queries. Also, you can take a look at the [typos and ranking rules](typos-ranking-rules.md) if you want to know the default rules used to sort the documents.

### Technical features

- Provides [6 default ranking criteria](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/criterion/mod.rs#L106-L111) used to [bucket sort](https://en.wikipedia.org/wiki/Bucket_sort) documents
- Accepts [custom criteria](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/criterion/mod.rs#L20-L29) and can apply them in any custom order
- Support [ranged queries](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L342), useful for paginating results
- Can [distinct](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L324-L329) and [filter](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L313-L318) returned documents based on context defined rules
- Searches for [concatenated](https://github.com/meilisearch/MeiliSearch/pull/164) and [splitted query words](https://github.com/meilisearch/MeiliSearch/pull/232) to improve the search quality.
- Can store complete documents or only [user schema specified fields](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/datasets/movies/schema.toml)
- The [default tokenizer](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-tokenizer/src/lib.rs) can index latin and kanji based languages
- Returns [the matching text areas](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-types/src/lib.rs#L49-L65), useful to highlight matched words in results
- Accepts query time search config like the [searchable attributes](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L331-L336)
- Supports [runtime incremental indexing](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/store/mod.rs#L143-L212)

## Performances

With a dataset composed of _100 353_ documents with _352_ attributes each and _3_ of them indexed.
So more than _300 000_ fields indexed for _35 million_ stored we can handle more than _2.8k req/sec_ with an average response time of _9 ms_ on an Intel i7-7700 (8) @ 4.2GHz.

Requests are made using [wrk](https://github.com/wg/wrk) and scripted to simulate real users' queries.

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

We also indexed a dataset containing something like _12 millions_ cities names in _24 minutes_ on a machine with _8 cores_, _64 GB of RAM_, and a _300 GB NMVe_ SSD.<br/>
The resulting database was _16 GB_ and search results were between _30 ms_ and _4 seconds_ for short prefix queries.

### Notes

With Rust 1.32 the allocator has been [changed to use the system allocator](https://blog.rust-lang.org/2019/01/17/Rust-1.32.0.html#jemalloc-is-removed-by-default).
We have seen much better performances when [using jemalloc as the global allocator](https://github.com/alexcrichton/jemallocator#documentation).

## Contributing

We will be glad if you submit issues and pull requests. You can help to grow this project and start contributing by checking [issues tagged "good-first-issue"](https://github.com/meilisearch/MeiliSearch/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). It is a good start!

### Analytic Events

We send events to our Amplitude instance to be aware of the number of people who use MeiliSearch.<br/>
We only send the platform on which the server runs once by day. No other information is sent.<br/>
If you do not want us to send events, you can disable these analytics by using the `MEILI_NO_ANALYTICS` env variable.
