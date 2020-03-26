<p align="center">
  <img src="assets/logo.png" alt="MeiliSearch" />
</p>

<h1 align="center">MeiliSearch</h1>

<h4 align="center">
  <a href="https://www.meilisearch.com">Website</a> | 
  <a href="https://blog.meilisearch.com">Blog</a> | 
  <a href="https://fr.linkedin.com/company/meilisearch">LinkedIn</a> | 
  <a href="https://twitter.com/meilisearch">Twitter</a> | 
  <a href="https://docs.meilisearch.com">Documentation</a> | 
  <a href="https://docs.meilisearch.com/resources/faq.html">FAQ</a>
</h4>

[![Build Status](https://github.com/meilisearch/MeiliSearch/workflows/Cargo%20test/badge.svg)](https://github.com/meilisearch/MeiliSearch/actions)
[![dependency status](https://deps.rs/repo/github/meilisearch/MeiliSearch/status.svg)](https://deps.rs/repo/github/meilisearch/MeiliSearch)
[![License](https://img.shields.io/badge/license-MIT-informational)](https://github.com/meilisearch/MeiliSearch/blob/master/LICENSE)
[![Slack](https://img.shields.io/badge/slack-MeiliSearch-blue.svg?logo=slack)](https://slack.meilisearch.com)

⚡ Ultra relevant and instant full-text **search API** 🔍

**MeiliSearch** is a powerful, fast, open-source, easy to use and deploy search engine. Both searching and indexing are highly customizable. Features such as typo-tolerance, filters, and synonyms are provided out-of-the-box.
For more information about features go to [our documentation](https://docs.meilisearch.com/).

[![crates.io demo gif](misc/crates-io-demo.gif)](https://crates.meilisearch.com)
> Meili helps the Rust community find crates on [crates.meilisearch.com](https://crates.meilisearch.com)

## Features
* Search as-you-type experience (answers < 50 milliseconds)
* Full-text search
* Typo tolerant (understands typos and miss-spelling)
* Supports Kanji
* Supports Synonym
* Easy to install, deploy, and maintain
* Whole documents are returned
* Highly customizable
* RESTful API

## Get started

### Deploy the Server

#### Run it using Docker

```bash
docker run -it -p 7700:7700 --rm getmeili/meilisearch
```

#### Installing with Homebrew

```bash
brew update && brew install meilisearch
meilisearch
```

#### Installing with APT

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

If you have the Rust toolchain already installed on your local system, clone the repository and change it to your working directory.

```bash
git clone https://github.com/meilisearch/MeiliSearch.git
cd MeiliSearch
```

In the cloned repository, compile MeiliSearch.

```bash
cargo run --release
```

### Create an Index and Upload Some Documents

Let's create an index! If you need a sample dataset, use [this movie database](https://www.notion.so/meilisearch/A-movies-dataset-to-test-Meili-1cbf7c9cfa4247249c40edfa22d7ca87#b5ae399b81834705ba5420ac70358a65). You can also find it in the `datasets/` directory.

```bash
curl -L 'https://bit.ly/2PAcw9l' -o movies.json
```

MeiliSearch can serve multiple indexes, with different kinds of documents.
It is required to create an index before sending documents to it.

```bash
curl -i -X POST 'http://127.0.0.1:7700/indexes' --data '{ "name": "Movies", "uid": "movies" }'
```

Now that the server knows about your brand new index, you're ready to send it some data.

```bash
curl -i -X POST 'http://127.0.0.1:7700/indexes/movies/documents' \
  --header 'content-type: application/json' \
  --data-binary @movies.json
```

### Search for Documents

#### In command line

The search engine is now aware of your documents and can serve those via a HTTP server.  
The [`jq` command-line tool](https://stedolan.github.io/jq/) can greatly help you read the server responses.

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

#### Use the Web Interface

We also deliver an **out-of-the-box web interface** in which you can test MeiliSearch interactively.

You can access the web interface in your web browser at the root of the server. The default URL is [http://127.0.0.1:7700](http://127.0.0.1:7700). All you need to do is open your web browser and enter MeiliSearch’s address to visit it. This will lead you to a web page with a search bar that allows you to search in a given set of documents.

### Documentation

Now that your MeiliSearch server is up and running, you can learn more about how to tune your search engine in [the documentation](https://docs.meilisearch.com).

## How it works

MeiliSearch uses [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) as the internal key-value store. The key-value store allows to handle updates and queries with small memory and CPU overheads. The whole ranking system is [data oriented](https://github.com/meilisearch/MeiliSearch/issues/82) and ensures great performances.

You can read [this document](deep-dive.md) if you want to dive deeper into the engine. The whole process of generating updates and handling queries is described in it. Besides, to learn the default rules used for sorting documents, you can take a look at this [typos and ranking rules explanation](typos-ranking-rules.md).

### Technical features

- Provides [6 default ranking criteria](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/criterion/mod.rs#L106-L111) used to [bucket sort](https://en.wikipedia.org/wiki/Bucket_sort) documents
- Accepts [custom criteria](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/criterion/mod.rs#L20-L29) and can apply them in any custom order
- Supports [ranged queries](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L342), useful for paginating results
- Can [distinct](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L324-L329) and [filter](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L313-L318) returned documents based on context defined rules
- Searches for [concatenated](https://github.com/meilisearch/MeiliSearch/pull/164) and [splitted query words](https://github.com/meilisearch/MeiliSearch/pull/232) to improve the search quality.
- Can store complete documents or only [user schema specified fields](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/datasets/movies/schema.toml)
- The [default tokenizer](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-tokenizer/src/lib.rs) can index Latin and Kanji based languages
- Returns [the matching text areas](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-types/src/lib.rs#L49-L65), useful to highlight matched words in results
- Accepts query time search config like the [searchable attributes](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/query_builder.rs#L331-L336)
- Supports [runtime incremental indexing](https://github.com/meilisearch/MeiliSearch/blob/3ea5aa18a209b6973b921542d46a79e1c753c163/meilisearch-core/src/store/mod.rs#L143-L212)

## Performances

When processing a dataset composed of _100 353_ documents with _352_ attributes each and _3_ of them indexed, which means more than _300 000_ fields indexed for _35 million_ stored, MeiliSearch is able to carry out more than _2.8k req/sec_ with an average response time of _9 ms_ on an Intel i7-7700 (8) @ 4.2GHz.

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

We also indexed a dataset containing about _12 millions_ cities names in _24 minutes_ on a _8 cores_, _64 GB of RAM_, and a _300 GB NMVe_ SSD machine.<br/>
The size of the resulting database reached _16 GB_ and search results were presented between _30 ms_ and _4 seconds_ for short prefix queries.

### Notes

In Rust 1.32, the allocator has been [changed to use the system allocator](https://blog.rust-lang.org/2019/01/17/Rust-1.32.0.html#jemalloc-is-removed-by-default).
We observed significant performance improvements when [using jemalloc as the global allocator](https://github.com/alexcrichton/jemallocator#documentation).

## Contributing

Hey! We're glad you're thinking about contributing to MeiliSearch! If you think something is missing or could be improved, please open issues and pull requests. If you'd like to help this project grow, we'd love to have you! To start contributing, checking [issues tagged as "good-first-issue"](https://github.com/meilisearch/MeiliSearch/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) is a good start!

### Analytic Events

Once a day, events are being sent to our Amplitude instance so we can know how many people are using MeiliSearch.<br/>
Only information concerning the platform on which the server runs is concerned. No other information is being sent.<br/>
If this doesn't suit you, you can disable these analytics by using the `MEILI_NO_ANALYTICS` env variable.

## Contact

Feel free to contact us about any questions you may have:
* At [bonjour@meilisearch.com](mailto:bonjour@meilisearch.com): English or French is welcome! 🇫🇷 🇬🇧
* Via the chat box available on every page of [our documentation](https://docs.meilisearch.com/) and on [our landing page](https://www.meilisearch.com/).
* By opening an issue.

Every feedback is appreciated. Thank you for your support!

