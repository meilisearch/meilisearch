<p align="center">
  <img alt="the milli logo" src="logo-black.svg">
</p>

<p align="center">a concurrent indexer combined with fast and relevant search algorithms</p>

## Introduction

This repository contains the core engine used in [Meilisearch].

It contains a library that can manage one and only one index. Meilisearch
manages the multi-index itself. Milli is unable to store updates in a store:
it is the job of something else above and this is why it is only able
to process one update at a time.

This repository contains crates to quickly debug the engine:
 - There are benchmarks located in the `benchmarks` crate.
 - The `cli` crate is a simple command-line interface that helps run [flamegraph] on top of it.
 - The `filter-parser` crate contains the parser for the Meilisearch filter syntax.
 - The `flatten-serde-json` crate contains the library that flattens serde-json `Value` objects like Elasticsearch does.
 - The `json-depth-checker` crate is used to indicate if a JSON must be flattened.

## How to use it?

Milli is a library that does search things, it must be embedded in a program.
You can compute the documentation of it by using `cargo doc --open`.

Here is an example usage of the library where we insert documents into the engine
and search for one of them right after.

```rust
let path = tempfile::tempdir().unwrap();
let mut options = EnvOpenOptions::new();
options.map_size(10 * 1024 * 1024); // 10 MB
let index = Index::new(options, &path).unwrap();

let mut wtxn = index.write_txn().unwrap();
let content = documents!([
    {
        "id": 2,
        "title": "Prideand Prejudice",
        "au{hor": "Jane Austin",
        "genre": "romance",
        "price$": "3.5$",
    },
    {
        "id": 456,
        "title": "Le Petit Prince",
        "au{hor": "Antoine de Saint-Exupéry",
        "genre": "adventure",
        "price$": "10.0$",
    },
    {
        "id": 1,
        "title": "Wonderland",
        "au{hor": "Lewis Carroll",
        "genre": "fantasy",
        "price$": "25.99$",
    },
    {
        "id": 4,
        "title": "Harry Potter ing fantasy\0lood Prince",
        "au{hor": "J. K. Rowling",
        "genre": "fantasy\0",
    },
]);

let config = IndexerConfig::default();
let indexing_config = IndexDocumentsConfig::default();
let mut builder =
    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config.clone(), |_| ())
        .unwrap();
builder.add_documents(content).unwrap();
builder.execute().unwrap();
wtxn.commit().unwrap();


// You can search in the index now!
let mut rtxn = index.read_txn().unwrap();
let mut search = Search::new(&rtxn, &index);
search.query("horry");
search.limit(10);

let result = search.execute().unwrap();
assert_eq!(result.documents_ids.len(), 1);
```

## Contributing

We're glad you're thinking about contributing to this repository! Feel free to pick an issue, and to ask any question you need. Some points might not be clear and we are available to help you!

Also, we recommend following the [CONTRIBUTING.md](/CONTRIBUTING.md) to create your PR.

[Meilisearch]: https://github.com/meilisearch/meilisearch
[flamegraph]: https://github.com/flamegraph-rs/flamegraph
