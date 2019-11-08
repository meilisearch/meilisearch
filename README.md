<p align="center">
  <a href="https://www.meilisearch.com">
    <img src="https://www.meilisearch.com/assets/logo-59b9e6f726360eccac4b0cae0e268ef29b64fc9d386ad4a4e3c10c3e02240ade.svg" width="318px" alt="Strapi logo" />
  </a>
</p>
<h3 align="center">Instant search API made simple.</h3>
<p align="center">A instant search engine, working out-of-the-box for user-friendly search experience.

</p>
<br />
<p align="center">
  <a href="https://dev.azure.com/thomas0884/thomas/_build/latest?definitionId=1&branchName=master">
    <img src="https://dev.azure.com/thomas0884/thomas/_apis/build/status/meilisearch.MeiliDB?branchName=master" alt="NPM Version" />
  </a>
  <a href="https://deps.rs/repo/github/meilisearch/MeiliDB">
    <img src="https://deps.rs/repo/github/meilisearch/MeiliDB/status.svg" alt="Monthly download on NPM" />
  </a>
  <a href="https://commonsclause.com/">
    <img src="https://img.shields.io/badge/license-commons%20clause-lightgrey" alt="Travis Build Status" />
  </a>
</p>

-------
<p align="center"> 
  <img align="center" src="https://github.com/meilisearch/MeiliDB/raw/update-readme/misc/crates-io-demo.gif?raw=true" >
</p>
<p align="center"> Crates.io with Meili </p>

## Goal 

Meili thrives in offering an **easy to use and deploy** solution for **full-text search**. No configuration is needed but customization of search and indexation is possible. 

<br />


## Quickstart

Let's index a [dataset of movies](#https://www.notion.so/meilisearch/A-movies-dataset-to-test-Meili-1cbf7c9cfa4247249c40edfa22d7ca87) in which we will search.

### Install & Run

```bash
cargo run --release
```

The search engine is now listening on: `http://127.0.0.1:8080`. 

> You can check with `curl -i http://127.0.0.1:8080/health`

### Index documents

Create an [index](#https://docs.meilisearch.com/indexes.html) without defining the [document](#https://docs.meilisearch.com/documents.html) [schema](#to_do_schema).
```bash
curl --request POST 'http://127.0.0.1:8080/indexes/myindex'
```

Add documents and [learn how to format your documents](#to_do_format).


```bash
curl --request POST 'http://127.0.0.1:8080/indexes/myindex/documents' \
  --header 'content-type: application/json' \
  --data @movies.json
```

You [can track updates](#to_do_updates) with the provided update id's .

### Search 
Now that our movie dataset has been indexed, you can try out the search engine with, for example, `botman` as a query.
```bash
curl 'http://127.0.0.1:8080/indexes/myindex/search?q=botman'
```

```
{
  "hits": [
    {
      "id": "29751",
      "title": "Batman Unmasked: The Psychology of the Dark Knight",
      "overview": "Delve into the world of Batman and the vigilante justice that he brought to the city of Gotham. Batman is a man who, after experiencing great tragedy, devotes his life to an ideal--but what happens when one man takes on the evil underworld alone? Examine why Batman is who he is--and explore how a boy scarred by tragedy becomes a symbol of hope to everyone else.",
    },
    {
      "id": "471474",
      "title": "Batman: Gotham by Gaslight",
      "overview": "In an alternative Victorian Age Gotham City, Batman begins his war on crime while he investigates a new series of murders by Jack the Ripper.",
    },
    ...
  ],
  "offset": 0,
  "limit": 20,
  "processingTimeMs": 1,
  "query": "botman"
}
```

## Features
- Provides [6 default ranking criteria](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/criterion/mod.rs#L107-L113) used to [bucket sort](https://en.wikipedia.org/wiki/Bucket_sort) documents
- Accepts [custom criteria](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/criterion/mod.rs#L24-L33) and can apply them in any custom order
- Support [ranged queries](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/query_builder.rs#L283), useful for paginating results
- Can [distinct](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/query_builder.rs#L265-L270) and [filter](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/query_builder.rs#L246-L259) returned documents based on context defined rules
- Searches for [concatenated](https://github.com/meilisearch/MeiliDB/pull/164) and [splitted query words](https://github.com/meilisearch/MeiliDB/pull/232) to improve the search quality.
- Can store complete documents or only [user schema specified fields](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-schema/src/lib.rs#L265-L279)
- The [default tokenizer](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-tokenizer/src/lib.rs) can index latin and kanji based languages
- Returns [the matching text areas](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/lib.rs#L66-L88), useful to highlight matched words in results
- Accepts query time search config like the [searchable attributes](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/query_builder.rs#L272-L275)
- Supports [runtime incremental indexing](https://github.com/meilisearch/MeiliDB/blob/dc5c42821e1340e96cb90a3da472264624a26326/meilidb-core/src/store/mod.rs#L143-L173)

## Ressources

* [Documentation](https://docs.meilisearch.com)
* [Main concept](#to_do_main_concept)
* [Deep dive](#link)
* [Contributing](#link)
* [SDK's](#link)


## License 

Common clause
