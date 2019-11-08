# Features

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



It uses [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) as the internal key-value store. The key-value store allows us to handle updates and queries with small memory and CPU overheads. The whole ranking system is [data oriented](https://github.com/meilisearch/MeiliDB/issues/82) and provides great performances.

You can [read the deep dive](deep-dive.md) if you want more information on the engine, it describes the whole process of generating updates and handling queries or you can take a look at the [typos and ranking rules](typos-ranking-rules.md) if you want to know the default rules used to sort the documents.

We will be proud if you submit issues and pull requests. You can help to grow this project and start contributing by checking [issues tagged "good-first-issue"](https://github.com/meilisearch/MeiliDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22). It is a good start!

The project is only a library yet. It means that there is no binary provided yet. To get started, you can check the examples wich are made to work with the data located in the `datasets/` folder.

MeiliDB will be a binary in a near future so you will be able to use it as a database out-of-the-box. We should be able to query it using HTTP. This is our current goal, [see the milestones](https://github.com/meilisearch/MeiliDB/milestones). In the end, the binary will be a bunch of network protocols and wrappers around the library - which will also be published on [crates.io](https://crates.io). Both the binary and the library will follow the same update cycle.
