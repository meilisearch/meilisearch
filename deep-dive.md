# A deep dive in MeiliSearch

On the 15 of May 2019.

MeiliSearch is a full text search engine based on a final state transducer named [fst](https://github.com/BurntSushi/fst) and a key-value store named [sled](https://github.com/spacejam/sled). The goal of a search engine is to store data and to respond to queries as accurate and fast as possible. To achieve this it must save the matching words in an [inverted index](https://en.wikipedia.org/wiki/Inverted_index).

<!-- MarkdownTOC autolink="true" -->

- [Where is the data stored?](#where-is-the-data-stored)
- [What does the key-value store contains?](#what-does-the-key-value-store-contains)
    - [The inverted word index](#the-inverted-word-index)
        - [A final state transducer](#a-final-state-transducer)
        - [Document indexes](#document-indexes)
    - [The schema](#the-schema)
    - [Document attributes](#document-attributes)
- [How is a request processed?](#how-is-a-request-processed)
    - [Query lexemes](#query-lexemes)
    - [Automatons and query index](#automatons-and-query-index)
    - [Sort by criteria](#sort-by-criteria)

<!-- /MarkdownTOC -->

## Where is the data stored?

MeiliSearch is entirely backed by a key-value store like any good database (i.e. Postgres, MySQL). This brings a great flexibility in the way documents can be stored and updates handled along time.

[sled will brings some](https://github.com/spacejam/sled/tree/434533332a3f485e6d2e467023be0a0b55d3a1af#plans) of the [A.C.I.D. properties](https://en.wikipedia.org/wiki/ACID_(computer_science)) to help us be sure the saved data is consistent.



## What does the key-value store contains?

It contain the inverted word index, the schema and the documents fields.

### The inverted word index

[The inverted word index](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-data/src/database/words_index.rs) is a sled Tree dedicated to store and give access to all documents that contains a specific word. The information stored under the word is simply a big ordered array of where in the document the word has been found. In other word, a big list of [`DocIndex`](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-core/src/lib.rs#L35-L51).

#### A final state transducer

_...also abbreviated fst_

This is the first entry point of the engine, you can read more about how it work with the beautiful blog post of @BurntSushi, [Index 1,600,000,000 Keys with Automata and Rust](https://blog.burntsushi.net/transducers/).

To make it short it is a powerful way to store all the words that are present in the indexed documents. You construct it by giving it all the words you want to index. When you want to search in it you can provide any automaton you want, in MeiliSearch [a custom levenshtein automaton](https://github.com/tantivy-search/levenshtein-automata/) is used.

#### Document indexes

The `fst` will only return the words that match with the search automaton but the goal of the search engine is to retrieve all matches in all the documents when a query is made. You want it to return some sort of position in an attribute in a document, an information about where the given word matched.

To make it possible we retrieve all of the `DocIndex` corresponding to all the matching words in the fst, we use the [`WordsIndex`](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-data/src/database/words_index.rs#L11-L21) Tree to get the `DocIndexes` corresponding the words.

### The schema

The schema is a data structure that represents which documents attributes should be stored and which should be indexed. It is stored under a the [`MainIndex`](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-data/src/database/main_index.rs#L12) Tree and given to MeiliSearch only at the creation of an index.

Each document attribute is associated to a unique 16 bit number named [`SchemaAttr`](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-data/src/schema.rs#L186).

In the future, this schema type could be given along with updates, the database could be able to handled a new schema and reindex the database according to the new one.

### Document attributes

When the engine handle a query the result that the requester want is a document, not only the [`Matches`](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-core/src/lib.rs#L62-L88) associated to it, fields of the original document must be returned too.

So MeiliSearch again uses the power of the underlying key-value store and save the documents attributes marked as _STORE_ in the schema. The dedicated Tree for this information is the [`DocumentsIndex`](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-data/src/database/documents_index.rs#L11).

When a document field is saved in the key-value store its value is binary encoded using [message pack](https://github.com/3Hren/msgpack-rust), so a document must be serializable using serde.



## How is a request processed?

Now that we have our inverted index we are able to return results based on a query. In the MeiliSearch universe a query is a simple string containing words.

### Query lexemes

The first step to be able to call the underlying structures is to split the query in words, for that we use a [custom tokenizer](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-tokenizer/src/lib.rs#L82-L84). Note that a tokenizer is specialized for a human language, this is the hard part.

### Automatons and query index

So to query the fst we need an automaton, in MeiliSearch we use a [levenshtein automaton](https://en.wikipedia.org/wiki/Levenshtein_automaton), this automaton is constructed using a string and a maximum distance. According to the [Algolia's blog post](https://blog.algolia.com/inside-the-algolia-engine-part-3-query-processing/#algolia%e2%80%99s-way-of-searching-for-alternatives) we [created the DFAs](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-core/src/automaton.rs#L59-L78) with different settings.

Thanks to the power of the fst library [it is possible to union multiple automatons](https://docs.rs/fst/0.3.2/fst/map/struct.OpBuilder.html#method.union) on the same fst set. The `Stream` is able to return all the matching words. We use these words to find the whole list of `DocIndexes` associated.

With all these informations it is possible [to reconstruct a list of all the `DocIndexes` associated](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-core/src/query_builder.rs#L103-L130) with the words queried.

### Sort by criteria

Now that we are able to get a big list of [DocIndexes](https://github.com/Kerollmops/MeiliSearch/blob/550dc1e99224e386516877450320f694947332d4/src/lib.rs#L21-L36) it is not enough to sort them by criteria, we need more informations like the levenshtein distance or the fact that a query word match exactly the word stored in the fst. So [we stuff it a little bit](https://github.com/Kerollmops/MeiliSearch/blob/550dc1e99224e386516877450320f694947332d4/src/rank/query_builder.rs#L86-L93), and aggregate all these [Matches](https://github.com/Kerollmops/MeiliSearch/blob/550dc1e99224e386516877450320f694947332d4/src/lib.rs#L47-L74) for each document. This way it will be easy to sort a simple vector of document using a bunch of functions.

With this big list of documents and associated matches [we are able to sort only the part of the slice that we want](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-core/src/query_builder.rs#L160-L188) using bucket sorting. [Each criterion](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-core/src/criterion/mod.rs#L95-L101) is evaluated on each subslice without copy, thanks to [GroupByMut](https://docs.rs/slice-group-by/0.2.4/slice_group_by/) which, I hope [will soon be merged](https://github.com/rust-lang/rfcs/pull/2477).

Note that it is possible to customize the criteria used by using the `QueryBuilder::with_criteria` constructor, this way you can implement some custom ranking based on the document attributes using the appropriate structure and the [`document` method](https://github.com/meilisearch/MeiliSearch/blob/3db823de002243004612e36a19b4578d800dab97/meilisearch-data/src/database/index.rs#L86).

At this point, MeiliSearch work is over 🎉
