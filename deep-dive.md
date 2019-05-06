# A deep dive in MeiliDB

On the 9 of december 2018.

MeiliDB is a full text search engine based on a final state transducer named [fst](https://github.com/BurntSushi/fst) and a key-value store named [RocksDB](https://github.com/facebook/rocksdb). The goal of a search engine is to store data and to respond to queries as accurate and fast as possible. To achieve this it must save the data as an [inverted index](https://en.wikipedia.org/wiki/Inverted_index).



<!-- MarkdownTOC autolink="true" -->

- [Where is the data stored?](#where-is-the-data-stored)
- [What does the key-value store contains?](#what-does-the-key-value-store-contains)
    - [The blob type](#the-blob-type)
        - [A final state transducer](#a-final-state-transducer)
        - [Document indexes](#document-indexes)
        - [Document ids](#document-ids)
    - [The schema](#the-schema)
    - [Document attributes](#document-attributes)
- [How is an update handled?](#how-is-an-update-handled)
    - [The merge operation is CPU consuming](#the-merge-operation-is-cpu-consuming)
- [How is a request processed?](#how-is-a-request-processed)
    - [Query lexemes](#query-lexemes)
    - [Automatons and query index](#automatons-and-query-index)
    - [Sort by criteria](#sort-by-criteria)
    - [Retrieve original documents](#retrieve-original-documents)

<!-- /MarkdownTOC -->

## Where is the data stored?

MeiliDB is entirely backed by a key-value store like any good database (i.e. Postgres, MySQL). This brings a great flexibility in the way documents can be stored and updates handled along time.

[RocksDB brings some](https://rocksdb.org/blog/2015/02/27/write-batch-with-index.html) of the [A.C.I.D. properties](https://en.wikipedia.org/wiki/ACID_(computer_science)) to help us be sure the saved data is consistent, for example we use SST files and the key-value store ability to load them in one time to manage updates.

Note that the SST file have the same restriction as the fst, it needs its keys to be added in order at creation.



## What does the key-value store contains?

It contain the blob, the schema and the documents stored attributes.

### The blob type

[The Blob type](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/blob/mod.rs#L16-L19) is a data structure that indicate if an update is a positive or a negative one. In the case where the update is considered positive, the blob will contain [an fst map and the document indexes](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/blob/positive/blob.rs#L15-L18) associated. In the other case it will only contain [all the document ids](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/blob/negative/blob.rs#L12-L14) that must be considered removed.

The Blob type [is stored under the "*data-index*" entry](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/update/positive/update.rs#L497-L499) and marked as [a merge operation](https://github.com/facebook/rocksdb/wiki/Merge-Operator-Implementation) in the key-value store.

#### A final state transducer

_...also abbreviated fst_

This is the first entry point of the engine, you can read more about how it work with the beautiful blog post of @BurntSushi, [Index 1,600,000,000 Keys with Automata and Rust](https://blog.burntsushi.net/transducers/).

To make it short it is a powerful way to store all the words that are present in the indexed documents. You construct it by giving it all the words you want to index associated with a value that, for the moment, can only be an `u64`. When you want to search in it you can provide any automaton you want, in MeiliDB [a custom levenshtein automaton](https://github.com/tantivy-search/levenshtein-automata/) is used.

Note that the number under each word is auto-incremental, each new word have a new number that is greater than the previous one.

Another powerful feature of `fst` is that it can nearly avoid using RAM and be streamed to disk for example, the problem is that the keys must be always added in lexicographic order, so you must sort them before, for the moment MeiliDB uses a [BTreeMap](https://github.com/Kerollmops/raptor-rs/blob/8abdb0a228e2808fe1814a6a0641a4b72d158579/src/metadata/doc_indexes.rs#L107-L112).

#### Document indexes

As it has been specified, the `fst` can only store a number corresponding to a word, an `u64`, but the goal of the search engine is to retrieve a match in a document when a query is made. You want it to return some sort of position in an attribute in a document, an information about where the given word match.

To make it possible, a custom data structure has been developed, the document indexes is composed of two arrays, the ranges array and all the docindexes corresponding to a given range, each range identify the word number. The [DocIndexes](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/data/doc_indexes.rs#L23) type is designed to be streamed when constructed, consumming a minimum amount of ram like the fst. Another advantage is that the slices are accessible in `O(1)` when you know the word associated number.

#### Document ids

This is a simple ordered list of all documents ids which must be considered deleted. It is used with [the sdset library](https://docs.rs/sdset/0.3.0/sdset/duo/struct.DifferenceByKey.html), the docindexes and the `DifferenceByKey` operation builder when merging blobs.

When a blob represent a negative update it only contains this simple slice of deleted documents ids.

### The schema

The schema is a data structure that represents which documents attributes should be stored and which should be indexed. It is stored under the "_data-schema_" entry and given to MeiliDB only at the creation.

Each document attribute is associated to a unique 32 bit number named `SchemaAttr`.

In the future this schema type could be given along with updates and probably be different from the original, the database could be able to handled this document structure and reindex it.

### Document attributes

When the engine handle a query the result that the requester want is a document, not only the [match](https://github.com/Kerollmops/MeiliDB/blob/fc2cdf92596fc002ce278e3aa8718640ac44724d/src/lib.rs#L51-L79) associated to it, fields of the original document must be returned too.

So MeiliDB again uses the power of the underlying key-value store and save the documents attributes marked as _STORE_. The key is prefixed by "_doc_" followed by the 64 bit document id in bytes and the schema attribute number in bytes corresponding to the document attribute stored.

When a document field is saved in the key-value store its value is binary encoded using the [bincode](https://docs.rs/bincode/) library, so a document must be serializable using serde.



## How is an update handled?

First of all an update in MeiliDB is nothing more than [a RocksDB SST file](https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files). It contains the blob and all the documents attributes binary encoded like described above. Note that the blob is stored under the "_data-index_" key marked as [a merge operation](https://github.com/facebook/rocksdb/wiki/Merge-Operator-Implementation).

### The merge operation is CPU consuming

When [the database ingest an update](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/mod.rs#L108-L145) it gives the SST file to the underlying RocksDB, once it has ingested it there is a "_data-index_" entry available, we can request it but the key-value store will call a function before, a merge operation is performed.

This merge operation is done on multiple blobs as you have understood and will compute a [PositiveBlob](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/blob/positive/blob.rs#L15), this type contains the fst and document indexes structures allowing us to search for documents. This two data structures can be considered as the inverted index.

The computation time of this merge is important, RocksDB doesn't keep the previous merged result, it will call our merge operation each time until it decided to do a compaction. So [we must force this compaction earlier](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/mod.rs#L129-L131) when we receive an update to reduce this cost.

This way when we request the "_data-index_" value it will gives us the previously merged positive blob without any other merge overhead.



## How is a request processed?

Now that we have our "_data-index_" we are able to return results based on a query. In the MeiliDB universe a query is a string.

### Query lexemes

The first step to be able to call the underlying structures is to split the query in words, for that we use a [custom tokenizer](https://github.com/Kerollmops/MeiliDB/blob/fc2cdf92596fc002ce278e3aa8718640ac44724d/src/tokenizer/mod.rs) that is not finished for the moment, [there is an open issue](https://github.com/Kerollmops/MeiliDB/issues/3). Note that a tokenizer is specialized for a human language, this is the hard part.

### Automatons and query index

So to query the fst we need an automaton, in MeiliDB we use a [levenshtein automaton](https://en.wikipedia.org/wiki/Levenshtein_automaton), this automaton is constructed using a string and a maximum distance. According to the [Algolia's blog post](https://blog.algolia.com/inside-the-algolia-engine-part-3-query-processing/#algolia%e2%80%99s-way-of-searching-for-alternatives) we [created the DFAs](https://github.com/Kerollmops/MeiliDB/blob/fc2cdf92596fc002ce278e3aa8718640ac44724d/src/automaton.rs#L62-L75) with different settings.

Thanks to the power of the fst library [it is possible to union multiple automatons](https://docs.rs/fst/0.3.2/fst/map/struct.OpBuilder.html#method.union) on the same fst map, it will allow us to know which [automaton returns a word according to its index](https://github.com/Kerollmops/MeiliDB/blob/fc2cdf92596fc002ce278e3aa8718640ac44724d/src/metadata/ops.rs#L111). The `Stream` is able to return all the numbers associated to the words. We use these numbers to find the whole list of `DocIndexes` associated and do the union set operation.

With all these informations it is possible [to reconstruct a list of all the DocIndexes associated](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/rank/query_builder.rs#L62-L99) with the words queried.

### Sort by criteria

Now that we are able to get a big list of [DocIndexes](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/lib.rs#L21-L36) it is not enough to sort them by criteria, we need more informations like the levenshtein distance or the fact that a query word match exactly the word stored in the fst. So [we stuff it a little bit](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/rank/query_builder.rs#L86-L93), and aggregate all these [Matches](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/lib.rs#L47-L74) for each document. This way it will be easy to sort a simple vector of document using a bunch of functions.

With this big list of documents and associated matches [we are able to sort only the part of the slice that we want](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/rank/query_builder.rs#L108-L119) using bucket sorting. [Each criterion](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/rank/criterion/mod.rs#L75-L87) is evaluated on each subslice without copy, thanks to [GroupByMut](https://github.com/Kerollmops/group-by/blob/cab857bae01463dbd0edb99b0e0d7f3624e6c6f5/src/lib.rs#L180-L185) which, I hope [will soon be merged](https://github.com/rust-lang/rfcs/pull/2477).

Note that it is possible to customize the criteria used by using the `QueryBuilder::with_criteria` constructor, this way you can implement some custom ranking based on the document attributes using the appropriate structure and the `retrieve_document` method.

### Retrieve original documents

The [DatabaseView](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/database_view.rs#L18-L24) structure that you must have created to be able to query the database have [two functions](https://github.com/Kerollmops/MeiliDB/blob/550dc1e99224e386516877450320f694947332d4/src/database/database_view.rs#L60-L76) that allows you to retrieve a full (or not) document according to the schema you specified at creation time (i.e. the _STORED_ attributes).

As you can see, these functions force the created type `T` to implement [the serde Deserialize trait](https://docs.rs/serde/1.0.81/serde/trait.Deserialize.html), MeiliDB will use the `bincode::deserialise` function for each attribute to construct your type and return it to you.



At this point, MeiliDB work is over 🎉

