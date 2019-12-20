# A deep dive in MeiliSearch

On the 15 of May 2019.

MeiliSearch is a full text search engine based on a final state transducer named [FST](https://github.com/BurntSushi/fst) and [LMDB](https://dbdb.io/db/lmdb), a fast memory-mapped key-value store. The goal of a search engine is to store data and to respond to queries as accurate and fast as possible. To achieve this it must save the documents words in an [inverted index](https://en.wikipedia.org/wiki/Inverted_index).

<!-- MarkdownTOC autolink="true" -->

- [Where is the data stored?](#where-is-the-data-stored)
- [What does the key-value store contains?](#what-does-the-key-value-store-contains)
    - [The documents words](#the-documents-words)
        - [A final state transducer](#a-final-state-transducer)
        - [Document indexes](#document-indexes)
    - [Document attributes](#document-attributes)
- [How is a request processed?](#how-is-a-request-processed)
    - [Query lexemes](#query-lexemes)
    - [Automatons and query index](#automatons-and-query-index)
    - [Sort by criteria](#sort-by-criteria)

<!-- /MarkdownTOC -->

## Where is the data stored?

MeiliSearch is entirely backed by a key-value store like any good database (i.e. Postgres, MySQL). This brings a great flexibility in the way documents can be stored and updates handled along time.

LMDB is fully transactional and brings [A.C.I.D properties](https://en.wikipedia.org/wiki/ACID_(computer_science)) to help us be sure the saved data is consistent.



## What does the key-value store contains?

It contains all the documents the words, the corresponding postings lists, and the documents fields.

### The documents words

The documents words are all of the words that the documents contains. Those words will be used when searching for documents matching a user query. Associated with the postings lists we can use them to compute the relevancy of any document. Those words are all packed in a final state automata (an [FST](https://docs.rs/fst)). This kind of immutable data structure is akin to a BTree, an ordered tree, where searches of matching states are fast.

The postings lists are the sorted and deduplicated lists of all the matching points in the documents. Those matching points are represented by an ordered list of [DocIndex](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-types/src/lib.rs#L25-L41)es, the document id, document attribute, and offset of the word is stored in it.

#### A final state transducer

_...also abbreviated FST_

This is the first entry point of the engine, you can read more about how it work with the beautiful blog post of @BurntSushi, [Index 1,600,000,000 Keys with Automata and Rust](https://blog.burntsushi.net/transducers/).

To make it short it is a powerful way to store all the words that are present in the indexed documents. You construct it by giving it all the words you want to index. When you want to search in it you can provide any automata you want, in MeiliSearch [a custom levenshtein automaton](https://github.com/tantivy-search/levenshtein-automata/) is used.

#### Document indexes

The `FST` will only return the words that match with the search automata but the goal of the search engine is to retrieve all the matches in all the documents when a query is made. You want it to return some sort of position in an attribute in a document, an information about where the given word matched.

To make it possible we retrieve all of the `DocIndex`es corresponding to all the matching words in the FST. We created [the sdset library](https://docs.rs/sdset) to make operations on those ordered postings lists fast. Each time a document is indexed the matchings corresponding to its content are merged with the already present ones.

### Document attributes

When the engine handle a query the result that the user want is a document, not only the [`DocIndex`](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-types/src/lib.rs#L25-L41)es associated to it, fields of the original document must be returned too.

So MeiliSearch again uses the power of the underlying key-value store and save the documents attributes marked as _Displayed_ in the schema. The dedicated store for this information is the [`DocumentsFields`](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-core/src/store/documents_fields.rs#L10).

When a document field is saved in the key-value store its value is JSON serialized using [serde_json](https://docs.rs/serde_json), so a document must be serializable using serde.



## How is a request processed?

Now that we have our inverted index we are able to return results based on a query. In the MeiliSearch universe a query is a simple string containing words.

### Query lexemes

The first step to be able to call the underlying structures is to split the query in words, for that we use a [custom tokenizer](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-tokenizer/src/lib.rs#L90-L92). Note that a tokenizer is specialized for a language, this is the hard part.

### Automatons and query index

To query the FST we need an automata, in MeiliSearch we use a [levenshtein automaton](https://en.wikipedia.org/wiki/Levenshtein_automaton), this automata is constructed using a string and a maximum distance. According to the [Algolia's blog post](https://blog.algolia.com/inside-the-algolia-engine-part-3-query-processing/#algolia%e2%80%99s-way-of-searching-for-alternatives) we [created the DFAs](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-core/src/automaton/dfa.rs#L17-L39) with different settings.

With all these informations it is possible to reconstruct a list of all the `DocIndex`es associated with the words queried.

### Sort by criteria

Now that we are able to get a big list of `DocIndex`es it is not enough to sort them by criteria, we need more informations likeê the levenshtein distance or the fact that a query word match exactly the word stored in the fst. So [we stuff it a little bit](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-core/src/bucket_sort.rs#L498-L504), and aggregate all these [`BareMatch`](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-core/src/bucket_sort.rs#L320-L326)es for each document. This way it will be easy to sort a simple vector of documents using a bunch of functions.

With this big list of documents and associated matches [we are able to sort only the part of the slice that we want](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-core/src/bucket_sort.rs#L99-L142) using bucket sorting. [Each criterion](https://github.com/meilisearch/MeiliSearch/blob/20b92fcb4c518aea1da5444cca2a6e50f45750eb/meilisearch-core/src/criterion/mod.rs#L30-L59) is evaluated on each subslice without copy, thanks to [GroupByMut](https://docs.rs/slice-group-by/0.2.4/slice_group_by/) which, I hope [will soon be merged](https://github.com/rust-lang/rfcs/pull/2477).

Note that it is possible to customize the criteria used by using the `QueryBuilder::with_criteria` constructor, this way you can implement some custom ranking based on the document attributes.

At this point, MeiliSearch work is over 🎉
