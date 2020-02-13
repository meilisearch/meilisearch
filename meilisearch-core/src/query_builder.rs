use std::ops::Range;
use std::time::Duration;

use crate::database::MainT;
use crate::bucket_sort::{bucket_sort, bucket_sort_with_distinct};
use crate::{criterion::Criteria, Document, DocumentId};
use crate::{reordered_attrs::ReorderedAttrs, store, MResult};

pub struct QueryBuilder<'c, 'f, 'd> {
    criteria: Criteria<'c>,
    searchable_attrs: Option<ReorderedAttrs>,
    filter: Option<Box<dyn Fn(DocumentId) -> bool + 'f>>,
    distinct: Option<(Box<dyn Fn(DocumentId) -> Option<u64> + 'd>, usize)>,
    timeout: Option<Duration>,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    synonyms_store: store::Synonyms,
    prefix_documents_cache_store: store::PrefixDocumentsCache,
    prefix_postings_lists_cache_store: store::PrefixPostingsListsCache,
}

impl<'c, 'f, 'd> QueryBuilder<'c, 'f, 'd> {
    pub fn new(
        main: store::Main,
        postings_lists: store::PostingsLists,
        documents_fields_counts: store::DocumentsFieldsCounts,
        synonyms: store::Synonyms,
        prefix_documents_cache: store::PrefixDocumentsCache,
        prefix_postings_lists_cache: store::PrefixPostingsListsCache,
    ) -> QueryBuilder<'c, 'f, 'd> {
        QueryBuilder::with_criteria(
            main,
            postings_lists,
            documents_fields_counts,
            synonyms,
            prefix_documents_cache,
            prefix_postings_lists_cache,
            Criteria::default(),
        )
    }

    pub fn with_criteria(
        main: store::Main,
        postings_lists: store::PostingsLists,
        documents_fields_counts: store::DocumentsFieldsCounts,
        synonyms: store::Synonyms,
        prefix_documents_cache: store::PrefixDocumentsCache,
        prefix_postings_lists_cache: store::PrefixPostingsListsCache,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, 'f, 'd> {
        QueryBuilder {
            criteria,
            searchable_attrs: None,
            filter: None,
            distinct: None,
            timeout: None,
            main_store: main,
            postings_lists_store: postings_lists,
            documents_fields_counts_store: documents_fields_counts,
            synonyms_store: synonyms,
            prefix_documents_cache_store: prefix_documents_cache,
            prefix_postings_lists_cache_store: prefix_postings_lists_cache,
        }
    }

    pub fn with_filter<F>(&mut self, function: F)
    where
        F: Fn(DocumentId) -> bool + 'f,
    {
        self.filter = Some(Box::new(function))
    }

    pub fn with_fetch_timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout)
    }

    pub fn with_distinct<F, K>(&mut self, function: F, size: usize)
    where
        F: Fn(DocumentId) -> Option<u64> + 'd,
    {
        self.distinct = Some((Box::new(function), size))
    }

    pub fn add_searchable_attribute(&mut self, attribute: u16) {
        let reorders = self.searchable_attrs.get_or_insert_with(ReorderedAttrs::new);
        reorders.insert_attribute(attribute);
    }

    pub fn query(
        self,
        reader: &heed::RoTxn<MainT>,
        query: &str,
        range: Range<usize>,
    ) -> MResult<Vec<Document>> {
        match self.distinct {
            Some((distinct, distinct_size)) => bucket_sort_with_distinct(
                reader,
                query,
                range,
                self.filter,
                distinct,
                distinct_size,
                self.criteria,
                self.searchable_attrs,
                self.main_store,
                self.postings_lists_store,
                self.documents_fields_counts_store,
                self.synonyms_store,
                self.prefix_documents_cache_store,
                self.prefix_postings_lists_cache_store,
            ),
            None => bucket_sort(
                reader,
                query,
                range,
                self.filter,
                self.criteria,
                self.searchable_attrs,
                self.main_store,
                self.postings_lists_store,
                self.documents_fields_counts_store,
                self.synonyms_store,
                self.prefix_documents_cache_store,
                self.prefix_postings_lists_cache_store,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeSet, HashMap};
    use std::iter::FromIterator;

    use fst::{IntoStreamer, Set};
    use meilisearch_schema::IndexedPos;
    use sdset::SetBuf;
    use tempfile::TempDir;

    use crate::DocIndex;
    use crate::automaton::normalize_str;
    use crate::bucket_sort::SimpleMatch;
    use crate::database::Database;
    use crate::store::Index;
    use meilisearch_schema::Schema;

    fn set_from_stream<'f, I, S>(stream: I) -> Set
    where
        I: for<'a> fst::IntoStreamer<'a, Into = S, Item = &'a [u8]>,
        S: 'f + for<'a> fst::Streamer<'a, Item = &'a [u8]>,
    {
        let mut builder = fst::SetBuilder::memory();
        builder.extend_stream(stream).unwrap();
        builder.into_inner().and_then(Set::from_bytes).unwrap()
    }

    fn insert_key(set: &Set, key: &[u8]) -> Set {
        let unique_key = {
            let mut builder = fst::SetBuilder::memory();
            builder.insert(key).unwrap();
            builder.into_inner().and_then(Set::from_bytes).unwrap()
        };

        let union_ = set.op().add(unique_key.into_stream()).r#union();

        set_from_stream(union_)
    }

    fn sdset_into_fstset(set: &sdset::Set<&str>) -> Set {
        let mut builder = fst::SetBuilder::memory();
        let set = SetBuf::from_dirty(set.into_iter().map(|s| normalize_str(s)).collect());
        builder.extend_iter(set.into_iter()).unwrap();
        builder.into_inner().and_then(Set::from_bytes).unwrap()
    }

    const fn doc_index(document_id: u64, word_index: u16) -> DocIndex {
        DocIndex {
            document_id: DocumentId(document_id),
            attribute: 0,
            word_index,
            char_index: 0,
            char_length: 0,
        }
    }

    const fn doc_char_index(document_id: u64, word_index: u16, char_index: u16) -> DocIndex {
        DocIndex {
            document_id: DocumentId(document_id),
            attribute: 0,
            word_index,
            char_index,
            char_length: 0,
        }
    }

    const fn doc_attr_index(document_id: u64, attribute: u16, word_index: u16) -> DocIndex {
        DocIndex {
            document_id: DocumentId(document_id),
            attribute,
            word_index,
            char_index: 0,
            char_length: 0,
        }
    }

    pub struct TempDatabase {
        database: Database,
        index: Index,
        _tempdir: TempDir,
    }

    impl TempDatabase {
        pub fn query_builder(&self) -> QueryBuilder {
            self.index.query_builder()
        }

        pub fn add_synonym(&mut self, word: &str, new: SetBuf<&str>) {
            let db = &self.database;
            let mut writer = db.main_write_txn().unwrap();

            let word = normalize_str(word);

            let alternatives = match self
                .index
                .synonyms
                .synonyms(&writer, word.as_bytes())
                .unwrap()
            {
                Some(alternatives) => alternatives,
                None => fst::Set::default(),
            };

            let new = sdset_into_fstset(&new);
            let new_alternatives =
                set_from_stream(alternatives.op().add(new.into_stream()).r#union());
            self.index
                .synonyms
                .put_synonyms(&mut writer, word.as_bytes(), &new_alternatives)
                .unwrap();

            let synonyms = match self.index.main.synonyms_fst(&writer).unwrap() {
                Some(synonyms) => synonyms,
                None => fst::Set::default(),
            };

            let synonyms_fst = insert_key(&synonyms, word.as_bytes());
            self.index
                .main
                .put_synonyms_fst(&mut writer, &synonyms_fst)
                .unwrap();

            writer.commit().unwrap();
        }
    }

    impl<'a> FromIterator<(&'a str, &'a [DocIndex])> for TempDatabase {
        fn from_iter<I: IntoIterator<Item = (&'a str, &'a [DocIndex])>>(iter: I) -> Self {
            let tempdir = TempDir::new().unwrap();
            let database = Database::open_or_create(&tempdir).unwrap();
            let index = database.create_index("default").unwrap();

            let db = &database;
            let mut writer = db.main_write_txn().unwrap();

            let mut words_fst = BTreeSet::new();
            let mut postings_lists = HashMap::new();
            let mut fields_counts = HashMap::<_, u16>::new();

            let mut schema = Schema::with_identifier("id");

            for (word, indexes) in iter {
                let mut final_indexes = Vec::new();
                for index in indexes {
                    let name = index.attribute.to_string();
                    schema.insert(&name).unwrap();
                    let indexed_pos = schema.set_indexed(&name).unwrap().1;
                    let index = DocIndex {
                        attribute: indexed_pos.0,
                        ..*index
                    };
                    final_indexes.push(index);
                }

                let word = word.to_lowercase().into_bytes();
                words_fst.insert(word.clone());
                postings_lists
                    .entry(word)
                    .or_insert_with(Vec::new)
                    .extend_from_slice(&final_indexes);
                for idx in final_indexes {
                    fields_counts.insert((idx.document_id, idx.attribute, idx.word_index), 1);
                }
            }

            index.main.put_schema(&mut writer, &schema).unwrap();

            let words_fst = Set::from_iter(words_fst).unwrap();

            index.main.put_words_fst(&mut writer, &words_fst).unwrap();

            for (word, postings_list) in postings_lists {
                let postings_list = SetBuf::from_dirty(postings_list);
                index
                    .postings_lists
                    .put_postings_list(&mut writer, &word, &postings_list)
                    .unwrap();
            }

            for ((docid, attr, _), count) in fields_counts {
                let prev = index
                    .documents_fields_counts
                    .document_field_count(&writer, docid, IndexedPos(attr))
                    .unwrap();

                let prev = prev.unwrap_or(0);

                index
                    .documents_fields_counts
                    .put_document_field_count(&mut writer, docid, IndexedPos(attr), prev + count)
                    .unwrap();
            }

            writer.commit().unwrap();

            TempDatabase { database, index, _tempdir: tempdir }
        }
    }

    #[test]
    fn simple() {
        let store = TempDatabase::from_iter(vec![
            ("iphone", &[doc_char_index(0, 0, 0)][..]),
            ("from", &[doc_char_index(0, 1, 1)][..]),
            ("apple", &[doc_char_index(0, 2, 2)][..]),
        ]);

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "iphone from apple", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. }));
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn simple_synonyms() {
        let mut store = TempDatabase::from_iter(vec![("hello", &[doc_index(0, 0)][..])]);

        store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "hello", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "bonjour", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    // #[test]
    // fn prefix_synonyms() {
    //     let mut store = TempDatabase::from_iter(vec![("hello", &[doc_index(0, 0)][..])]);

    //     store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello"]));
    //     store.add_synonym("salut", SetBuf::from_dirty(vec!["hello"]));

    //     let db = &store.database;
    //     let reader = db.main_read_txn().unwrap();

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "sal", 0..20).unwrap();
    //     let mut iter = results.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "bonj", 0..20).unwrap();
    //     let mut iter = results.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "sal blabla", 0..20).unwrap();
    //     let mut iter = results.into_iter();

    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "bonj blabla", 0..20).unwrap();
    //     let mut iter = results.into_iter();

    //     assert_matches!(iter.next(), None);
    // }

    // #[test]
    // fn levenshtein_synonyms() {
    //     let mut store = TempDatabase::from_iter(vec![("hello", &[doc_index(0, 0)][..])]);

    //     store.add_synonym("salutation", SetBuf::from_dirty(vec!["hello"]));

    //     let db = &store.database;
    //     let reader = db.main_read_txn().unwrap();

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "salutution", 0..20).unwrap();
    //     let mut iter = results.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "saluttion", 0..20).unwrap();
    //     let mut iter = results.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);
    // }

    #[test]
    fn harder_synonyms() {
        let mut store = TempDatabase::from_iter(vec![
            ("hello", &[doc_index(0, 0)][..]),
            ("bonjour", &[doc_index(1, 3)]),
            ("salut", &[doc_index(2, 5)]),
        ]);

        store.add_synonym("hello", SetBuf::from_dirty(vec!["bonjour", "salut"]));
        store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello", "salut"]));
        store.add_synonym("salut", SetBuf::from_dirty(vec!["hello", "bonjour"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "hello", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 3, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 5, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "bonjour", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 3, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 5, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "salut", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 3, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 5, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn unique_to_multiword_synonyms() {
        let mut store = TempDatabase::from_iter(vec![
            ("new", &[doc_char_index(0, 0, 0)][..]),
            ("york", &[doc_char_index(0, 1, 1)][..]),
            ("city", &[doc_char_index(0, 2, 2)][..]),
            ("subway", &[doc_char_index(0, 3, 3)][..]),
            ("NY", &[doc_char_index(1, 0, 0)][..]),
            ("subway", &[doc_char_index(1, 1, 1)][..]),
        ]);

        store.add_synonym(
            "NY",
            SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]),
        );
        store.add_synonym(
            "NYC",
            SetBuf::from_dirty(vec!["NY", "new york", "new york city"]),
        );

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // NY ± new
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // NY ± york
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // NY ± city
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // NYC ± new
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // NYC ± york
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // NYC ± city
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn unique_to_multiword_synonyms_words_proximity() {
        let mut store = TempDatabase::from_iter(vec![
            ("new", &[doc_char_index(0, 0, 0)][..]),
            ("york", &[doc_char_index(0, 1, 1)][..]),
            ("city", &[doc_char_index(0, 2, 2)][..]),
            ("subway", &[doc_char_index(0, 3, 3)][..]),
            ("york", &[doc_char_index(1, 0, 0)][..]),
            ("new", &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),
            ("NY", &[doc_char_index(2, 0, 0)][..]),
            ("subway", &[doc_char_index(2, 1, 1)][..]),
        ]);

        store.add_synonym("NY", SetBuf::from_dirty(vec!["york new"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "NY", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. })); // NY ± york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. })); // NY ± new
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. })); // new  = NY
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 1, .. })); // york  = NY
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 0, .. })); // new = NY
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "new york", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. })); // york
            assert_matches!(matches.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 1, .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 0, .. })); // new
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn unique_to_multiword_synonyms_cumulative_word_index() {
        let mut store = TempDatabase::from_iter(vec![
            ("NY", &[doc_char_index(0, 0, 0)][..]),
            ("subway", &[doc_char_index(0, 1, 1)][..]),
            ("new", &[doc_char_index(1, 0, 0)][..]),
            ("york", &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),
        ]);

        store.add_synonym("new york", SetBuf::from_dirty(vec!["NY"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NY
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        // assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
        //     let mut matches = matches.into_iter();
        //     assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 2, is_exact: true, .. })); // subway
        //     assert_matches!(matches.next(), None);
        // });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "new york subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn harder_unique_to_multiword_synonyms_one() {
        let mut store = TempDatabase::from_iter(vec![
            ("new", &[doc_char_index(0, 0, 0)][..]),
            ("york", &[doc_char_index(0, 1, 1)][..]),
            ("city", &[doc_char_index(0, 2, 2)][..]),
            ("yellow", &[doc_char_index(0, 3, 3)][..]),
            ("subway", &[doc_char_index(0, 4, 4)][..]),
            ("broken", &[doc_char_index(0, 5, 5)][..]),
            ("NY", &[doc_char_index(1, 0, 0)][..]),
            ("blue", &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),
        ]);

        store.add_synonym(
            "NY",
            SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]),
        );
        store.add_synonym(
            "NYC",
            SetBuf::from_dirty(vec!["NY", "new york", "new york city"]),
        );

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                   // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);                   // position rewritten ^
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // NYC
            //                                                          because one-word to one-word ^^^^
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                   // position rewritten ^
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn even_harder_unique_to_multiword_synonyms() {
        let mut store = TempDatabase::from_iter(vec![
            ("new", &[doc_char_index(0, 0, 0)][..]),
            ("york", &[doc_char_index(0, 1, 1)][..]),
            ("city", &[doc_char_index(0, 2, 2)][..]),
            ("yellow", &[doc_char_index(0, 3, 3)][..]),
            ("underground", &[doc_char_index(0, 4, 4)][..]),
            ("train", &[doc_char_index(0, 5, 5)][..]),
            ("broken", &[doc_char_index(0, 6, 6)][..]),
            ("NY", &[doc_char_index(1, 0, 0)][..]),
            ("blue", &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),
        ]);

        store.add_synonym(
            "NY",
            SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]),
        );
        store.add_synonym(
            "NYC",
            SetBuf::from_dirty(vec!["NY", "new york", "new york city"]),
        );
        store.add_synonym("subway", SetBuf::from_dirty(vec!["underground train"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "NY subway broken", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: false, .. })); // underground = subway
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: false, .. })); // train       = subway
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 5, word_index: 6, is_exact: true,  .. })); // broken
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // underground = subway
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: true, .. })); // train       = subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            //                                                       because one-word to one-word ^^^^
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: false, .. })); // subway = underground
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: false, .. })); // subway = train
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Multi-word has multi-word synonyms
    fn multiword_to_multiword_synonyms() {
        let mut store = TempDatabase::from_iter(vec![
            ("NY", &[doc_char_index(0, 0, 0)][..]),
            ("subway", &[doc_char_index(0, 1, 1)][..]),
            ("NYC", &[doc_char_index(1, 0, 0)][..]),
            ("blue", &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),
            ("broken", &[doc_char_index(1, 3, 3)][..]),
            ("new", &[doc_char_index(2, 0, 0)][..]),
            ("york", &[doc_char_index(2, 1, 1)][..]),
            ("underground", &[doc_char_index(2, 2, 2)][..]),
            ("train", &[doc_char_index(2, 3, 3)][..]),
            ("broken", &[doc_char_index(2, 4, 4)][..]),
        ]);

        store.add_synonym(
            "new york",
            SetBuf::from_dirty(vec!["NYC", "NY", "new york city"]),
        );
        store.add_synonym(
            "new york city",
            SetBuf::from_dirty(vec!["NYC", "NY", "new york"]),
        );
        store.add_synonym("underground train", SetBuf::from_dirty(vec!["subway"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder
            .query(&reader, "new york underground train broken", 0..20)
            .unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // underground
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // train
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // broken
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // NYC = new
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // NYC = york
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // NYC = city
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway = underground
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: true,  .. })); // subway = train
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 5, word_index: 6, is_exact: true,  .. })); // broken
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder
            .query(&reader, "new york city underground train broken", 0..20)
            .unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 2, is_exact: true,  .. })); // underground
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // train
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 5, word_index: 4, is_exact: true,  .. })); // broken
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC = new
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC = york
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway = underground
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true, .. })); // subway = train
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 5, word_index: 5, is_exact: true, .. })); // broken
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn intercrossed_multiword_synonyms() {
        let mut store = TempDatabase::from_iter(vec![
            ("new", &[doc_index(0, 0)][..]),
            ("york", &[doc_index(0, 1)][..]),
            ("big", &[doc_index(0, 2)][..]),
            ("city", &[doc_index(0, 3)][..]),
        ]);

        store.add_synonym("new york", SetBuf::from_dirty(vec!["new york city"]));
        store.add_synonym("new york city", SetBuf::from_dirty(vec!["new york"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "new york big ", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false,  .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 4, is_exact: false,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // big
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let mut store = TempDatabase::from_iter(vec![
            ("NY", &[doc_index(0, 0)][..]),
            ("city", &[doc_index(0, 1)][..]),
            ("subway", &[doc_index(0, 2)][..]),
            ("NY", &[doc_index(1, 0)][..]),
            ("subway", &[doc_index(1, 1)][..]),
            ("NY", &[doc_index(2, 0)][..]),
            ("york", &[doc_index(2, 1)][..]),
            ("city", &[doc_index(2, 2)][..]),
            ("subway", &[doc_index(2, 3)][..]),
        ]);

        store.add_synonym("NY", SetBuf::from_dirty(vec!["new york city story"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "NY subway ", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // story
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn cumulative_word_indices() {
        let mut store = TempDatabase::from_iter(vec![
            ("NYC", &[doc_index(0, 0)][..]),
            ("long", &[doc_index(0, 1)][..]),
            ("subway", &[doc_index(0, 2)][..]),
            ("cool", &[doc_index(0, 3)][..]),
        ]);

        store.add_synonym("new york city", SetBuf::from_dirty(vec!["NYC"]));
        store.add_synonym("subway", SetBuf::from_dirty(vec!["underground train"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder
            .query(&reader, "new york city long subway cool ", 0..20)
            .unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new  = NYC
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york = NYC
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city = NYC
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // long
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // subway = underground
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // subway = train
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 6, word_index: 6, is_exact: true,  .. })); // cool
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn deunicoded_synonyms() {
        let mut store = TempDatabase::from_iter(vec![
            ("telephone", &[doc_index(0, 0)][..]), // meilisearch indexes the unidecoded
            ("téléphone", &[doc_index(0, 0)][..]), // and the original words on the same DocIndex
            ("iphone", &[doc_index(1, 0)][..]),
        ]);

        store.add_synonym("téléphone", SetBuf::from_dirty(vec!["iphone"]));

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "telephone", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "téléphone", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let results = builder.query(&reader, "télephone", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, distance: 1, word_index: 0, is_exact: false, .. })); // iphone | telephone
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn simple_concatenation() {
        let store = TempDatabase::from_iter(vec![
            ("iphone", &[doc_index(0, 0)][..]),
            ("case", &[doc_index(0, 1)][..]),
        ]);

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "i phone case", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // iphone
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, distance: 0, .. })); // iphone
            // assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 0, distance: 1, .. })); "phone"
            //                                                                        but no typo on first letter  ^^^^^^^
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, distance: 0, .. })); // case
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn exact_field_count_one_word() {
        let store = TempDatabase::from_iter(vec![
            ("searchengine", &[doc_index(0, 0)][..]),
            ("searchengine", &[doc_index(1, 0)][..]),
            ("blue",         &[doc_index(1, 1)][..]),
            ("searchangine", &[doc_index(2, 0)][..]),
            ("searchengine", &[doc_index(3, 0)][..]),
        ]);

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "searchengine", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // searchengine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(3), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // searchengine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // searchengine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 1, .. })); // searchengine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn simple_phrase_query_splitting() {
        let store = TempDatabase::from_iter(vec![
            ("search", &[doc_index(0, 0)][..]),
            ("engine", &[doc_index(0, 1)][..]),
            ("search", &[doc_index(1, 0)][..]),
            ("slow", &[doc_index(1, 1)][..]),
            ("engine", &[doc_index(1, 2)][..]),
        ]);

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "searchengine", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // search
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 1, distance: 0, .. })); // engine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn harder_phrase_query_splitting() {
        let store = TempDatabase::from_iter(vec![
            ("search", &[doc_index(0, 0)][..]),
            ("search", &[doc_index(0, 1)][..]),
            ("engine", &[doc_index(0, 2)][..]),
            ("search", &[doc_index(1, 0)][..]),
            ("slow", &[doc_index(1, 1)][..]),
            ("search", &[doc_index(1, 2)][..]),
            ("engine", &[doc_index(1, 3)][..]),
            ("search", &[doc_index(1, 0)][..]),
            ("search", &[doc_index(1, 1)][..]),
            ("slow", &[doc_index(1, 2)][..]),
            ("engine", &[doc_index(1, 3)][..]),
        ]);

        let db = &store.database;
        let reader = db.main_read_txn().unwrap();

        let builder = store.query_builder();
        let results = builder.query(&reader, "searchengine", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 1, distance: 0, .. })); // search
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 2, distance: 0, .. })); // engine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 2, distance: 0, .. })); // search
            assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 3, distance: 0, .. })); // engine
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }
}
