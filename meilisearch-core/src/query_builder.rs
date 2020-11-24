use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::{Deref, Range};
use std::time::Duration;

use either::Either;
use sdset::{SetOperation, SetBuf, Set};

use meilisearch_schema::FieldId;

use crate::bucket_sort::{bucket_sort, bucket_sort_with_distinct, SortResult, placeholder_document_sort, facet_count};
use crate::database::MainT;
use crate::facets::FacetFilter;
use crate::distinct_map::{DistinctMap, BufferedDistinctMap};
use crate::Document;
use crate::{criterion::Criteria, DocumentId};
use crate::{reordered_attrs::ReorderedAttrs, store, MResult, MainReader};

pub struct QueryBuilder<'c, 'f, 'd, 'i> {
    criteria: Criteria<'c>,
    searchable_attrs: Option<ReorderedAttrs>,
    filter: Option<Box<dyn Fn(DocumentId) -> bool + 'f>>,
    distinct: Option<(Box<dyn Fn(DocumentId) -> Option<u64> + 'd>, usize)>,
    timeout: Option<Duration>,
    index: &'i store::Index,
    facet_filter: Option<FacetFilter>,
    facets: Option<Vec<(FieldId, String)>>,
}

impl<'c, 'f, 'd, 'i> QueryBuilder<'c, 'f, 'd, 'i> {
    pub fn new(index: &'i store::Index) -> Self {
        QueryBuilder::with_criteria(index, Criteria::default())
    }

    /// sets facet attributes to filter on
    pub fn set_facet_filter(&mut self, facets: Option<FacetFilter>) {
        self.facet_filter = facets;
    }

    /// sets facet attributes for which to return the count
    pub fn set_facets(&mut self, facets: Option<Vec<(FieldId, String)>>) {
        self.facets = facets;
    }

    pub fn with_criteria(index: &'i store::Index, criteria: Criteria<'c>) -> Self {
        QueryBuilder {
            criteria,
            searchable_attrs: None,
            filter: None,
            distinct: None,
            timeout: None,
            index,
            facet_filter: None,
            facets: None,
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

    pub fn with_distinct<F>(&mut self, size: usize, function: F)
    where
        F: Fn(DocumentId) -> Option<u64> + 'd,
    {
        self.distinct = Some((Box::new(function), size))
    }

    pub fn add_searchable_attribute(&mut self, attribute: u16) {
        let reorders = self.searchable_attrs.get_or_insert_with(ReorderedAttrs::new);
        reorders.insert_attribute(attribute);
    }

    /// returns the documents ids associated with a facet filter by computing the union and
    /// intersection of the document sets
    fn facets_docids(&self, reader: &MainReader) -> MResult<Option<SetBuf<DocumentId>>> {
        let facet_docids = match self.facet_filter {
            Some(ref facets) => {
                let mut ands = Vec::with_capacity(facets.len());
                let mut ors = Vec::new();
                for f in facets.deref() {
                    match f {
                        Either::Left(keys) => {
                            ors.reserve(keys.len());
                            for key in keys {
                                let docids = self
                                    .index
                                    .facets
                                    .facet_document_ids(reader, &key)?
                                    .unwrap_or_default();
                                ors.push(docids);
                            }
                            let sets: Vec<_> = ors.iter().map(|(_, i)| i).map(Cow::deref).collect();
                            let or_result = sdset::multi::OpBuilder::from_vec(sets).union().into_set_buf();
                            ands.push(Cow::Owned(or_result));
                            ors.clear();
                        }
                        Either::Right(key) => {
                            match self.index.facets.facet_document_ids(reader, &key)? {
                                Some((_name, docids)) => ands.push(docids),
                                // no candidates for search, early return.
                                None => return Ok(Some(SetBuf::default())),
                            }
                        }
                    };
                }
                let ands: Vec<_> = ands.iter().map(Cow::deref).collect();
                Some(
                    sdset::multi::OpBuilder::from_vec(ands)
                    .intersection()
                    .into_set_buf(),
                )
            }
            None => None,
        };
        Ok(facet_docids)
    }

    fn standard_query(self, reader: &MainReader, query: &str, range: Range<usize>) -> MResult<SortResult> {
        let facets_docids = match self.facets_docids(reader)? {
            Some(ids) if ids.is_empty() => return Ok(SortResult::default()),
            other => other
        };
        // for each field to retrieve the count for, create an HashMap associating the attribute
        // value to a set of matching documents. The HashMaps are them collected in another
        // HashMap, associating each HashMap to it's field.
        let facet_count_docids = self.facet_count_docids(reader)?;

        match self.distinct {
            Some((distinct, distinct_size)) => bucket_sort_with_distinct(
                reader,
                query,
                range,
                facets_docids,
                facet_count_docids,
                self.filter,
                distinct,
                distinct_size,
                self.criteria,
                self.searchable_attrs,
                self.index,
            ),
            None => bucket_sort(
                reader,
                query,
                range,
                facets_docids,
                facet_count_docids,
                self.filter,
                self.criteria,
                self.searchable_attrs,
                self.index,
            ),
        }
    }

    fn placeholder_query(self, reader: &heed::RoTxn<MainT>, range: Range<usize>) -> MResult<SortResult> {
        match self.facets_docids(reader)? {
            Some(docids) => {
                // We sort the docids from facets according to the criteria set by the user
                let mut sorted_docids = docids.clone().into_vec();
                let mut sort_result = match self.index.main.ranked_map(reader)? {
                    Some(ranked_map) => {
                        placeholder_document_sort(&mut sorted_docids, self.index, reader, &ranked_map)?;
                        self.sort_result_from_docids(&sorted_docids, range)
                    },
                    // if we can't perform a sort, we return documents unordered
                    None => self.sort_result_from_docids(&docids, range),
                };

                if let Some(f) = self.facet_count_docids(reader)? {
                    sort_result.exhaustive_facets_count = Some(true);
                    sort_result.facets = Some(facet_count(f, &docids));
                }

                Ok(sort_result)
            },
            None => {
                match self.index.main.sorted_document_ids_cache(reader)? {
                    // build result from cached document ids
                    Some(docids) => { let mut sort_result = self.sort_result_from_docids(&docids, range);
                        if let Some(f) = self.facet_count_docids(reader)? {
                            sort_result.exhaustive_facets_count = Some(true);
                            // document ids are not sorted in natural order, we need to construct a new set
                            let document_set = SetBuf::from_dirty(Vec::from(docids));
                            sort_result.facets = Some(facet_count(f, &document_set));
                        }

                        Ok(sort_result)
                    },
                    // no document id cached, return empty result
                    None => Ok(SortResult::default()),
                }
            }
        }
    }

    fn facet_count_docids<'a>(&self, reader: &'a MainReader) -> MResult<Option<HashMap<String, HashMap<String, (&'a str, Cow<'a, Set<DocumentId>>)>>>> {
        match self.facets {
            Some(ref field_ids) => {
                let mut facet_count_map = HashMap::new();
                for (field_id, field_name) in field_ids {
                    let mut key_map = HashMap::new();
                    for pair in self.index.facets.field_document_ids(reader, *field_id)? {
                        let (facet_key, document_ids) = pair?;
                        let value = facet_key.value();
                        key_map.insert(value.to_string(), document_ids);
                    }
                    facet_count_map.insert(field_name.clone(), key_map);
                }
                Ok(Some(facet_count_map))
            }
            None => Ok(None),
        }
    }

    fn sort_result_from_docids(&self, docids: &[DocumentId], range: Range<usize>) -> SortResult {
        let mut sort_result = SortResult::default();
        let mut filtered_count = 0;
        let mut result = match self.filter {
            Some(ref filter) => docids
                .iter()
                .filter(|item| {
                    let accepted = (filter)(**item);
                    if !accepted {
                        filtered_count += 1;
                    }
                    accepted
                })
                .skip(range.start)
                .take(range.end - range.start)
                .map(|&id| Document::from_highlights(id, &[]))
                .collect::<Vec<_>>(),
            None => docids
                .iter()
                .skip(range.start)
                .take(range.end - range.start)
                .map(|&id| Document::from_highlights(id, &[]))
                .collect::<Vec<_>>(),
        };

        // distinct is set, remove duplicates with disctinct function
        if let Some((distinct, distinct_size)) = &self.distinct {
            let mut distinct_map = DistinctMap::new(*distinct_size);
            let mut distinct_map = BufferedDistinctMap::new(&mut distinct_map);
            result.retain(|doc| {
                let id = doc.id;
                let key = (distinct)(id);
                let distinct_accepted = match key {
                    Some(key) => distinct_map.register(key),
                    None => distinct_map.register_without_key(),
                };
                if !distinct_accepted {
                    filtered_count += 1;
                }
                distinct_accepted
            });
        }

        sort_result.documents = result;
        sort_result.nb_hits = docids.len() - filtered_count;
        sort_result
    }

    pub fn query(
        self,
        reader: &heed::RoTxn<MainT>,
        query: Option<&str>,
        range: Range<usize>,
    ) -> MResult<SortResult> {
        match query {
            Some(query) => self.standard_query(reader, query, range),
            None => self.placeholder_query(reader, range),
        }
    }
}

//#[cfg(test)]
//mod tests {
    //use super::*;

    //use std::collections::{BTreeSet, HashMap};
    //use std::iter::FromIterator;

    //use fst::IntoStreamer;
    //use meilisearch_schema::IndexedPos;
    //use sdset::SetBuf;
    //use tempfile::TempDir;

    //use crate::automaton::normalize_str;
    //use crate::bucket_sort::SimpleMatch;
    //use crate::database::{Database, DatabaseOptions};
    //use crate::store::Index;
    //use crate::DocIndex;
    //use crate::Document;
    //use meilisearch_schema::Schema;

    //fn set_from_stream<'f, I, S>(stream: I) -> fst::Set<Vec<u8>>
    //where
        //I: for<'a> fst::IntoStreamer<'a, Into = S, Item = &'a [u8]>,
        //S: 'f + for<'a> fst::Streamer<'a, Item = &'a [u8]>,
    //{
        //let mut builder = fst::SetBuilder::memory();
        //builder.extend_stream(stream).unwrap();
        //builder.into_set()
    //}

    //fn insert_key<A: AsRef<[u8]>>(set: &fst::Set<A>, key: &[u8]) -> fst::Set<Vec<u8>> {
        //let unique_key = {
            //let mut builder = fst::SetBuilder::memory();
            //builder.insert(key).unwrap();
            //builder.into_set()
        //};

        //let union_ = set.op().add(unique_key.into_stream()).r#union();

        //set_from_stream(union_)
    //}

    //fn sdset_into_fstset(set: &sdset::Set<&str>) -> fst::Set<Vec<u8>> {
        //let mut builder = fst::SetBuilder::memory();
        //let set = SetBuf::from_dirty(set.into_iter().map(|s| normalize_str(s)).collect());
        //builder.extend_iter(set.into_iter()).unwrap();
        //builder.into_set()
    //}

    //const fn doc_index(document_id: u32, word_index: u16) -> DocIndex {
        //DocIndex {
            //document_id: DocumentId(document_id),
            //attribute: 0,
            //word_index,
            //char_index: 0,
            //char_length: 0,
        //}
    //}

    //const fn doc_char_index(document_id: u32, word_index: u16, char_index: u16) -> DocIndex {
        //DocIndex {
            //document_id: DocumentId(document_id),
            //attribute: 0,
            //word_index,
            //char_index,
            //char_length: 0,
        //}
    //}

    //pub struct TempDatabase {
        //database: Database,
        //index: Index,
        //_tempdir: TempDir,
    //}

    //impl TempDatabase {
        //pub fn query_builder(&self) -> QueryBuilder {
            //self.index.query_builder()
        //}

        //pub fn add_synonym(&mut self, word: &str, new: SetBuf<&str>) {
            //let db = &self.database;
            //let mut writer = db.main_write_txn().unwrap();

            //let word = normalize_str(word);

            //let alternatives = self
                //.index
                //.synonyms
                //.synonyms_fst(&writer, word.as_bytes())
                //.unwrap();

            //let new = sdset_into_fstset(&new);
            //let new_alternatives =
                //set_from_stream(alternatives.op().add(new.into_stream()).r#union());
            //self.index
                //.synonyms
                //.put_synonyms(&mut writer, word.as_bytes(), &new_alternatives)
                //.unwrap();

            //let synonyms = self.index.main.synonyms_fst(&writer).unwrap();

            //let synonyms_fst = insert_key(&synonyms, word.as_bytes());
            //self.index
                //.main
                //.put_synonyms_fst(&mut writer, &synonyms_fst)
                //.unwrap();

            //writer.commit().unwrap();
        //}
    //}

    //impl<'a> FromIterator<(&'a str, &'a [DocIndex])> for TempDatabase {
        //fn from_iter<I: IntoIterator<Item = (&'a str, &'a [DocIndex])>>(iter: I) -> Self {
            //let tempdir = TempDir::new().unwrap();
            //let database = Database::open_or_create(&tempdir, DatabaseOptions::default()).unwrap();
            //let index = database.create_index("default").unwrap();

            //let db = &database;
            //let mut writer = db.main_write_txn().unwrap();

            //let mut words_fst = BTreeSet::new();
            //let mut postings_lists = HashMap::new();
            //let mut fields_counts = HashMap::<_, u16>::new();

            //let mut schema = Schema::with_primary_key("id");

            //for (word, indexes) in iter {
                //let mut final_indexes = Vec::new();
                //for index in indexes {
                    //let name = index.attribute.to_string();
                    //schema.insert(&name).unwrap();
                    //let indexed_pos = schema.set_indexed(&name).unwrap().1;
                    //let index = DocIndex {
                        //attribute: indexed_pos.0,
                        //..*index
                    //};
                    //final_indexes.push(index);
                //}

                //let word = word.to_lowercase().into_bytes();
                //words_fst.insert(word.clone());
                //postings_lists
                    //.entry(word)
                    //.or_insert_with(Vec::new)
                    //.extend_from_slice(&final_indexes);
                //for idx in final_indexes {
                    //fields_counts.insert((idx.document_id, idx.attribute, idx.word_index), 1);
                //}
            //}

            //index.main.put_schema(&mut writer, &schema).unwrap();

            //let words_fst = fst::Set::from_iter(words_fst).unwrap();

            //index.main.put_words_fst(&mut writer, &words_fst).unwrap();

            //for (word, postings_list) in postings_lists {
                //let postings_list = SetBuf::from_dirty(postings_list);
                //index
                    //.postings_lists
                    //.put_postings_list(&mut writer, &word, &postings_list)
                    //.unwrap();
            //}

            //for ((docid, attr, _), count) in fields_counts {
                //let prev = index
                    //.documents_fields_counts
                    //.document_field_count(&writer, docid, IndexedPos(attr))
                    //.unwrap();

                //let prev = prev.unwrap_or(0);

                //index
                    //.documents_fields_counts
                    //.put_document_field_count(&mut writer, docid, IndexedPos(attr), prev + count)
                    //.unwrap();
            //}

            //writer.commit().unwrap();

            //TempDatabase { database, index, _tempdir: tempdir }
        //}
    //}

    //#[test]
    //fn simple() {
        //let store = TempDatabase::from_iter(vec![
            //("iphone", &[doc_char_index(0, 0, 0)][..]),
            //("from", &[doc_char_index(0, 1, 1)][..]),
            //("apple", &[doc_char_index(0, 2, 2)][..]),
        //]);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("iphone from apple"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. }));
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn simple_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![("hello", &[doc_index(0, 0)][..])]);

        //store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("hello"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("bonjour"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //// #[test]
    //// fn prefix_synonyms() {
    ////     let mut store = TempDatabase::from_iter(vec![("hello", &[doc_index(0, 0)][..])]);

    ////     store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello"]));
    ////     store.add_synonym("salut", SetBuf::from_dirty(vec!["hello"]));

    ////     let db = &store.database;
    ////     let reader = db.main_read_txn().unwrap();

    ////     let builder = store.query_builder();
    ////     let results = builder.query(&reader, "sal", 0..20).unwrap();
    ////     let mut iter = documents.into_iter();

    ////     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    ////         let mut matches = matches.into_iter();
    ////         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    ////         assert_matches!(matches.next(), None);
    ////     });
    ////     assert_matches!(iter.next(), None);

    ////     let builder = store.query_builder();
    ////     let results = builder.query(&reader, "bonj", 0..20).unwrap();
    ////     let mut iter = documents.into_iter();

    ////     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    ////         let mut matches = matches.into_iter();
    ////         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    ////         assert_matches!(matches.next(), None);
    ////     });
    ////     assert_matches!(iter.next(), None);

    ////     let builder = store.query_builder();
    ////     let results = builder.query(&reader, "sal blabla", 0..20).unwrap();
    ////     let mut iter = documents.into_iter();

    ////     assert_matches!(iter.next(), None);

    ////     let builder = store.query_builder();
    ////     let results = builder.query(&reader, "bonj blabla", 0..20).unwrap();
    ////     let mut iter = documents.into_iter();

    ////     assert_matches!(iter.next(), None);
    //// }

    //// #[test]
    //// fn levenshtein_synonyms() {
    ////     let mut store = TempDatabase::from_iter(vec![("hello", &[doc_index(0, 0)][..])]);

    ////     store.add_synonym("salutation", SetBuf::from_dirty(vec!["hello"]));

    ////     let db = &store.database;
    ////     let reader = db.main_read_txn().unwrap();

    ////     let builder = store.query_builder();
    ////     let results = builder.query(&reader, "salutution", 0..20).unwrap();
    ////     let mut iter = documents.into_iter();

    ////     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    ////         let mut matches = matches.into_iter();
    ////         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    ////         assert_matches!(matches.next(), None);
    ////     });
    ////     assert_matches!(iter.next(), None);

    ////     let builder = store.query_builder();
    ////     let results = builder.query(&reader, "saluttion", 0..20).unwrap();
    ////     let mut iter = documents.into_iter();

    ////     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    ////         let mut matches = matches.into_iter();
    ////         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    ////         assert_matches!(matches.next(), None);
    ////     });
    ////     assert_matches!(iter.next(), None);
    //// }

    //#[test]
    //fn harder_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![
            //("hello", &[doc_index(0, 0)][..]),
            //("bonjour", &[doc_index(1, 3)]),
            //("salut", &[doc_index(2, 5)]),
        //]);

        //store.add_synonym("hello", SetBuf::from_dirty(vec!["bonjour", "salut"]));
        //store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello", "salut"]));
        //store.add_synonym("salut", SetBuf::from_dirty(vec!["hello", "bonjour"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("hello"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 3, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 5, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("bonjour"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 3, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 5, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("salut"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 3, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 5, .. }));
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    ///// Unique word has multi-word synonyms
    //fn unique_to_multiword_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![
            //("new", &[doc_char_index(0, 0, 0)][..]),
            //("york", &[doc_char_index(0, 1, 1)][..]),
            //("city", &[doc_char_index(0, 2, 2)][..]),
            //("subway", &[doc_char_index(0, 3, 3)][..]),
            //("NY", &[doc_char_index(1, 0, 0)][..]),
            //("subway", &[doc_char_index(1, 1, 1)][..]),
        //]);

        //store.add_synonym(
            //"NY",
            //SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]),
        //);
        //store.add_synonym(
            //"NYC",
            //SetBuf::from_dirty(vec!["NY", "new york", "new york city"]),
        //);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NY subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway
            //assert_matches!(iter.next(), None);                // position rewritten ^
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // NY ± new
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // NY ± york
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // NY ± city
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NYC subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway
            //assert_matches!(iter.next(), None);                // position rewritten ^
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // NYC ± new
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // NYC ± york
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // NYC ± city
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn unique_to_multiword_synonyms_words_proximity() {
        //let mut store = TempDatabase::from_iter(vec![
            //("new", &[doc_char_index(0, 0, 0)][..]),
            //("york", &[doc_char_index(0, 1, 1)][..]),
            //("city", &[doc_char_index(0, 2, 2)][..]),
            //("subway", &[doc_char_index(0, 3, 3)][..]),
            //("york", &[doc_char_index(1, 0, 0)][..]),
            //("new", &[doc_char_index(1, 1, 1)][..]),
            //("subway", &[doc_char_index(1, 2, 2)][..]),
            //("NY", &[doc_char_index(2, 0, 0)][..]),
            //("subway", &[doc_char_index(2, 1, 1)][..]),
        //]);

        //store.add_synonym("NY", SetBuf::from_dirty(vec!["york new"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NY"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. })); // NY ± york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. })); // NY ± new
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. })); // york = NY
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. })); // new  = NY
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 1, .. })); // york  = NY
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 0, .. })); // new = NY
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("new york"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, .. })); // york
            //assert_matches!(matches.next(), None);                // position rewritten ^
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 1, .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 0, .. })); // new
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn unique_to_multiword_synonyms_cumulative_word_index() {
        //let mut store = TempDatabase::from_iter(vec![
            //("NY", &[doc_char_index(0, 0, 0)][..]),
            //("subway", &[doc_char_index(0, 1, 1)][..]),
            //("new", &[doc_char_index(1, 0, 0)][..]),
            //("york", &[doc_char_index(1, 1, 1)][..]),
            //("subway", &[doc_char_index(1, 2, 2)][..]),
        //]);

        //store.add_synonym("new york", SetBuf::from_dirty(vec!["NY"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NY subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NY
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // subway
            //assert_matches!(matches.next(), None);
        //});
        //// assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
        ////     let mut matches = matches.into_iter();
        ////     assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 2, is_exact: true, .. })); // subway
        ////     assert_matches!(matches.next(), None);
        //// });
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } =
            //builder.query(&reader, Some("new york subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // subway
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // subway
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    ///// Unique word has multi-word synonyms
    //fn harder_unique_to_multiword_synonyms_one() {
        //let mut store = TempDatabase::from_iter(vec![
            //("new", &[doc_char_index(0, 0, 0)][..]),
            //("york", &[doc_char_index(0, 1, 1)][..]),
            //("city", &[doc_char_index(0, 2, 2)][..]),
            //("yellow", &[doc_char_index(0, 3, 3)][..]),
            //("subway", &[doc_char_index(0, 4, 4)][..]),
            //("broken", &[doc_char_index(0, 5, 5)][..]),
            //("NY", &[doc_char_index(1, 0, 0)][..]),
            //("blue", &[doc_char_index(1, 1, 1)][..]),
            //("subway", &[doc_char_index(1, 2, 2)][..]),
        //]);

        //store.add_synonym(
            //"NY",
            //SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]),
        //);
        //store.add_synonym(
            //"NYC",
            //SetBuf::from_dirty(vec!["NY", "new york", "new york city"]),
        //);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NY subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway
            //assert_matches!(iter.next(), None);                   // position rewritten ^
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway
            //assert_matches!(iter.next(), None);                   // position rewritten ^
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NYC subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // NYC
            ////                                                          because one-word to one-word ^^^^
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway
            //assert_matches!(iter.next(), None);                   // position rewritten ^
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    ///// Unique word has multi-word synonyms
    //fn even_harder_unique_to_multiword_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![
            //("new", &[doc_char_index(0, 0, 0)][..]),
            //("york", &[doc_char_index(0, 1, 1)][..]),
            //("city", &[doc_char_index(0, 2, 2)][..]),
            //("yellow", &[doc_char_index(0, 3, 3)][..]),
            //("underground", &[doc_char_index(0, 4, 4)][..]),
            //("train", &[doc_char_index(0, 5, 5)][..]),
            //("broken", &[doc_char_index(0, 6, 6)][..]),
            //("NY", &[doc_char_index(1, 0, 0)][..]),
            //("blue", &[doc_char_index(1, 1, 1)][..]),
            //("subway", &[doc_char_index(1, 2, 2)][..]),
        //]);

        //store.add_synonym(
            //"NY",
            //SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]),
        //);
        //store.add_synonym(
            //"NYC",
            //SetBuf::from_dirty(vec!["NY", "new york", "new york city"]),
        //);
        //store.add_synonym("subway", SetBuf::from_dirty(vec!["underground train"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult {documents, .. } = builder.query(&reader, Some("NY subway broken"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: false, .. })); // underground = subway
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: false, .. })); // train       = subway
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 5, word_index: 6, is_exact: true,  .. })); // broken
            //assert_matches!(iter.next(), None);                // position rewritten ^
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NYC subway"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // underground = subway
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: true, .. })); // train       = subway
            //assert_matches!(iter.next(), None);                // position rewritten ^
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            ////                                                       because one-word to one-word ^^^^
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: false, .. })); // subway = underground
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: false, .. })); // subway = train
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    ///// Multi-word has multi-word synonyms
    //fn multiword_to_multiword_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![
            //("NY", &[doc_char_index(0, 0, 0)][..]),
            //("subway", &[doc_char_index(0, 1, 1)][..]),
            //("NYC", &[doc_char_index(1, 0, 0)][..]),
            //("blue", &[doc_char_index(1, 1, 1)][..]),
            //("subway", &[doc_char_index(1, 2, 2)][..]),
            //("broken", &[doc_char_index(1, 3, 3)][..]),
            //("new", &[doc_char_index(2, 0, 0)][..]),
            //("york", &[doc_char_index(2, 1, 1)][..]),
            //("underground", &[doc_char_index(2, 2, 2)][..]),
            //("train", &[doc_char_index(2, 3, 3)][..]),
            //("broken", &[doc_char_index(2, 4, 4)][..]),
        //]);

        //store.add_synonym(
            //"new york",
            //SetBuf::from_dirty(vec!["NYC", "NY", "new york city"]),
        //);
        //store.add_synonym(
            //"new york city",
            //SetBuf::from_dirty(vec!["NYC", "NY", "new york"]),
        //);
        //store.add_synonym("underground train", SetBuf::from_dirty(vec!["subway"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder
            //.query(&reader, Some("new york underground train broken"), 0..20)
            //.unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // underground
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // train
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // broken
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // NYC = new
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // NYC = york
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // NYC = city
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway = underground
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 5, is_exact: true,  .. })); // subway = train
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 5, word_index: 6, is_exact: true,  .. })); // broken
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder
            //.query(&reader, Some("new york city underground train broken"), 0..20)
            //.unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 2, is_exact: true,  .. })); // underground
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // train
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 5, word_index: 4, is_exact: true,  .. })); // broken
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC = new
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC = york
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway = underground
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true, .. })); // subway = train
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 5, word_index: 5, is_exact: true, .. })); // broken
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn intercrossed_multiword_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![
            //("new", &[doc_index(0, 0)][..]),
            //("york", &[doc_index(0, 1)][..]),
            //("big", &[doc_index(0, 2)][..]),
            //("city", &[doc_index(0, 3)][..]),
        //]);

        //store.add_synonym("new york", SetBuf::from_dirty(vec!["new york city"]));
        //store.add_synonym("new york city", SetBuf::from_dirty(vec!["new york"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("new york big "), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: false,  .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 4, is_exact: false,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // big
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let mut store = TempDatabase::from_iter(vec![
            //("NY", &[doc_index(0, 0)][..]),
            //("city", &[doc_index(0, 1)][..]),
            //("subway", &[doc_index(0, 2)][..]),
            //("NY", &[doc_index(1, 0)][..]),
            //("subway", &[doc_index(1, 1)][..]),
            //("NY", &[doc_index(2, 0)][..]),
            //("york", &[doc_index(2, 1)][..]),
            //("city", &[doc_index(2, 2)][..]),
            //("subway", &[doc_index(2, 3)][..]),
        //]);

        //store.add_synonym("NY", SetBuf::from_dirty(vec!["new york city story"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("NY subway "), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // subway
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: false,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: false,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // subway
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // story
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true, .. })); // subway
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn cumulative_word_indices() {
        //let mut store = TempDatabase::from_iter(vec![
            //("NYC", &[doc_index(0, 0)][..]),
            //("long", &[doc_index(0, 1)][..]),
            //("subway", &[doc_index(0, 2)][..]),
            //("cool", &[doc_index(0, 3)][..]),
        //]);

        //store.add_synonym("new york city", SetBuf::from_dirty(vec!["NYC"]));
        //store.add_synonym("subway", SetBuf::from_dirty(vec!["underground train"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder
            //.query(&reader, Some("new york city long subway cool "), 0..20)
            //.unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut matches = matches.into_iter();
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new  = NYC
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york = NYC
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city = NYC
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // long
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // subway = underground
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // subway = train
            //assert_matches!(matches.next(), Some(SimpleMatch { query_index: 6, word_index: 6, is_exact: true,  .. })); // cool
            //assert_matches!(matches.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn deunicoded_synonyms() {
        //let mut store = TempDatabase::from_iter(vec![
            //("telephone", &[doc_index(0, 0)][..]), // meilisearch indexes the unidecoded
            //("téléphone", &[doc_index(0, 0)][..]), // and the original words on the same DocIndex
            //("iphone", &[doc_index(1, 0)][..]),
        //]);

        //store.add_synonym("téléphone", SetBuf::from_dirty(vec!["iphone"]));

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("telephone"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("téléphone"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("télephone"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, .. }));
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, distance: 1, word_index: 0, is_exact: false, .. })); // iphone | telephone
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn simple_concatenation() {
        //let store = TempDatabase::from_iter(vec![
            //("iphone", &[doc_index(0, 0)][..]),
            //("case", &[doc_index(0, 1)][..]),
        //]);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("i phone case"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // iphone
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 1, distance: 0, .. })); // iphone
            //// assert_matches!(iter.next(), Some(SimpleMatch { query_index: 1, word_index: 0, distance: 1, .. })); "phone"
            ////                                                                        but no typo on first letter  ^^^^^^^
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 2, word_index: 2, distance: 0, .. })); // case
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn exact_field_count_one_word() {
        //let store = TempDatabase::from_iter(vec![
            //("searchengine", &[doc_index(0, 0)][..]),
            //("searchengine", &[doc_index(1, 0)][..]),
            //("blue",         &[doc_index(1, 1)][..]),
            //("searchangine", &[doc_index(2, 0)][..]),
            //("searchengine", &[doc_index(3, 0)][..]),
        //]);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("searchengine"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // searchengine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(3), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // searchengine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // searchengine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 1, .. })); // searchengine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn simple_phrase_query_splitting() {
        //let store = TempDatabase::from_iter(vec![
            //("search", &[doc_index(0, 0)][..]),
            //("engine", &[doc_index(0, 1)][..]),
            //("search", &[doc_index(1, 0)][..]),
            //("slow", &[doc_index(1, 1)][..]),
            //("engine", &[doc_index(1, 2)][..]),
        //]);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("searchengine"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 0, distance: 0, .. })); // search
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 1, distance: 0, .. })); // engine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}

    //#[test]
    //fn harder_phrase_query_splitting() {
        //let store = TempDatabase::from_iter(vec![
            //("search", &[doc_index(0, 0)][..]),
            //("search", &[doc_index(0, 1)][..]),
            //("engine", &[doc_index(0, 2)][..]),
            //("search", &[doc_index(1, 0)][..]),
            //("slow", &[doc_index(1, 1)][..]),
            //("search", &[doc_index(1, 2)][..]),
            //("engine", &[doc_index(1, 3)][..]),
            //("search", &[doc_index(1, 0)][..]),
            //("search", &[doc_index(1, 1)][..]),
            //("slow", &[doc_index(1, 2)][..]),
            //("engine", &[doc_index(1, 3)][..]),
        //]);

        //let db = &store.database;
        //let reader = db.main_read_txn().unwrap();

        //let builder = store.query_builder();
        //let SortResult { documents, .. } = builder.query(&reader, Some("searchengine"), 0..20).unwrap();
        //let mut iter = documents.into_iter();

        //assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 1, distance: 0, .. })); // search
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 2, distance: 0, .. })); // engine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            //let mut iter = matches.into_iter();
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 2, distance: 0, .. })); // search
            //assert_matches!(iter.next(), Some(SimpleMatch { query_index: 0, word_index: 3, distance: 0, .. })); // engine
            //assert_matches!(iter.next(), None);
        //});
        //assert_matches!(iter.next(), None);
    //}
//}
