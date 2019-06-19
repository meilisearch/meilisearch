use std::hash::Hash;
use std::ops::Range;
use std::rc::Rc;
use std::time::Instant;
use std::{cmp, mem};

use fst::{Streamer, IntoStreamer};
use hashbrown::{HashMap, HashSet};
use log::info;
use meilidb_tokenizer::{is_cjk, split_query_string};
use rayon::slice::ParallelSliceMut;
use sdset::SetBuf;
use slice_group_by::GroupByMut;

use crate::automaton::{DfaExt, AutomatonExt, build_dfa, build_prefix_dfa};
use crate::distinct_map::{DistinctMap, BufferedDistinctMap};
use crate::criterion::Criteria;
use crate::raw_documents_from_matches;
use crate::{Match, DocumentId, Store, RawDocument, Document};

const NGRAMS: usize = 3;

fn generate_automatons<S: Store>(query: &str, store: &S) -> Result<Vec<(usize, DfaExt)>, S::Error> {
    let has_end_whitespace = query.chars().last().map_or(false, char::is_whitespace);
    let query_words: Vec<_> = split_query_string(query).map(str::to_lowercase).collect();
    let mut automatons = Vec::new();

    let synonyms = store.synonyms()?;

    for n in 1..=NGRAMS {
        let mut index = 0;
        let mut ngrams = query_words.windows(n).peekable();

        while let Some(ngram) = ngrams.next() {
            let ngram_nb_words = ngram.len();
            let ngram = ngram.join(" ");

            let has_following_word = ngrams.peek().is_some();
            let not_prefix_dfa = has_following_word || has_end_whitespace || ngram.chars().all(is_cjk);

            let lev = if not_prefix_dfa { build_dfa(&ngram) } else { build_prefix_dfa(&ngram) };
            let mut stream = synonyms.search(&lev).into_stream();
            while let Some(base) = stream.next() {

                // only trigger alternatives when the last word has been typed
                // i.e. "new " do not but "new yo" triggers alternatives to "new york"
                let base = std::str::from_utf8(base).unwrap();
                let base_nb_words = split_query_string(base).count();
                if ngram_nb_words != base_nb_words { continue }

                if let Some(synonyms) = store.alternatives_to(base.as_bytes())? {

                    let mut stream = synonyms.into_stream();
                    while let Some(synonyms) = stream.next() {

                        let synonyms = std::str::from_utf8(synonyms).unwrap();
                        for synonym in split_query_string(synonyms) {
                            let lev = build_dfa(synonym);
                            automatons.push((index, synonym.to_owned(), lev));
                        }
                    }
                }
            }

            if n == 1 {
                automatons.push((index, ngram, lev));
            }

            index += 1;
        }
    }

    automatons.sort_unstable_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    automatons.dedup_by(|a, b| (a.0, &a.1) == (b.0, &b.1));
    let automatons = automatons.into_iter().map(|(i, _, a)| (i, a)).collect();

    Ok(automatons)
}

pub struct QueryBuilder<'c, S, FI = fn(DocumentId) -> bool> {
    store: S,
    criteria: Criteria<'c>,
    searchable_attrs: Option<HashSet<u16>>,
    filter: Option<FI>,
}

impl<'c, S> QueryBuilder<'c, S, fn(DocumentId) -> bool> {
    pub fn new(store: S) -> Self {
        QueryBuilder::with_criteria(store, Criteria::default())
    }

    pub fn with_criteria(store: S, criteria: Criteria<'c>) -> Self {
        QueryBuilder { store, criteria, searchable_attrs: None, filter: None }
    }
}

impl<'c, S, FI> QueryBuilder<'c, S, FI>
{
    pub fn with_filter<F>(self, function: F) -> QueryBuilder<'c, S, F>
    where F: Fn(DocumentId) -> bool,
    {
        QueryBuilder {
            store: self.store,
            criteria: self.criteria,
            searchable_attrs: self.searchable_attrs,
            filter: Some(function),
        }
    }

    pub fn with_distinct<F, K>(self, function: F, size: usize) -> DistinctQueryBuilder<'c, S, FI, F>
    where F: Fn(DocumentId) -> Option<K>,
          K: Hash + Eq,
    {
        DistinctQueryBuilder { inner: self, function, size }
    }

    pub fn add_searchable_attribute(&mut self, attribute: u16) {
        let attributes = self.searchable_attrs.get_or_insert_with(HashSet::new);
        attributes.insert(attribute);
    }
}

impl<'c, S, FI> QueryBuilder<'c, S, FI>
where S: Store,
{
    fn query_all(&self, query: &str) -> Result<Vec<RawDocument>, S::Error> {
        let automatons = generate_automatons(query, &self.store)?;
        let words = self.store.words()?.as_fst();

        let mut stream = {
            let mut op_builder = fst::raw::OpBuilder::new();
            for (_index, automaton) in &automatons {
                let stream = words.search(automaton);
                op_builder.push(stream);
            }
            op_builder.r#union()
        };

        let mut matches = Vec::new();

        while let Some((input, indexed_values)) = stream.next() {
            for iv in indexed_values {
                let (index, automaton) = &automatons[iv.index];
                let distance = automaton.eval(input).to_u8();
                let is_exact = distance == 0 && input.len() == automaton.query_len();

                let doc_indexes = self.store.word_indexes(input)?;
                let doc_indexes = match doc_indexes {
                    Some(doc_indexes) => doc_indexes,
                    None => continue,
                };

                for di in doc_indexes.as_slice() {
                    if self.searchable_attrs.as_ref().map_or(true, |r| r.contains(&di.attribute)) {
                        let match_ = Match {
                            query_index: *index as u32,
                            distance,
                            attribute: di.attribute,
                            word_index: di.word_index,
                            is_exact,
                            char_index: di.char_index,
                            char_length: di.char_length,
                        };
                        matches.push((di.document_id, match_));
                    }
                }
            }
        }

        matches.par_sort_unstable();

        for document_matches in matches.linear_group_by_mut(|(a, _), (b, _)| a == b) {
            let mut offset = 0;
            for query_indexes in document_matches.linear_group_by_mut(|(_, a), (_, b)| a.query_index == b.query_index) {
                let word_index = query_indexes[0].1.word_index - offset as u16;
                for (_, match_) in query_indexes.iter_mut() {
                    match_.word_index = word_index;
                }
                offset += query_indexes.len() - 1;
            }
        }

        let total_matches = matches.len();
        let padded_matches = SetBuf::from_dirty(matches);
        let raw_documents = raw_documents_from_matches(padded_matches);

        info!("{} total documents to classify", raw_documents.len());
        info!("{} total matches to classify", total_matches);

        Ok(raw_documents)
    }
}

impl<'c, S, FI> QueryBuilder<'c, S, FI>
where S: Store,
      FI: Fn(DocumentId) -> bool,
{
    pub fn query(self, query: &str, range: Range<usize>) -> Result<Vec<Document>, S::Error> {
        // We delegate the filter work to the distinct query builder,
        // specifying a distinct rule that has no effect.
        if self.filter.is_some() {
            let builder = self.with_distinct(|_| None as Option<()>, 1);
            return builder.query(query, range);
        }

        let start = Instant::now();
        let mut documents = self.query_all(query)?;
        info!("query_all took {:.2?}", start.elapsed());

        let mut groups = vec![documents.as_mut_slice()];

        'criteria: for criterion in self.criteria.as_ref() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut documents_seen = 0;

            for group in tmp_groups {
                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < range.start {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                let start = Instant::now();
                group.par_sort_unstable_by(|a, b| criterion.evaluate(a, b));
                info!("criterion {} sort took {:.2?}", criterion.name(), start.elapsed());

                for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b)) {
                    info!("criterion {} produced a group of size {}", criterion.name(), group.len());

                    documents_seen += group.len();
                    groups.push(group);

                    // we have sort enough documents if the last document sorted is after
                    // the end of the requested range, we can continue to the next criterion
                    if documents_seen >= range.end { continue 'criteria }
                }
            }
        }

        let offset = cmp::min(documents.len(), range.start);
        let iter = documents.into_iter().skip(offset).take(range.len());
        Ok(iter.map(|d| Document::from_raw(&d)).collect())
    }
}

pub struct DistinctQueryBuilder<'c, I, FI, FD> {
    inner: QueryBuilder<'c, I, FI>,
    function: FD,
    size: usize,
}

impl<'c, I, FI, FD> DistinctQueryBuilder<'c, I, FI, FD>
{
    pub fn with_filter<F>(self, function: F) -> DistinctQueryBuilder<'c, I, F, FD>
    where F: Fn(DocumentId) -> bool,
    {
        DistinctQueryBuilder {
            inner: self.inner.with_filter(function),
            function: self.function,
            size: self.size
        }
    }

    pub fn add_searchable_attribute(&mut self, attribute: u16) {
        self.inner.add_searchable_attribute(attribute);
    }
}

impl<'c, S, FI, FD, K> DistinctQueryBuilder<'c, S, FI, FD>
where S: Store,
      FI: Fn(DocumentId) -> bool,
      FD: Fn(DocumentId) -> Option<K>,
      K: Hash + Eq,
{
    pub fn query(self, query: &str, range: Range<usize>) -> Result<Vec<Document>, S::Error> {
        let start = Instant::now();
        let mut documents = self.inner.query_all(query)?;
        info!("query_all took {:.2?}", start.elapsed());

        let mut groups = vec![documents.as_mut_slice()];
        let mut key_cache = HashMap::new();

        let mut filter_map = HashMap::new();
        // these two variables informs on the current distinct map and
        // on the raw offset of the start of the group where the
        // range.start bound is located according to the distinct function
        let mut distinct_map = DistinctMap::new(self.size);
        let mut distinct_raw_offset = 0;

        'criteria: for criterion in self.inner.criteria.as_ref() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut buf_distinct = BufferedDistinctMap::new(&mut distinct_map);
            let mut documents_seen = 0;

            for group in tmp_groups {
                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < distinct_raw_offset {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                let start = Instant::now();
                group.par_sort_unstable_by(|a, b| criterion.evaluate(a, b));
                info!("criterion {} sort took {:.2?}", criterion.name(), start.elapsed());

                for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b)) {
                    // we must compute the real distinguished len of this sub-group
                    for document in group.iter() {
                        let filter_accepted = match &self.inner.filter {
                            Some(filter) => {
                                let entry = filter_map.entry(document.id);
                                *entry.or_insert_with(|| (filter)(document.id))
                            },
                            None => true,
                        };

                        if filter_accepted {
                            let entry = key_cache.entry(document.id);
                            let key = entry.or_insert_with(|| (self.function)(document.id).map(Rc::new));

                            match key.clone() {
                                Some(key) => buf_distinct.register(key),
                                None => buf_distinct.register_without_key(),
                            };
                        }

                        // the requested range end is reached: stop computing distinct
                        if buf_distinct.len() >= range.end { break }
                    }

                    info!("criterion {} produced a group of size {}", criterion.name(), group.len());

                    documents_seen += group.len();
                    groups.push(group);

                    // if this sub-group does not overlap with the requested range
                    // we must update the distinct map and its start index
                    if buf_distinct.len() < range.start {
                        buf_distinct.transfert_to_internal();
                        distinct_raw_offset = documents_seen;
                    }

                    // we have sort enough documents if the last document sorted is after
                    // the end of the requested range, we can continue to the next criterion
                    if buf_distinct.len() >= range.end { continue 'criteria }
                }
            }
        }

        let mut out_documents = Vec::with_capacity(range.len());
        let mut seen = BufferedDistinctMap::new(&mut distinct_map);

        for document in documents.into_iter().skip(distinct_raw_offset) {
            let filter_accepted = match &self.inner.filter {
                Some(_) => filter_map.remove(&document.id).expect("BUG: filtered not found"),
                None => true,
            };

            if filter_accepted {
                let key = key_cache.remove(&document.id).expect("BUG: cached key not found");
                let distinct_accepted = match key {
                    Some(key) => seen.register(key),
                    None => seen.register_without_key(),
                };

                if distinct_accepted && seen.len() > range.start {
                    out_documents.push(Document::from_raw(&document));
                    if out_documents.len() == range.len() { break }
                }
            }
        }

        Ok(out_documents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeSet, HashMap};
    use std::iter::FromIterator;

    use sdset::SetBuf;
    use fst::{Set, IntoStreamer};

    use crate::DocIndex;
    use crate::store::Store;

    #[derive(Default)]
    struct InMemorySetStore {
        set: Set,
        synonyms: Set,
        indexes: HashMap<Vec<u8>, SetBuf<DocIndex>>,
        alternatives: HashMap<Vec<u8>, Set>,
    }

    fn set_from_stream<'f, I, S>(stream: I) -> Set
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=&'a [u8]>,
        S: 'f + for<'a> fst::Streamer<'a, Item=&'a [u8]>,
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
        let set = SetBuf::from_dirty(set.into_iter().map(|s| s.to_lowercase()).collect());
        builder.extend_iter(set.into_iter()).unwrap();
        builder.into_inner().and_then(Set::from_bytes).unwrap()
    }

    impl InMemorySetStore {
        pub fn add_synonym(&mut self, word: &str, new: SetBuf<&str>) {
            let word = word.to_lowercase();
            let alternatives = self.alternatives.entry(word.as_bytes().to_vec()).or_default();
            let new = sdset_into_fstset(&new);
            *alternatives = set_from_stream(alternatives.op().add(new.into_stream()).r#union());

            self.synonyms = insert_key(&self.synonyms, word.as_bytes());
        }
    }

    impl<'a> FromIterator<(&'a str, &'a [DocIndex])> for InMemorySetStore {
        fn from_iter<I: IntoIterator<Item=(&'a str, &'a [DocIndex])>>(iter: I) -> Self {
            let mut tree = BTreeSet::new();
            let mut map = HashMap::new();

            for (word, indexes) in iter {
                let word = word.to_lowercase().into_bytes();
                tree.insert(word.clone());
                map.entry(word).or_insert_with(Vec::new).extend_from_slice(indexes);
            }

            InMemorySetStore {
                set: Set::from_iter(tree).unwrap(),
                synonyms: Set::default(),
                indexes: map.into_iter().map(|(k, v)| (k, SetBuf::from_dirty(v))).collect(),
                alternatives: HashMap::new(),
            }
        }
    }

    impl Store for InMemorySetStore {
        type Error = std::io::Error;

        fn words(&self) -> Result<&Set, Self::Error> {
            Ok(&self.set)
        }

        fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
            Ok(self.indexes.get(word).cloned())
        }

        fn synonyms(&self) -> Result<&Set, Self::Error> {
            Ok(&self.synonyms)
        }

        fn alternatives_to(&self, word: &[u8]) -> Result<Option<Set>, Self::Error> {
            Ok(self.alternatives.get(word).map(|s| Set::from_bytes(s.as_fst().to_vec()).unwrap()))
        }
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

    #[test]
    fn simple_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("hello", &[doc_index(0, 0)][..]),
        ]);

        store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("hello", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonjour", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn prefix_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("hello", &[doc_index(0, 0)][..]),
        ]);

        store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello"]));
        store.add_synonym("salut", SetBuf::from_dirty(vec!["hello"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("sal", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonj", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("sal blabla", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonj blabla", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), None);
    }

    #[test]
    fn levenshtein_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("hello", &[doc_index(0, 0)][..]),
        ]);

        store.add_synonym("salutation", SetBuf::from_dirty(vec!["hello"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("salutution", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("saluttion", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn harder_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("hello",     &[doc_index(0, 0)][..]),
            ("bonjour",   &[doc_index(1, 3)]),
            ("salut",     &[doc_index(2, 5)]),
        ]);

        store.add_synonym("hello", SetBuf::from_dirty(vec!["bonjour", "salut"]));
        store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello", "salut"]));
        store.add_synonym("salut", SetBuf::from_dirty(vec!["hello", "bonjour"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("hello", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 3);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 5);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonjour", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 3);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 5);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("salut", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 0);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 3);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches }) => {
            assert_eq!(matches.len(), 1);
            let match_ = matches[0];
            assert_eq!(match_.query_index, 0);
            assert_eq!(match_.word_index, 5);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn unique_to_multiword_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("new", &[doc_char_index(0, 0, 0)][..]),
            ("york", &[doc_char_index(0, 1, 1)][..]),
            ("city", &[doc_char_index(0, 2, 2)][..]),
            ("subway", &[doc_char_index(0, 3, 3)][..]),

            ("NY", &[doc_char_index(1, 0, 0)][..]),
            ("subway", &[doc_char_index(1, 1, 1)][..]),
        ]);

        store.add_synonym("NY",  SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]));
        store.add_synonym("NYC", SetBuf::from_dirty(vec!["NY",  "new york", "new york city"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // new  = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // city = NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 1, .. })); // subway
            assert_matches!(iter.next(), None);             // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 1, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // new  = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // city = NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 1, .. })); // subway
            assert_matches!(iter.next(), None);             // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 1, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn harder_unique_to_multiword_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("new",     &[doc_char_index(0, 0, 0)][..]),
            ("york",    &[doc_char_index(0, 1, 1)][..]),
            ("city",    &[doc_char_index(0, 2, 2)][..]),
            ("yellow",  &[doc_char_index(0, 3, 3)][..]),
            ("subway",  &[doc_char_index(0, 4, 4)][..]),
            ("broken",  &[doc_char_index(0, 5, 5)][..]),

            ("NY",      &[doc_char_index(1, 0, 0)][..]),
            ("blue",    &[doc_char_index(1, 1, 1)][..]),
            ("subway",  &[doc_char_index(1, 2, 2)][..]),
        ]);

        store.add_synonym("NY",  SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]));
        store.add_synonym("NYC", SetBuf::from_dirty(vec!["NY",  "new york", "new york city"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // new  = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // city = NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // subway
            assert_matches!(iter.next(), None);             // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // new  = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // city = NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // subway
            assert_matches!(iter.next(), None);             // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn even_harder_unique_to_multiword_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("new",         &[doc_char_index(0, 0, 0)][..]),
            ("york",        &[doc_char_index(0, 1, 1)][..]),
            ("city",        &[doc_char_index(0, 2, 2)][..]),
            ("yellow",      &[doc_char_index(0, 3, 3)][..]),
            ("underground", &[doc_char_index(0, 4, 4)][..]),
            ("train",       &[doc_char_index(0, 5, 5)][..]),
            ("broken",      &[doc_char_index(0, 6, 6)][..]),

            ("NY",      &[doc_char_index(1, 0, 0)][..]),
            ("blue",    &[doc_char_index(1, 1, 1)][..]),
            ("subway",  &[doc_char_index(1, 2, 2)][..]),
        ]);

        store.add_synonym("NY",  SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]));
        store.add_synonym("NYC", SetBuf::from_dirty(vec!["NY",  "new york", "new york city"]));
        store.add_synonym("subway", SetBuf::from_dirty(vec!["underground train"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY subway broken", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // new  = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // city = NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // underground = subway
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // train       = subway
            assert_matches!(iter.next(), Some(Match { query_index: 2, word_index: 3, .. })); // broken
            assert_matches!(iter.next(), None);             // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // new  = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // city = NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // underground = subway
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // train       = subway
            assert_matches!(iter.next(), None);             // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY
            assert_matches!(iter.next(), Some(Match { query_index: 1, word_index: 2, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Multi-word has multi-word synonyms
    fn multiword_to_multiword_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("NY",      &[doc_char_index(0, 0, 0)][..]),
            ("subway",  &[doc_char_index(0, 1, 1)][..]),

            ("NYC",     &[doc_char_index(1, 0, 0)][..]),
            ("blue",    &[doc_char_index(1, 1, 1)][..]),
            ("subway",  &[doc_char_index(1, 2, 2)][..]),
            ("broken",  &[doc_char_index(1, 3, 3)][..]),
        ]);

        store.add_synonym("new york", SetBuf::from_dirty(vec!["NYC", "NY", "new york city"]));
        store.add_synonym("new york city", SetBuf::from_dirty(vec!["NYC", "NY", "new york"]));
        store.add_synonym("underground train", SetBuf::from_dirty(vec!["subway"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york underground train broken", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NYC    = new york
            assert_matches!(iter.next(), Some(Match { query_index: 2, word_index: 2, .. })); // subway = underground train
            assert_matches!(iter.next(), Some(Match { query_index: 4, word_index: 3, .. })); // broken
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY     = new york
            assert_matches!(iter.next(), Some(Match { query_index: 2, word_index: 1, .. })); // subway = underground train
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york city underground train broken", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NYC    = new york city
            assert_matches!(iter.next(), Some(Match { query_index: 3, word_index: 2, .. })); // subway = underground train
            assert_matches!(iter.next(), Some(Match { query_index: 5, word_index: 3, .. })); // broken
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(Match { query_index: 0, word_index: 0, .. })); // NY     = new york city
            assert_matches!(iter.next(), Some(Match { query_index: 3, word_index: 1, .. })); // subway = underground train
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }
}
