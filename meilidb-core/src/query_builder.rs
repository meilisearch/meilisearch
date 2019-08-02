use std::hash::Hash;
use std::ops::Range;
use std::rc::Rc;
use std::time::Instant;
use std::{cmp, mem};

use fst::{Streamer, IntoStreamer};
use hashbrown::HashMap;
use levenshtein_automata::DFA;
use log::info;
use meilidb_tokenizer::{is_cjk, split_query_string};
use rayon::slice::ParallelSliceMut;
use sdset::SetBuf;
use slice_group_by::{GroupBy, GroupByMut};

use crate::automaton::{build_dfa, build_prefix_dfa};
use crate::criterion::Criteria;
use crate::distinct_map::{DistinctMap, BufferedDistinctMap};
use crate::query_enhancer::{QueryEnhancerBuilder, QueryEnhancer};
use crate::raw_documents_from;
use crate::reordered_attrs::ReorderedAttrs;
use crate::{TmpMatch, Highlight, DocumentId, Store, RawDocument, Document};

const NGRAMS: usize = 3;

struct Automaton {
    query_len: usize,
    is_exact: bool,
    dfa: DFA,
}

impl Automaton {
    fn exact(query: &str) -> Automaton {
        Automaton {
            query_len: query.len(),
            is_exact: true,
            dfa: build_dfa(query),
        }
    }

    fn prefix_exact(query: &str) -> Automaton {
        Automaton {
            query_len: query.len(),
            is_exact: true,
            dfa: build_prefix_dfa(query),
        }
    }

    fn non_exact(query: &str) -> Automaton {
        Automaton {
            query_len: query.len(),
            is_exact: false,
            dfa: build_dfa(query),
        }
    }
}

pub fn normalize_str(string: &str) -> String {
    let mut string = string.to_lowercase();

    if !string.contains(is_cjk) {
        string = deunicode::deunicode_with_tofu(&string, "");
    }

    string
}

fn generate_automatons<S: Store>(query: &str, store: &S) -> Result<(Vec<Automaton>, QueryEnhancer), S::Error> {
    let has_end_whitespace = query.chars().last().map_or(false, char::is_whitespace);
    let query_words: Vec<_> = split_query_string(query).map(str::to_lowercase).collect();
    let synonyms = store.synonyms()?;

    let mut automatons = Vec::new();
    let mut enhancer_builder = QueryEnhancerBuilder::new(&query_words);

    // We must not declare the original words to the query enhancer
    // *but* we need to push them in the automatons list first
    let mut original_words = query_words.iter().peekable();
    while let Some(word) = original_words.next() {

        let has_following_word = original_words.peek().is_some();
        let not_prefix_dfa = has_following_word || has_end_whitespace || word.chars().all(is_cjk);

        let automaton = if not_prefix_dfa {
            Automaton::exact(word)
        } else {
            Automaton::prefix_exact(word)
        };
        automatons.push(automaton);
    }

    for n in 1..=NGRAMS {

        let mut ngrams = query_words.windows(n).enumerate().peekable();
        while let Some((query_index, ngram_slice)) = ngrams.next() {

            let query_range = query_index..query_index + n;
            let ngram_nb_words = ngram_slice.len();
            let ngram = ngram_slice.join(" ");

            let has_following_word = ngrams.peek().is_some();
            let not_prefix_dfa = has_following_word || has_end_whitespace || ngram.chars().all(is_cjk);

            // automaton of synonyms of the ngrams
            let normalized = normalize_str(&ngram);
            let lev = if not_prefix_dfa { build_dfa(&normalized) } else { build_prefix_dfa(&normalized) };

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
                        let synonyms_words: Vec<_> = split_query_string(synonyms).collect();
                        let nb_synonym_words = synonyms_words.len();

                        let real_query_index = automatons.len();
                        enhancer_builder.declare(query_range.clone(), real_query_index, &synonyms_words);

                        for synonym in synonyms_words {
                            let automaton = if nb_synonym_words == 1 {
                                Automaton::exact(synonym)
                            } else {
                                Automaton::non_exact(synonym)
                            };
                            automatons.push(automaton);
                        }
                    }
                }
            }

            if n != 1 {
                // automaton of concatenation of query words
                let concat = ngram_slice.concat();
                let normalized = normalize_str(&concat);

                let real_query_index = automatons.len();
                enhancer_builder.declare(query_range.clone(), real_query_index, &[&normalized]);

                let automaton = Automaton::exact(&normalized);
                automatons.push(automaton);
            }
        }
    }

    Ok((automatons, enhancer_builder.build()))
}

pub struct QueryBuilder<'c, S, FI = fn(DocumentId) -> bool> {
    store: S,
    criteria: Criteria<'c>,
    searchable_attrs: Option<ReorderedAttrs>,
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
        let reorders = self.searchable_attrs.get_or_insert_with(ReorderedAttrs::new);
        reorders.insert_attribute(attribute);
    }
}

fn multiword_rewrite_matches(
    mut matches: Vec<(DocumentId, TmpMatch)>,
    query_enhancer: &QueryEnhancer,
) -> SetBuf<(DocumentId, TmpMatch)>
{
    let mut padded_matches = Vec::with_capacity(matches.len());

    // we sort the matches by word index to make them rewritable
    let start = Instant::now();
    matches.par_sort_unstable_by_key(|(id, match_)| (*id, match_.attribute, match_.word_index));
    info!("rewrite sort by word_index took {:.2?}", start.elapsed());

    let start = Instant::now();
    // for each attribute of each document
    for same_document_attribute in matches.linear_group_by_key(|(id, m)| (*id, m.attribute)) {

        // padding will only be applied
        // to word indices in the same attribute
        let mut padding = 0;
        let mut iter = same_document_attribute.linear_group_by_key(|(_, m)| m.word_index);

        // for each match at the same position
        // in this document attribute
        while let Some(same_word_index) = iter.next() {

            // find the biggest padding
            let mut biggest = 0;
            for (id, match_) in same_word_index {

                let mut replacement = query_enhancer.replacement(match_.query_index);
                let replacement_len = replacement.len();
                let nexts = iter.remainder().linear_group_by_key(|(_, m)| m.word_index);

                if let Some(query_index) = replacement.next() {
                    let word_index = match_.word_index + padding as u16;
                    let match_ = TmpMatch { query_index, word_index, ..match_.clone() };
                    padded_matches.push((*id, match_));
                }

                let mut found = false;

                // look ahead and if there already is a match
                // corresponding to this padding word, abort the padding
                'padding: for (x, next_group) in nexts.enumerate() {

                    for (i, query_index) in replacement.clone().enumerate().skip(x) {
                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                        let padmatch = TmpMatch { query_index, word_index, ..match_.clone() };

                        for (_, nmatch_) in next_group {
                            let mut rep = query_enhancer.replacement(nmatch_.query_index);
                            let query_index = rep.next().unwrap();
                            if query_index == padmatch.query_index {

                                if !found {
                                    // if we find a corresponding padding for the
                                    // first time we must push preceding paddings
                                    for (i, query_index) in replacement.clone().enumerate().take(i) {
                                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                                        let match_ = TmpMatch { query_index, word_index, ..match_.clone() };
                                        padded_matches.push((*id, match_));
                                        biggest = biggest.max(i + 1);
                                    }
                                }

                                padded_matches.push((*id, padmatch));
                                found = true;
                                continue 'padding;
                            }
                        }
                    }

                    // if we do not find a corresponding padding in the
                    // next groups so stop here and pad what was found
                    break
                }

                if !found {
                    // if no padding was found in the following matches
                    // we must insert the entire padding
                    for (i, query_index) in replacement.enumerate() {
                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                        let match_ = TmpMatch { query_index, word_index, ..match_.clone() };
                        padded_matches.push((*id, match_));
                    }

                    biggest = biggest.max(replacement_len - 1);
                }
            }

            padding += biggest;
        }
    }
    info!("main multiword rewrite took {:.2?}", start.elapsed());

    let start = Instant::now();
    for document_matches in padded_matches.linear_group_by_key_mut(|(id, _)| *id) {
        document_matches.sort_unstable();
    }
    info!("final rewrite sort took {:.2?}", start.elapsed());

    SetBuf::new_unchecked(padded_matches)
}

impl<'c, S, FI> QueryBuilder<'c, S, FI>
where S: Store,
{
    fn query_all(&self, query: &str) -> Result<Vec<RawDocument>, S::Error> {
        let (automatons, query_enhancer) = generate_automatons(query, &self.store)?;
        let words = self.store.words()?.as_fst();
        let searchables = self.searchable_attrs.as_ref();

        let mut stream = {
            let mut op_builder = fst::raw::OpBuilder::new();
            for Automaton { dfa, .. } in &automatons {
                let stream = words.search(dfa);
                op_builder.push(stream);
            }
            op_builder.r#union()
        };

        let mut matches = Vec::new();
        let mut highlights = Vec::new();

        let mut query_db = std::time::Duration::default();

        let start = Instant::now();
        while let Some((input, indexed_values)) = stream.next() {
            for iv in indexed_values {
                let Automaton { is_exact, query_len, ref dfa } = automatons[iv.index];
                let distance = dfa.eval(input).to_u8();
                let is_exact = is_exact && distance == 0 && input.len() == query_len;

                let start = Instant::now();
                let doc_indexes = self.store.word_indexes(input)?;
                let doc_indexes = match doc_indexes {
                    Some(doc_indexes) => doc_indexes,
                    None => continue,
                };
                query_db += start.elapsed();

                for di in doc_indexes.as_slice() {
                    let attribute = searchables.map_or(Some(di.attribute), |r| r.get(di.attribute));
                    if let Some(attribute) = attribute {
                        let match_ = TmpMatch {
                            query_index: iv.index as u32,
                            distance,
                            attribute,
                            word_index: di.word_index,
                            is_exact,
                        };

                        let highlight = Highlight {
                            attribute: di.attribute,
                            char_index: di.char_index,
                            char_length: di.char_length,
                        };

                        matches.push((di.document_id, match_));
                        highlights.push((di.document_id, highlight));
                    }
                }
            }
        }
        info!("main query all took {:.2?} (get indexes {:.2?})", start.elapsed(), query_db);

        info!("{} total matches to rewrite", matches.len());

        let start = Instant::now();
        let matches = multiword_rewrite_matches(matches, &query_enhancer);
        info!("multiword rewrite took {:.2?}", start.elapsed());

        let start = Instant::now();
        let highlights = {
            highlights.par_sort_unstable_by_key(|(id, _)| *id);
            SetBuf::new_unchecked(highlights)
        };
        info!("sorting highlights took {:.2?}", start.elapsed());

        info!("{} total matches to classify", matches.len());

        let start = Instant::now();
        let raw_documents = raw_documents_from(matches, highlights);
        info!("making raw documents took {:.2?}", start.elapsed());

        info!("{} total documents to classify", raw_documents.len());

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
        Ok(iter.map(|d| Document::from_raw(d)).collect())
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
                    out_documents.push(Document::from_raw(document));
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
        let set = SetBuf::from_dirty(set.into_iter().map(|s| normalize_str(s)).collect());
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
    fn simple() {
        let store = InMemorySetStore::from_iter(vec![
            ("iphone",  &[doc_char_index(0, 0, 0)][..]),
            ("from",    &[doc_char_index(0, 1, 1)][..]),
            ("apple",   &[doc_char_index(0, 2, 2)][..]),
        ]);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("iphone from apple", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, .. }));
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
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

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonjour", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
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

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonj", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
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

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("saluttion", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn harder_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("hello",   &[doc_index(0, 0)][..]),
            ("bonjour", &[doc_index(1, 3)]),
            ("salut",   &[doc_index(2, 5)]),
        ]);

        store.add_synonym("hello", SetBuf::from_dirty(vec!["bonjour", "salut"]));
        store.add_synonym("bonjour", SetBuf::from_dirty(vec!["hello", "salut"]));
        store.add_synonym("salut", SetBuf::from_dirty(vec!["hello", "bonjour"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("hello", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 3, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 5, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("bonjour", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 3, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 5, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("salut", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 3, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 5, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn unique_to_multiword_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("new",    &[doc_char_index(0, 0, 0)][..]),
            ("york",   &[doc_char_index(0, 1, 1)][..]),
            ("city",   &[doc_char_index(0, 2, 2)][..]),
            ("subway", &[doc_char_index(0, 3, 3)][..]),

            ("NY",     &[doc_char_index(1, 0, 0)][..]),
            ("subway", &[doc_char_index(1, 1, 1)][..]),
        ]);

        store.add_synonym("NY",  SetBuf::from_dirty(vec!["NYC", "new york", "new york city"]));
        store.add_synonym("NYC", SetBuf::from_dirty(vec!["NY",  "new york", "new york city"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NY ± new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NY ± york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // NY ± city
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC ± new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC ± york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // NYC ± city
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn unique_to_multiword_synonyms_words_proximity() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("new",    &[doc_char_index(0, 0, 0)][..]),
            ("york",   &[doc_char_index(0, 1, 1)][..]),
            ("city",   &[doc_char_index(0, 2, 2)][..]),
            ("subway", &[doc_char_index(0, 3, 3)][..]),

            ("york",   &[doc_char_index(1, 0, 0)][..]),
            ("new",    &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),

            ("NY",     &[doc_char_index(2, 0, 0)][..]),
            ("subway", &[doc_char_index(2, 1, 1)][..]),
        ]);

        store.add_synonym("NY",  SetBuf::from_dirty(vec!["york new"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. })); // NY ± york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, .. })); // NY ± new
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. })); // york = NY
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, .. })); // new  = NY
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 1, .. })); // york  = NY
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 0, .. })); // new = NY
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, .. })); // york
            assert_matches!(matches.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 1, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 0, .. })); // new
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn unique_to_multiword_synonyms_cumulative_word_index() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("NY",     &[doc_char_index(0, 0, 0)][..]),
            ("subway", &[doc_char_index(0, 1, 1)][..]),

            ("new",    &[doc_char_index(1, 0, 0)][..]),
            ("york",   &[doc_char_index(1, 1, 1)][..]),
            ("subway", &[doc_char_index(1, 2, 2)][..]),
        ]);

        store.add_synonym("new york", SetBuf::from_dirty(vec!["NY"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NY
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 2, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    /// Unique word has multi-word synonyms
    fn harder_unique_to_multiword_synonyms_one() {
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

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // NYC
            //                                                       because one-word to one-word ^^^^
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway
            assert_matches!(iter.next(), None);             // position rewritten ^
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

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: false, .. })); // underground = subway
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 5, is_exact: false, .. })); // train       = subway
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 5, word_index: 6, is_exact: true,  .. })); // broken
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NY
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // underground = subway
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 5, is_exact: true, .. })); // train       = subway
            assert_matches!(iter.next(), None);                // position rewritten ^
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NYC subway", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city = NYC
            //                                                       because one-word to one-word ^^^^
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // underground = subway
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 5, is_exact: true, .. })); // train       = subway
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new  = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city = NYC
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: false, .. })); // underground = subway
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 5, is_exact: false, .. })); // train       = subway
            assert_matches!(iter.next(), None);                // position rewritten ^
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

            ("new",         &[doc_char_index(2, 0, 0)][..]),
            ("york",        &[doc_char_index(2, 1, 1)][..]),
            ("underground", &[doc_char_index(2, 2, 2)][..]),
            ("train",       &[doc_char_index(2, 3, 3)][..]),
            ("broken",      &[doc_char_index(2, 4, 4)][..]),
        ]);

        store.add_synonym("new york", SetBuf::from_dirty(vec![          "NYC", "NY", "new york city" ]));
        store.add_synonym("new york city", SetBuf::from_dirty(vec![     "NYC", "NY", "new york"      ]));
        store.add_synonym("underground train", SetBuf::from_dirty(vec![ "subway"                     ]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york underground train broken", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // underground
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // train
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // broken
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // NYC = new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // NYC = york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // NYC = city
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true,  .. })); // subway = underground
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 5, is_exact: true,  .. })); // subway = train
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 5, word_index: 6, is_exact: true,  .. })); // broken
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // NY = new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // NY = york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // NY = city
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway = underground
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // subway = train
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york city underground train broken", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // underground
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // train
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // broken
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC = new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // NYC = new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC = york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // NYC = york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // NYC = city
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 4, is_exact: true, .. })); // subway = underground
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 5, is_exact: true, .. })); // subway = train
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 5, word_index: 6, is_exact: true, .. })); // broken
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // NY = new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // NY = new
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // NY = york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // NY = york
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // NY = city
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // subway = underground
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // subway = train
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn intercrossed_multiword_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("new",   &[doc_index(0, 0)][..]),
            ("york",  &[doc_index(0, 1)][..]),
            ("big",   &[doc_index(0, 2)][..]),
            ("city",  &[doc_index(0, 3)][..]),
        ]);

        store.add_synonym("new york", SetBuf::from_dirty(vec![      "new york city" ]));
        store.add_synonym("new york city", SetBuf::from_dirty(vec![ "new york"      ]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york big ", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: false, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new

            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york

            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 4, is_exact: false, .. })); // city

            assert_matches!(matches.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // big
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let mut store = InMemorySetStore::from_iter(vec![
            ("NY",     &[doc_index(0, 0)][..]),
            ("city",   &[doc_index(0, 1)][..]),
            ("subway", &[doc_index(0, 2)][..]),

            ("NY",     &[doc_index(1, 0)][..]),
            ("subway", &[doc_index(1, 1)][..]),

            ("NY",     &[doc_index(2, 0)][..]),
            ("york",   &[doc_index(2, 1)][..]),
            ("city",   &[doc_index(2, 2)][..]),
            ("subway", &[doc_index(2, 3)][..]),
        ]);

        store.add_synonym("NY", SetBuf::from_dirty(vec!["new york city story"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("NY subway ", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true, .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true, .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true, .. })); // story
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 4, word_index: 4, is_exact: true, .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(2), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: false, .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: false, .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 4, word_index: 3, is_exact: true,  .. })); // subway
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn cumulative_word_indices() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("NYC",    &[doc_index(0, 0)][..]),
            ("long",   &[doc_index(0, 1)][..]),
            ("subway", &[doc_index(0, 2)][..]),
            ("cool",   &[doc_index(0, 3)][..]),
        ]);

        store.add_synonym("new york city", SetBuf::from_dirty(vec!["NYC"]));
        store.add_synonym("subway",        SetBuf::from_dirty(vec!["underground train"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("new york city long subway cool ", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 0, word_index: 0, is_exact: true,  .. })); // new  = NYC
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 1, word_index: 1, is_exact: true,  .. })); // york = NYC
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 2, word_index: 2, is_exact: true,  .. })); // city = NYC
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 3, word_index: 3, is_exact: true,  .. })); // long
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 4, word_index: 4, is_exact: true,  .. })); // subway = underground
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 5, word_index: 5, is_exact: true,  .. })); // subway = train
            assert_matches!(matches.next(), Some(TmpMatch { query_index: 6, word_index: 6, is_exact: true,  .. })); // cool
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn deunicoded_synonyms() {
        let mut store = InMemorySetStore::from_iter(vec![
            ("telephone", &[doc_index(0, 0)][..]), // meilidb-data indexes the unidecoded
            ("téléphone", &[doc_index(0, 0)][..]), // and the original words with the same DocIndex

            ("iphone",    &[doc_index(1, 0)][..]),
        ]);

        store.add_synonym("téléphone", SetBuf::from_dirty(vec!["iphone"]));

        let builder = QueryBuilder::new(&store);
        let results = builder.query("telephone", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("téléphone", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("télephone", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(1), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, .. }));
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, distance: 1, word_index: 0, is_exact: false, .. })); // iphone
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, distance: 1, word_index: 0, is_exact: false, .. })); // téléphone
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }

    #[test]
    fn simple_concatenation() {
        let store = InMemorySetStore::from_iter(vec![
            ("iphone",  &[doc_index(0, 0)][..]),
            ("case",    &[doc_index(0, 1)][..]),
        ]);

        let builder = QueryBuilder::new(&store);
        let results = builder.query("i phone case", 0..20).unwrap();
        let mut iter = results.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut iter = matches.into_iter();
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 0, word_index: 0, distance: 0, .. })); // iphone
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 1, distance: 0, .. })); // iphone
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 1, word_index: 0, distance: 1, .. })); // phone
            assert_matches!(iter.next(), Some(TmpMatch { query_index: 2, word_index: 2, distance: 0, .. })); // case
            assert_matches!(iter.next(), None);
        });
        assert_matches!(iter.next(), None);
    }
}
