use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::{Range, Deref};
use std::time::Duration;

use either::Either;
use sdset::{SetOperation, Set, SetBuf};

use meilisearch_schema::FieldId;

use crate::database::MainT;
use crate::{criterion::Criteria, DocumentId};
use crate::{reordered_attrs::ReorderedAttrs, store, MResult};
use crate::facets::FacetFilter;

use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::fmt;

use compact_arena::{SmallArena, Idx32, mk_arena};
use log::debug;
use meilisearch_types::DocIndex;
use sdset::{exponential_search, Counter, duo::OpBuilder};
use slice_group_by::{GroupBy, GroupByMut};

use crate::error::Error;
use crate::criterion::{Context, ContextMut};
use crate::distinct_map::{BufferedDistinctMap, DistinctMap};
use crate::raw_document::RawDocument;
use crate::Document;
use crate::query_tree::{create_query_tree, traverse_query_tree};
use crate::query_tree::{Operation, QueryResult, QueryKind, QueryId, PostingsKey};
use crate::query_tree::Context as QTContext;

pub struct QueryBuilder<'c, 'f, 'd, 'i, 'txn> {
    pub criteria: Criteria<'c>,
    pub searchable_attrs: Option<ReorderedAttrs>,
    pub filter: Option<Box<dyn Fn(DocumentId) -> bool + 'f>>,
    pub distinct: Option<(Box<dyn Fn(DocumentId) -> Option<u64> + 'd>, usize)>,
    pub timeout: Option<Duration>,
    pub index: &'i store::Index,
    pub facet_filter: Option<SetBuf<DocumentId>>,
    pub facets: Option<HashMap<String, HashMap<String, Cow<'txn, Set<DocumentId>>>>>,
}

#[derive(Debug, Default)]
pub struct SortResult {
    pub documents: Vec<Document>,
    pub nb_hits: usize,
    pub exhaustive_nb_hit: bool,
    pub facets: Option<HashMap<String, HashMap<String, usize>>>,
    pub exhaustive_facet_count: Option<bool>,
}

impl<'c, 'f, 'd, 'i, 'txn> QueryBuilder<'c, 'f, 'd, 'i, 'txn> {
    pub fn new(index: &'i store::Index) -> Self {
        QueryBuilder::with_criteria(
            index,
            Criteria::default(),
        )
    }

    /// set the facet filter for the query. Internally, it transforms the `FacetFilter` in a Set of
    /// `DocumentId` that correspond to the filter.
    pub fn with_facet_filter(
        &mut self,
        reader: &heed::RoTxn<MainT>,
        facets: FacetFilter
    ) -> MResult<()> {
        let mut ands = Vec::with_capacity(facets.len());
        let mut ors = Vec::new();
        for f in facets.deref() {
            match f {
                Either::Left(keys) => {
                    ors.reserve(keys.len());
                    for key in keys {
                        let docids = self.index.facets.facet_document_ids(reader, &key)?.unwrap_or_default();
                        ors.push(docids);
                    }
                    let sets: Vec<_> = ors.iter().map(Cow::deref).collect();
                    let or_result = sdset::multi::OpBuilder::from_vec(sets).union().into_set_buf();
                    ands.push(Cow::Owned(or_result));
                    ors.clear();
                }
                Either::Right(key) =>{
                    match self.index.facets.facet_document_ids(reader, &key)? {
                        Some(docids) => ands.push(docids),
                        // intersection with nothing is the empty set.
                        None => {
                            self.facet_filter = Some(SetBuf::new_unchecked(Vec::new()));
                            return Ok(());
                        },
                    }
                }
            }
        }
        let ands: Vec<_> = ands.iter().map(Cow::deref).collect();
        self.facet_filter = Some(sdset::multi::OpBuilder::from_vec(ands).intersection().into_set_buf());
        Ok(())
    }

    /// Sets the facets for which to retrieve count. Internally, transforms the `Vec` of fields into
    /// a mapping between field values and documents.
    pub fn with_facets_count(
        &mut self,
        reader: &'txn heed::RoTxn<MainT>,
        facets: Vec<(FieldId, String)>,
    ) -> MResult<()> {
        let mut facet_count_map = HashMap::new();
        for (field_id, field_name) in facets {
            let mut key_map = HashMap::new();
            for pair in self.index.facets.field_document_ids(reader, field_id)? {
                let (facet_key, document_ids) = pair?;
                let value = facet_key.value();
                key_map.insert(value.to_string(), document_ids);
            }
            facet_count_map.insert(field_name, key_map);
        }
        self.facets = Some(facet_count_map);
        Ok(())
    }

    pub fn with_criteria(
        index: &'i store::Index,
        criteria: Criteria<'c>,
    ) -> Self {
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

    pub fn query(
        self,
        reader: &heed::RoTxn<MainT>,
        query: &str,
        range: Range<usize>,
    ) -> MResult<SortResult> {

        // When there is no candidate document in the facet filter, we don't perform the search,
        // as the result will alway be empty.
        if let Some(ref facet_filter) = self.facet_filter {
            if facet_filter.is_empty() {
                return Ok(SortResult::default())
            }
        }

        if self.distinct.is_some() {
            self.bucket_sort_with_distinct(reader, query, range)
        } else {
            self.bucket_sort(reader, query, range)
        }
    }

    pub fn bucket_sort(
        mut self,
        reader: &heed::RoTxn<MainT>,
        query: &str,
        range: Range<usize>,
    ) -> MResult<SortResult>
    {
        // We delegate the filter work to the distinct query builder,
        // specifying a distinct rule that has no effect.
        if self.filter.is_some() {
            self.distinct = Some((Box::new(|_| None ), 1));
            return self.bucket_sort_with_distinct(
                reader,
                query,
                range,
            );
        }

        let mut result = SortResult::default();

        let words_set = match unsafe { self.index.main.static_words_fst(reader)? } {
            Some(words) => words,
            None => return Ok(SortResult::default()),
        };

        let stop_words = self.index.main.stop_words_fst(reader)?.unwrap_or_default();

        let context = QTContext {
            words_set,
            stop_words,
            synonyms: self.index.synonyms,
            postings_lists: self.index.postings_lists,
            prefix_postings_lists: self.index.prefix_postings_lists_cache,
        };

        let (operation, mapping) = create_query_tree(reader, &context, query)?;
        debug!("operation:\n{:?}", operation);
        debug!("mapping:\n{:?}", mapping);

        fn recurs_operation<'o>(map: &mut HashMap<QueryId, &'o QueryKind>, operation: &'o Operation) {
            match operation {
                Operation::And(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
                Operation::Or(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
                Operation::Query(query) => { map.insert(query.id, &query.kind); },
            }
        }

        let mut queries_kinds = HashMap::new();
        recurs_operation(&mut queries_kinds, &operation);

        let QueryResult { mut docids, queries } = traverse_query_tree(reader, &context, &operation)?;
        debug!("found {} documents", docids.len());
        debug!("number of postings {:?}", queries.len());

        if let Some(ref facets_docids) = self.facet_filter {
            let intersection = sdset::duo::OpBuilder::new(docids.as_ref(), facets_docids.as_set())
                .intersection()
                .into_set_buf();
            docids = Cow::Owned(intersection);
        }

        if let Some(f) = self.facets.take() {
            // hardcoded value, until approximation optimization
            result.exhaustive_facet_count = Some(true);
            result.facets = Some(facet_count(f, &docids));
        }

        let before = Instant::now();
        mk_arena!(arena);
        let mut bare_matches = cleanup_bare_matches(&mut arena, &docids, queries);
        debug!("matches cleaned in {:.02?}", before.elapsed());

        let before_bucket_sort = Instant::now();

        let before_raw_documents_building = Instant::now();
        let mut raw_documents = Vec::new();
        for bare_matches in bare_matches.linear_group_by_key_mut(|sm| sm.document_id) {
            let raw_document = RawDocument::new(bare_matches, &mut arena, self.searchable_attrs.as_ref());
            raw_documents.push(raw_document);
        }
        debug!("creating {} candidates documents took {:.02?}",
            raw_documents.len(),
            before_raw_documents_building.elapsed(),
        );

        let before_criterion_loop = Instant::now();
        let proximity_count = AtomicUsize::new(0);

        let mut groups = vec![raw_documents.as_mut_slice()];

        'criteria: for criterion in self.criteria.as_ref() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut documents_seen = 0;

            for mut group in tmp_groups {
                let before_criterion_preparation = Instant::now();

                let ctx = ContextMut {
                    reader,
                    postings_lists: &mut arena,
                    query_mapping: &mapping,
                    documents_fields_counts_store: self.index.documents_fields_counts,
                };

                criterion.prepare(ctx, &mut group)?;
                debug!("{:?} preparation took {:.02?}", criterion.name(), before_criterion_preparation.elapsed());

                let ctx = Context {
                    postings_lists: &arena,
                    query_mapping: &mapping,
                };

                let before_criterion_sort = Instant::now();
                group.sort_unstable_by(|a, b| criterion.evaluate(&ctx, a, b));
                debug!("{:?} evaluation took {:.02?}", criterion.name(), before_criterion_sort.elapsed());

                for group in group.binary_group_by_mut(|a, b| criterion.eq(&ctx, a, b)) {
                    debug!("{:?} produced a group of size {}", criterion.name(), group.len());

                    documents_seen += group.len();
                    groups.push(group);

                    // we have sort enough documents if the last document sorted is after
                    // the end of the requested range, we can continue to the next criterion
                    if documents_seen >= range.end {
                        continue 'criteria;
                    }
                }
            }
        }

        debug!("criterion loop took {:.02?}", before_criterion_loop.elapsed());
        debug!("proximity evaluation called {} times", proximity_count.load(Ordering::Relaxed));

        let schema = self.index.main.schema(reader)?.ok_or(Error::SchemaMissing)?;
        let iter = raw_documents.into_iter().skip(range.start).take(range.len());
        let iter = iter.map(|rd| Document::from_raw(rd, &queries_kinds, &arena, self.searchable_attrs.as_ref(), &schema));
        let documents = iter.collect();

        debug!("bucket sort took {:.02?}", before_bucket_sort.elapsed());

        result.documents = documents;
        result.nb_hits = docids.len();

        Ok(result)
    }

    pub fn bucket_sort_with_distinct(
        self,
        reader: &heed::RoTxn<MainT>,
        query: &str,
        range: Range<usize>,
    ) -> MResult<SortResult>
    {

        let mut result = SortResult::default();

        let (distinct, distinct_size) = self.distinct.expect("Bucket_sort_with_distinct need distinct");

        let words_set = match unsafe { self.index.main.static_words_fst(reader)? } {
            Some(words) => words,
            None => return Ok(SortResult::default()),
        };

        let stop_words = self.index.main.stop_words_fst(reader)?.unwrap_or_default();

        let context = QTContext {
            words_set,
            stop_words,
            synonyms: self.index.synonyms,
            postings_lists: self.index.postings_lists,
            prefix_postings_lists: self.index.prefix_postings_lists_cache,
        };

        let (operation, mapping) = create_query_tree(reader, &context, query)?;
        debug!("operation:\n{:?}", operation);
        debug!("mapping:\n{:?}", mapping);

        fn recurs_operation<'o>(map: &mut HashMap<QueryId, &'o QueryKind>, operation: &'o Operation) {
            match operation {
                Operation::And(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
                Operation::Or(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
                Operation::Query(query) => { map.insert(query.id, &query.kind); },
            }
        }

        let mut queries_kinds = HashMap::new();
        recurs_operation(&mut queries_kinds, &operation);

        let QueryResult { mut docids, queries } = traverse_query_tree(reader, &context, &operation)?;
        debug!("found {} documents", docids.len());
        debug!("number of postings {:?}", queries.len());

        if let Some(facets_docids) = self.facet_filter {
            let intersection = OpBuilder::new(docids.as_ref(), facets_docids.as_set())
                .intersection()
                .into_set_buf();
            docids = Cow::Owned(intersection);
        }

        if let Some(f) = self.facets {
            // hardcoded value, until approximation optimization
            result.exhaustive_facet_count = Some(true);
            result.facets = Some(facet_count(f, &docids));
        }

        let before = Instant::now();
        mk_arena!(arena);
        let mut bare_matches = cleanup_bare_matches(&mut arena, &docids, queries);
        debug!("matches cleaned in {:.02?}", before.elapsed());

        let before_raw_documents_building = Instant::now();
        let mut raw_documents = Vec::new();
        for bare_matches in bare_matches.linear_group_by_key_mut(|sm| sm.document_id) {
            let raw_document = RawDocument::new(bare_matches, &mut arena, self.searchable_attrs.as_ref());
            raw_documents.push(raw_document);
        }
        debug!("creating {} candidates documents took {:.02?}",
            raw_documents.len(),
            before_raw_documents_building.elapsed(),
        );

        let mut groups = vec![raw_documents.as_mut_slice()];
        let mut key_cache = HashMap::new();

        let mut filter_map = HashMap::new();
        // these two variables informs on the current distinct map and
        // on the raw offset of the start of the group where the
        // range.start bound is located according to the distinct function
        let mut distinct_map = DistinctMap::new(distinct_size);
        let mut distinct_raw_offset = 0;

        'criteria: for criterion in self.criteria.as_ref() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut buf_distinct = BufferedDistinctMap::new(&mut distinct_map);
            let mut documents_seen = 0;

            for mut group in tmp_groups {
                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < distinct_raw_offset {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                let ctx = ContextMut {
                    reader,
                    postings_lists: &mut arena,
                    query_mapping: &mapping,
                    documents_fields_counts_store: self.index.documents_fields_counts,
                };

                let before_criterion_preparation = Instant::now();
                criterion.prepare(ctx, &mut group)?;
                debug!("{:?} preparation took {:.02?}", criterion.name(), before_criterion_preparation.elapsed());

                let ctx = Context {
                    postings_lists: &arena,
                    query_mapping: &mapping,
                };

                let before_criterion_sort = Instant::now();
                group.sort_unstable_by(|a, b| criterion.evaluate(&ctx, a, b));
                debug!("{:?} evaluation took {:.02?}", criterion.name(), before_criterion_sort.elapsed());

                for group in group.binary_group_by_mut(|a, b| criterion.eq(&ctx, a, b)) {
                    // we must compute the real distinguished len of this sub-group
                    for document in group.iter() {
                        let filter_accepted = match &self.filter {
                            Some(filter) => {
                                let entry = filter_map.entry(document.id);
                                *entry.or_insert_with(|| (filter)(document.id))
                            }
                            None => true,
                        };

                        if filter_accepted {
                            let entry = key_cache.entry(document.id);
                            let key = entry.or_insert_with(|| (distinct)(document.id).map(Rc::new));

                            match key.clone() {
                                Some(key) => buf_distinct.register(key),
                                None => buf_distinct.register_without_key(),
                            };
                        }

                        // the requested range end is reached: stop computing distinct
                        if buf_distinct.len() >= range.end {
                            break;
                        }
                    }

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
                    if buf_distinct.len() >= range.end {
                        continue 'criteria;
                    }
                }
            }
        }

        // once we classified the documents related to the current
        // automatons we save that as the next valid result
        let mut seen = BufferedDistinctMap::new(&mut distinct_map);
        let schema = self.index.main.schema(reader)?.ok_or(Error::SchemaMissing)?;

        let mut documents = Vec::with_capacity(range.len());
        for raw_document in raw_documents.into_iter().skip(distinct_raw_offset) {
            let filter_accepted = match &self.filter {
                Some(_) => filter_map.remove(&raw_document.id).unwrap(),
                None => true,
            };

            if filter_accepted {
                let key = key_cache.remove(&raw_document.id).unwrap();
                let distinct_accepted = match key {
                    Some(key) => seen.register(key),
                    None => seen.register_without_key(),
                };

                if distinct_accepted && seen.len() > range.start {
                    documents.push(Document::from_raw(raw_document, &queries_kinds, &arena, self.searchable_attrs.as_ref(), &schema));
                    if documents.len() == range.len() {
                        break;
                    }
                }
            }
        }
        result.documents = documents;
        result.nb_hits = docids.len();

        Ok(result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimpleMatch {
    pub query_index: usize,
    pub distance: u8,
    pub attribute: u16,
    pub word_index: u16,
    pub is_exact: bool,
}

fn facet_count(
    facet_docids: HashMap<String, HashMap<String, Cow<Set<DocumentId>>>>,
    candidate_docids: &Set<DocumentId>,
) -> HashMap<String, HashMap<String, usize>> {
    let mut facets_counts = HashMap::with_capacity(facet_docids.len());
    for (key, doc_map) in facet_docids {
        let mut count_map = HashMap::with_capacity(doc_map.len());
        for (value, docids) in doc_map {
            let mut counter = Counter::new();
            let op = OpBuilder::new(docids.as_ref(), candidate_docids).intersection();
            SetOperation::<DocumentId>::extend_collection(op, &mut counter);
            count_map.insert(value, counter.0);
        }
        facets_counts.insert(key, count_map);
    }
    facets_counts
}

pub struct BareMatch<'tag> {
    pub document_id: DocumentId,
    pub query_index: usize,
    pub distance: u8,
    pub is_exact: bool,
    pub postings_list: Idx32<'tag>,
}

#[derive(Clone)]
pub enum PostingsListView<'txn> {
    Original {
        input: Rc<[u8]>,
        postings_list: Rc<Cow<'txn, Set<DocIndex>>>,
        offset: usize,
        len: usize,
    },
    Rewritten {
        input: Rc<[u8]>,
        postings_list: SetBuf<DocIndex>,
    },
}

impl fmt::Debug for PostingsListView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostingsListView")
            .field("input", &std::str::from_utf8(&self.input()).unwrap())
            .field("postings_list", &self.as_ref())
            .finish()
    }
}

impl<'txn> PostingsListView<'txn> {
    pub fn original(input: Rc<[u8]>, postings_list: Rc<Cow<'txn, Set<DocIndex>>>) -> PostingsListView<'txn> {
        let len = postings_list.len();
        PostingsListView::Original { input, postings_list, offset: 0, len }
    }

    pub fn rewritten(input: Rc<[u8]>, postings_list: SetBuf<DocIndex>) -> PostingsListView<'txn> {
        PostingsListView::Rewritten { input, postings_list }
    }

    pub fn rewrite_with(&mut self, postings_list: SetBuf<DocIndex>) {
        let input = match self {
            PostingsListView::Original { input, .. } => input.clone(),
            PostingsListView::Rewritten { input, .. } => input.clone(),
        };
        *self = PostingsListView::rewritten(input, postings_list);
    }

    pub fn len(&self) -> usize {
        match self {
            PostingsListView::Original { len, .. } => *len,
            PostingsListView::Rewritten { postings_list, .. } => postings_list.len(),
        }
    }

    pub fn input(&self) -> &[u8] {
        match self {
            PostingsListView::Original { ref input, .. } => input,
            PostingsListView::Rewritten { ref input, .. } => input,
        }
    }

    pub fn range(&self, range_offset: usize, range_len: usize) -> PostingsListView<'txn> {
        match self {
            PostingsListView::Original { input, postings_list, offset, len } => {
                assert!(range_offset + range_len <= *len);
                PostingsListView::Original {
                    input: input.clone(),
                    postings_list: postings_list.clone(),
                    offset: offset + range_offset,
                    len: range_len,
                }
            },
            PostingsListView::Rewritten { .. } => {
                panic!("Cannot create a range on a rewritten postings list view");
            }
        }
    }
}

impl AsRef<Set<DocIndex>> for PostingsListView<'_> {
    fn as_ref(&self) -> &Set<DocIndex> {
        self
    }
}

impl Deref for PostingsListView<'_> {
    type Target = Set<DocIndex>;

    fn deref(&self) -> &Set<DocIndex> {
        match *self {
            PostingsListView::Original { ref postings_list, offset, len, .. } => {
                Set::new_unchecked(&postings_list[offset..offset + len])
            },
            PostingsListView::Rewritten { ref postings_list, .. } => postings_list,
        }
    }
}
fn cleanup_bare_matches<'tag, 'txn>(
    arena: &mut SmallArena<'tag, PostingsListView<'txn>>,
    docids: &Set<DocumentId>,
    queries: HashMap<PostingsKey, Cow<'txn, Set<DocIndex>>>,
) -> Vec<BareMatch<'tag>>
{
    let docidslen = docids.len() as f32;
    let mut bare_matches = Vec::new();

    for (PostingsKey { query, input, distance, is_exact }, matches) in queries {
        let postings_list_view = PostingsListView::original(Rc::from(input), Rc::new(matches));
        let pllen = postings_list_view.len() as f32;

        if docidslen / pllen >= 0.8 {
            let mut offset = 0;
            for matches in postings_list_view.linear_group_by_key(|m| m.document_id) {
                let document_id = matches[0].document_id;
                if docids.contains(&document_id) {
                    let range = postings_list_view.range(offset, matches.len());
                    let posting_list_index = arena.add(range);

                    let bare_match = BareMatch {
                        document_id,
                        query_index: query.id,
                        distance,
                        is_exact,
                        postings_list: posting_list_index,
                    };

                    bare_matches.push(bare_match);
                }

                offset += matches.len();
            }

        } else {
            let mut offset = 0;
            for id in docids.as_slice() {
                let di = DocIndex { document_id: *id, ..DocIndex::default() };
                let pos = exponential_search(&postings_list_view[offset..], &di).unwrap_or_else(|x| x);

                offset += pos;

                let group = postings_list_view[offset..]
                    .linear_group_by_key(|m| m.document_id)
                    .next()
                    .filter(|matches| matches[0].document_id == *id);

                if let Some(matches) = group {
                    let range = postings_list_view.range(offset, matches.len());
                    let posting_list_index = arena.add(range);

                    let bare_match = BareMatch {
                        document_id: *id,
                        query_index: query.id,
                        distance,
                        is_exact,
                        postings_list: posting_list_index,
                    };

                    bare_matches.push(bare_match);
                }
            }
        }
    }

    let before_raw_documents_presort = Instant::now();
    bare_matches.sort_unstable_by_key(|sm| sm.document_id);
    debug!("sort by documents ids took {:.02?}", before_raw_documents_presort.elapsed());

    bare_matches
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
    use crate::Document;
    use crate::automaton::normalize_str;
    use crate::query_builder::SimpleMatch;
    use crate::database::{Database,DatabaseOptions};
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
            let database = Database::open_or_create(&tempdir, DatabaseOptions::default()).unwrap();
            let index = database.create_index("default").unwrap();

            let db = &database;
            let mut writer = db.main_write_txn().unwrap();

            let mut words_fst = BTreeSet::new();
            let mut postings_lists = HashMap::new();
            let mut fields_counts = HashMap::<_, u16>::new();

            let mut schema = Schema::with_primary_key("id");

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
        let SortResult { documents, .. } = builder.query(&reader, "iphone from apple", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult { documents, .. } = builder.query(&reader, "hello", 0..20).unwrap();
        let mut iter = documents.into_iter();

        assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
            let mut matches = matches.into_iter();
            assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
            assert_matches!(matches.next(), None);
        });
        assert_matches!(iter.next(), None);

        let builder = store.query_builder();
        let SortResult { documents, .. } = builder.query(&reader, "bonjour", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
    //     let mut iter = documents.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "bonj", 0..20).unwrap();
    //     let mut iter = documents.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "sal blabla", 0..20).unwrap();
    //     let mut iter = documents.into_iter();

    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "bonj blabla", 0..20).unwrap();
    //     let mut iter = documents.into_iter();

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
    //     let mut iter = documents.into_iter();

    //     assert_matches!(iter.next(), Some(Document { id: DocumentId(0), matches, .. }) => {
    //         let mut matches = matches.into_iter();
    //         assert_matches!(matches.next(), Some(SimpleMatch { query_index: 0, word_index: 0, .. }));
    //         assert_matches!(matches.next(), None);
    //     });
    //     assert_matches!(iter.next(), None);

    //     let builder = store.query_builder();
    //     let results = builder.query(&reader, "saluttion", 0..20).unwrap();
    //     let mut iter = documents.into_iter();

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
        let SortResult { documents, .. } = builder.query(&reader, "hello", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult { documents, .. } = builder.query(&reader, "bonjour", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult { documents, .. } = builder.query(&reader, "salut", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult { documents, .. } = builder.query(&reader, "NY subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult { documents, .. } = builder.query(&reader, "NYC subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NY", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "new york", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NY subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "new york subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NY subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NYC subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NY subway broken", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NYC subway", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder
            .query(&reader, "new york underground train broken", 0..20)
            .unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder
            .query(&reader, "new york city underground train broken", 0..20)
            .unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "new york big ", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "NY subway ", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder
            .query(&reader, "new york city long subway cool ", 0..20)
            .unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "telephone", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "téléphone", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "télephone", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "i phone case", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "searchengine", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "searchengine", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
        let SortResult {documents, .. } = builder.query(&reader, "searchengine", 0..20).unwrap();
        let mut iter = documents.into_iter();

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
