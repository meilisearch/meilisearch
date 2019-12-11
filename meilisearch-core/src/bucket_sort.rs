use std::ops::Deref;
use std::fmt;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::io::Write;
use std::mem;
use std::ops::Range;
use std::rc::Rc;
use std::time::{Duration, Instant};

use compact_arena::{SmallArena, Idx32, mk_arena};
use fst::{IntoStreamer, Streamer};
use levenshtein_automata::DFA;
use log::debug;
use meilisearch_tokenizer::{is_cjk, split_query_string};
use meilisearch_types::{DocIndex, Highlight};
use sdset::{Set, SetBuf};
use slice_group_by::{GroupBy, GroupByMut};
use itertools::EitherOrBoth;

use crate::automaton::NGRAMS;
use crate::automaton::{QueryEnhancer, QueryEnhancerBuilder};
use crate::automaton::{build_dfa, build_prefix_dfa, build_exact_dfa};
use crate::automaton::{normalize_str, split_best_frequency};

use crate::criterion2::*;
use crate::levenshtein::prefix_damerau_levenshtein;
use crate::{database::MainT, reordered_attrs::ReorderedAttrs};
use crate::{store, Document, DocumentId, MResult};

pub fn bucket_sort<'c>(
    reader: &heed::RoTxn<MainT>,
    query: &str,
    range: Range<usize>,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    synonyms_store: store::Synonyms,
) -> MResult<Vec<Document>>
{
    let (automatons, query_enhancer) =
        construct_automatons2(reader, query, main_store, postings_lists_store, synonyms_store)?;

    debug!("{:?}", query_enhancer);

    let before_postings_lists_fetching = Instant::now();
    mk_arena!(arena);
    let mut bare_matches = fetch_matches(reader, &automatons, &mut arena, main_store, postings_lists_store)?;
    debug!("bare matches ({}) retrieved in {:.02?}",
        bare_matches.len(),
        before_postings_lists_fetching.elapsed(),
    );

    let before_raw_documents_presort = Instant::now();
    bare_matches.sort_unstable_by_key(|sm| sm.document_id);
    debug!("sort by documents ids took {:.02?}", before_raw_documents_presort.elapsed());

    let before_raw_documents_building = Instant::now();
    let mut prefiltered_documents = 0;
    let mut raw_documents = Vec::new();
    for raw_matches in bare_matches.linear_group_by_key_mut(|sm| sm.document_id) {
        prefiltered_documents += 1;
        if let Some(raw_document) = RawDocument::new(raw_matches, &automatons, &mut arena) {
            raw_documents.push(raw_document);
        }
    }
    debug!("creating {} (original {}) candidates documents took {:.02?}",
        raw_documents.len(),
        prefiltered_documents,
        before_raw_documents_building.elapsed(),
    );

    dbg!(mem::size_of::<BareMatch>());
    dbg!(mem::size_of::<SimpleMatch>());

    let mut groups = vec![raw_documents.as_mut_slice()];

    let criteria = [
        Box::new(Typo) as Box<dyn Criterion>,
        Box::new(Words),
        Box::new(Proximity),
        Box::new(Attribute),
        Box::new(WordsPosition),
        Box::new(Exact),
        Box::new(StableDocId),
    ];

    'criteria: for criterion in &criteria {
        let tmp_groups = mem::replace(&mut groups, Vec::new());
        let mut documents_seen = 0;

        for mut group in tmp_groups {
            let before_criterion_preparation = Instant::now();
            criterion.prepare(&mut group, &mut arena, &query_enhancer, &automatons);
            debug!("{:?} preparation took {:.02?}", criterion.name(), before_criterion_preparation.elapsed());

            let before_criterion_sort = Instant::now();
            group.sort_unstable_by(|a, b| criterion.evaluate(a, b, &arena));
            debug!("{:?} evaluation took {:.02?}", criterion.name(), before_criterion_sort.elapsed());

            for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b, &arena)) {
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

    let iter = raw_documents.into_iter().skip(range.start).take(range.len());
    let iter = iter.map(|d| {
        let highlights = d.raw_matches.iter().flat_map(|sm| {
            let postings_list = &arena[sm.postings_list];
            let input = postings_list.input();
            let query = &automatons[sm.query_index as usize].query;
            postings_list.iter().map(move |m| {
                let covered_area = if query.len() > input.len() {
                    input.len()
                } else {
                    prefix_damerau_levenshtein(query.as_bytes(), input).1
                };
                Highlight { attribute: m.attribute, char_index: m.char_index, char_length: covered_area as u16 }
            })
        }).collect();

        Document {
            id: d.raw_matches[0].document_id,
            highlights,
            #[cfg(test)] matches: Vec::new(),
        }
    });

    Ok(iter.collect())
}

pub struct RawDocument<'a, 'tag> {
    pub raw_matches: &'a mut [BareMatch<'tag>],
    pub processed_matches: Vec<SimpleMatch>,
    /// The list of minimum `distance` found
    pub processed_distances: Vec<Option<u8>>,
}

impl<'a, 'tag> RawDocument<'a, 'tag> {
    fn new<'txn>(
        raw_matches: &'a mut [BareMatch<'tag>],
        automatons: &[QueryWordAutomaton],
        postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
    ) -> Option<RawDocument<'a, 'tag>>
    {
        raw_matches.sort_unstable_by_key(|m| m.query_index);

        let mut previous_word = None;
        for i in 0..raw_matches.len() {
            let a = &raw_matches[i];
            let auta = &automatons[a.query_index as usize];

            match auta.phrase_query {
                Some((0, _)) => {
                    let b = match raw_matches.get(i + 1) {
                        Some(b) => b,
                        None => {
                            postings_lists[a.postings_list].rewrite_with(SetBuf::default());
                            continue;
                        }
                    };

                    if a.query_index + 1 != b.query_index {
                        postings_lists[a.postings_list].rewrite_with(SetBuf::default());
                        continue
                    }

                    let pla = &postings_lists[a.postings_list];
                    let plb = &postings_lists[b.postings_list];

                    let mut iter = itertools::merge_join_by(pla.iter(), plb.iter(), |a, b| {
                        a.attribute.cmp(&b.attribute).then((a.word_index + 1).cmp(&b.word_index))
                    });

                    let mut newa = Vec::new();
                    let mut newb = Vec::new();

                    for eb in iter {
                        if let EitherOrBoth::Both(a, b) = eb {
                            newa.push(*a);
                            newb.push(*b);
                        }
                    }

                    if !newa.is_empty() {
                        previous_word = Some(a.query_index);
                    }

                    postings_lists[a.postings_list].rewrite_with(SetBuf::new_unchecked(newa));
                    postings_lists[b.postings_list].rewrite_with(SetBuf::new_unchecked(newb));
                },
                Some((1, _)) => {
                    if previous_word.take() != Some(a.query_index - 1) {
                        postings_lists[a.postings_list].rewrite_with(SetBuf::default());
                    }
                },
                Some((_, _)) => unreachable!(),
                None => (),
            }
        }

        if raw_matches.iter().all(|rm| postings_lists[rm.postings_list].is_empty()) {
            return None
        }

        Some(RawDocument {
            raw_matches,
            processed_matches: Vec::new(),
            processed_distances: Vec::new(),
        })
    }
}

pub struct BareMatch<'tag> {
    pub document_id: DocumentId,
    pub query_index: u16,
    pub distance: u8,
    pub is_exact: bool,
    pub postings_list: Idx32<'tag>,
}

impl fmt::Debug for BareMatch<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BareMatch")
            .field("document_id", &self.document_id)
            .field("query_index", &self.query_index)
            .field("distance", &self.distance)
            .field("is_exact", &self.is_exact)
            .finish()
    }
}

// TODO remove that
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimpleMatch {
    pub query_index: u16,
    pub distance: u8,
    pub attribute: u16,
    pub word_index: u16,
    pub is_exact: bool,
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

fn fetch_matches<'txn, 'tag>(
    reader: &'txn heed::RoTxn<MainT>,
    automatons: &[QueryWordAutomaton],
    arena: &mut SmallArena<'tag, PostingsListView<'txn>>,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
) -> MResult<Vec<BareMatch<'tag>>>
{
    let mut before_words_fst = Instant::now();
    let words = match main_store.words_fst(reader)? {
        Some(words) => words,
        None => return Ok(Vec::new()),
    };
    debug!("words fst took {:.02?}", before_words_fst.elapsed());

    let mut total_postings_lists = Vec::new();

    let mut dfa_time = Duration::default();
    let mut stream_next_time = Duration::default();
    let mut postings_lists_fetching_time = Duration::default();

    for (query_index, automaton) in automatons.iter().enumerate() {
        let before_dfa = Instant::now();
        let dfa = automaton.dfa();
        let QueryWordAutomaton { query, is_exact, is_prefix, phrase_query } = automaton;
        dfa_time += before_dfa.elapsed();

        let mut number_of_words = 0;
        let mut stream = words.search(&dfa).into_stream();

        // while let Some(input) = stream.next() {
        loop {
            let before_stream_next = Instant::now();
            let input = match stream.next() {
                Some(input) => input,
                None => break,
            };
            stream_next_time += before_stream_next.elapsed();

            number_of_words += 1;

            let distance = dfa.eval(input).to_u8();
            let is_exact = *is_exact && distance == 0 && input.len() == query.len();

            let before_postings_lists_fetching = Instant::now();
            if let Some(postings_list) = postings_lists_store.postings_list(reader, input)? {

                let input = Rc::from(input);
                let postings_list = Rc::new(postings_list);
                let postings_list_view = PostingsListView::original(input, postings_list);

                let mut offset = 0;
                for group in postings_list_view.linear_group_by_key(|di| di.document_id) {

                    let posting_list_index = arena.add(postings_list_view.range(offset, group.len()));
                    let document_id = group[0].document_id;
                    let bare_match = BareMatch {
                        document_id,
                        query_index: query_index as u16,
                        distance,
                        is_exact,
                        postings_list: posting_list_index,
                    };

                    total_postings_lists.push(bare_match);
                    offset += group.len();
                }
            }
            postings_lists_fetching_time += before_postings_lists_fetching.elapsed();
        }

        debug!("{:?} gives {} words", query, number_of_words);
    }

    debug!("stream next took {:.02?}", stream_next_time);
    debug!("postings lists fetching took {:.02?}", postings_lists_fetching_time);
    debug!("dfa creation took {:.02?}", dfa_time);

    Ok(total_postings_lists)
}

#[derive(Debug)]
pub struct QueryWordAutomaton {
    pub query: String,
    /// Is it a word that must be considered exact
    /// or is it some derived word (i.e. a synonym)
    pub is_exact: bool,
    pub is_prefix: bool,
    /// If it's a phrase query and what is
    /// its index an the length of the phrase
    pub phrase_query: Option<(u16, u16)>,
}

impl QueryWordAutomaton {
    pub fn exact(query: &str) -> QueryWordAutomaton {
        QueryWordAutomaton {
            query: query.to_string(),
            is_exact: true,
            is_prefix: false,
            phrase_query: None,
        }
    }

    pub fn exact_prefix(query: &str) -> QueryWordAutomaton {
        QueryWordAutomaton {
            query: query.to_string(),
            is_exact: true,
            is_prefix: true,
            phrase_query: None,
        }
    }

    pub fn non_exact(query: &str) -> QueryWordAutomaton {
        QueryWordAutomaton {
            query: query.to_string(),
            is_exact: false,
            is_prefix: false,
            phrase_query: None,
        }
    }

    pub fn dfa(&self) -> DFA {
        if self.phrase_query.is_some() {
            build_exact_dfa(&self.query)
        } else if self.is_prefix {
            build_prefix_dfa(&self.query)
        } else {
            build_dfa(&self.query)
        }
    }
}

fn construct_automatons2(
    reader: &heed::RoTxn<MainT>,
    query: &str,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    synonym_store: store::Synonyms,
) -> MResult<(Vec<QueryWordAutomaton>, QueryEnhancer)> {
    let has_end_whitespace = query.chars().last().map_or(false, char::is_whitespace);
    let query_words: Vec<_> = split_query_string(query).map(str::to_lowercase).collect();
    let synonyms = match main_store.synonyms_fst(reader)? {
        Some(synonym) => synonym,
        None => fst::Set::default(),
    };

    let mut automaton_index = 0;
    let mut automatons = Vec::new();
    let mut enhancer_builder = QueryEnhancerBuilder::new(&query_words);

    // We must not declare the original words to the query enhancer
    // *but* we need to push them in the automatons list first
    let mut original_words = query_words.iter().peekable();
    while let Some(word) = original_words.next() {
        let has_following_word = original_words.peek().is_some();
        let not_prefix_dfa = has_following_word || has_end_whitespace || word.chars().all(is_cjk);

        let automaton = if not_prefix_dfa {
            QueryWordAutomaton::exact(word)
        } else {
            QueryWordAutomaton::exact_prefix(word)
        };
        automaton_index += 1;
        automatons.push(automaton);
    }

    for n in 1..=NGRAMS {
        let mut ngrams = query_words.windows(n).enumerate().peekable();
        while let Some((query_index, ngram_slice)) = ngrams.next() {
            let query_range = query_index..query_index + n;
            let ngram_nb_words = ngram_slice.len();
            let ngram = ngram_slice.join(" ");

            let has_following_word = ngrams.peek().is_some();
            let not_prefix_dfa =
                has_following_word || has_end_whitespace || ngram.chars().all(is_cjk);

            // automaton of synonyms of the ngrams
            let normalized = normalize_str(&ngram);
            let lev = if not_prefix_dfa {
                build_dfa(&normalized)
            } else {
                build_prefix_dfa(&normalized)
            };

            let mut stream = synonyms.search(&lev).into_stream();
            while let Some(base) = stream.next() {
                // only trigger alternatives when the last word has been typed
                // i.e. "new " do not but "new yo" triggers alternatives to "new york"
                let base = std::str::from_utf8(base).unwrap();
                let base_nb_words = split_query_string(base).count();
                if ngram_nb_words != base_nb_words {
                    continue;
                }

                if let Some(synonyms) = synonym_store.synonyms(reader, base.as_bytes())? {
                    let mut stream = synonyms.into_stream();
                    while let Some(synonyms) = stream.next() {
                        let synonyms = std::str::from_utf8(synonyms).unwrap();
                        let synonyms_words: Vec<_> = split_query_string(synonyms).collect();
                        let nb_synonym_words = synonyms_words.len();

                        let real_query_index = automaton_index;
                        enhancer_builder.declare(query_range.clone(), real_query_index, &synonyms_words);

                        for synonym in synonyms_words {
                            let automaton = if nb_synonym_words == 1 {
                                QueryWordAutomaton::exact(synonym)
                            } else {
                                QueryWordAutomaton::non_exact(synonym)
                            };
                            automaton_index += 1;
                            automatons.push(automaton);
                        }
                    }
                }
            }

            if n == 1 {
                // automatons for splitted words
                if let Some((left, right)) = split_best_frequency(reader, &normalized, postings_lists_store)? {
                    let mut left_automaton = QueryWordAutomaton::exact(left);
                    left_automaton.phrase_query = Some((0, 2));
                    enhancer_builder.declare(query_range.clone(), automaton_index, &[left]);
                    automaton_index += 1;
                    automatons.push(left_automaton);

                    let mut right_automaton = QueryWordAutomaton::exact(right);
                    right_automaton.phrase_query = Some((1, 2));
                    enhancer_builder.declare(query_range.clone(), automaton_index, &[right]);
                    automaton_index += 1;
                    automatons.push(right_automaton);
                }
            } else {
                // automaton of concatenation of query words
                let concat = ngram_slice.concat();
                let normalized = normalize_str(&concat);

                let real_query_index = automaton_index;
                enhancer_builder.declare(query_range.clone(), real_query_index, &[&normalized]);

                let automaton = QueryWordAutomaton::exact(&normalized);
                automaton_index += 1;
                automatons.push(automaton);
            }
        }
    }

    Ok((automatons, enhancer_builder.build()))
}
