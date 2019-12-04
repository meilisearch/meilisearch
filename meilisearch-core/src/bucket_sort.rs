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
use sdset::Set;
use slice_group_by::{GroupBy, GroupByMut};

use crate::automaton::{build_dfa, build_prefix_dfa};
use crate::{database::MainT, reordered_attrs::ReorderedAttrs};
use crate::{store, Document, DocumentId, MResult};
use crate::criterion2::*;

pub fn bucket_sort<'c>(
    reader: &heed::RoTxn<MainT>,
    query: &str,
    range: Range<usize>,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
) -> MResult<Vec<Document>>
{
    let automatons = construct_automatons(query);

    let before_postings_lists_fetching = Instant::now();
    mk_arena!(arena);
    let mut bare_matches = fetch_matches(reader, automatons, &mut arena, main_store, postings_lists_store)?;
    debug!("bare matches ({}) retrieved in {:.02?}",
        bare_matches.len(),
        before_postings_lists_fetching.elapsed(),
    );

    let before_raw_documents_presort = Instant::now();
    bare_matches.sort_unstable_by_key(|sm| sm.document_id);
    debug!("sort by documents ids took {:.02?}", before_raw_documents_presort.elapsed());

    dbg!(mem::size_of::<BareMatch>());

    let before_raw_documents_building = Instant::now();
    let mut raw_documents = Vec::new();
    for matches in bare_matches.linear_group_by_key_mut(|sm| sm.document_id) {
        raw_documents.push(RawDocument { matches });
    }
    debug!("creating {} candidates documents took {:.02?}",
        raw_documents.len(),
        before_raw_documents_building.elapsed(),
    );

    let mut groups = vec![raw_documents.as_mut_slice()];

    let criteria = [
        Box::new(Typo) as Box<dyn Criterion>,
        Box::new(Words),
        // Box::new(Proximity),
        Box::new(Attribute),
        Box::new(WordsPosition),
        Box::new(Exact),
    ];

    'criteria: for criterion in &criteria {
        let tmp_groups = mem::replace(&mut groups, Vec::new());
        let mut documents_seen = 0;

        for mut group in tmp_groups {
            let before_criterion_preparation = Instant::now();
            criterion.prepare(&mut group, &mut arena);
            debug!("preparing postings lists for {:?} took {:.02?}", criterion.name(), before_criterion_preparation.elapsed());

            let before_criterion_sort = Instant::now();
            group.sort_unstable_by(|a, b| criterion.evaluate(a, b, &arena));
            debug!("evaluation of {:?} took {:.02?}", criterion.name(), before_criterion_sort.elapsed());

            for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b, &arena)) {
                debug!("criterion {} produced a group of size {}", criterion.name(), group.len());

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

    println!("called 'attributes' {}x times",
        crate::criterion2::ATTRIBUTE_CALLED_NUMBER.load(std::sync::atomic::Ordering::Relaxed),
    );

    let iter = raw_documents.into_iter().skip(range.start).take(range.len());
    let iter = iter.map(|d| {
        let highlights = d.matches.iter().flat_map(|sm| {
            let postings_list = &arena[sm.postings_list];
            postings_list.iter().filter(|m| m.document_id == d.matches[0].document_id).map(|m| {
                Highlight { attribute: m.attribute, char_index: m.char_index, char_length: m.char_length }
            })
        }).collect();

        // let highlights = Default::default();

        Document {
            id: d.matches[0].document_id,
            highlights,
            #[cfg(test)] matches: Vec::new(),
        }
    });

    Ok(iter.collect())
}

pub struct RawDocument<'a, 'tag> {
    pub matches: &'a mut [BareMatch<'tag>],
}

pub struct BareMatch<'tag> {
    pub document_id: DocumentId,
    pub query_index: u16,
    pub distance: u8,
    pub is_exact: bool,
    pub postings_list: Idx32<'tag>,
}

pub struct RangedPostingsList<'a> {
    postings_list: Rc<Cow<'a, Set<DocIndex>>>,
    range: Range<usize>,
}

impl AsRef<[DocIndex]> for RangedPostingsList<'_> {
    fn as_ref(&self) -> &[DocIndex] {
        let range = self.range.clone();
        &self.postings_list[range]
    }
}

fn fetch_matches<'txn, 'tag>(
    reader: &'txn heed::RoTxn<MainT>,
    automatons: Vec<QueryWordAutomaton>,
    arena: &mut SmallArena<'tag, Cow<'txn, Set<DocIndex>>>,
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

    for (query_index, automaton) in automatons.into_iter().enumerate() {
        let before_dfa = Instant::now();
        let dfa = automaton.dfa();
        let QueryWordAutomaton { query, is_exact, is_prefix } = automaton;
        dfa_time += before_dfa.elapsed();

        let mut number_of_words = 0;

        let before_fst_search = Instant::now();
        let mut stream = words.search(&dfa).into_stream();
        debug!("fst search took {:.02?}", before_fst_search.elapsed());

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
            let is_exact = is_exact && distance == 0 && input.len() == query.len();

            let before_postings_lists_fetching = Instant::now();
            if let Some(postings_list) = postings_lists_store.postings_list(reader, input)? {

                let posting_list_index = arena.add(postings_list);
                for group in arena[posting_list_index].linear_group_by_key(|di| di.document_id) {
                    let document_id = group[0].document_id;
                    let stuffed = BareMatch {
                        document_id,
                        query_index: query_index as u16,
                        distance,
                        is_exact,
                        postings_list: posting_list_index,
                    };

                    total_postings_lists.push(stuffed);
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
    query: String,
    /// Is it a word that must be considered exact
    /// or is it some derived word (i.e. a synonym)
    is_exact: bool,
    is_prefix: bool,
}

impl QueryWordAutomaton {
    pub fn exact(query: String) -> QueryWordAutomaton {
        QueryWordAutomaton { query, is_exact: true, is_prefix: false }
    }

    pub fn exact_prefix(query: String) -> QueryWordAutomaton {
        QueryWordAutomaton { query, is_exact: true, is_prefix: true }
    }

    pub fn dfa(&self) -> DFA {
        if self.is_prefix {
            build_prefix_dfa(&self.query)
        } else {
            build_dfa(&self.query)
        }
    }
}

fn construct_automatons(query: &str) -> Vec<QueryWordAutomaton> {
    let has_end_whitespace = query.chars().last().map_or(false, char::is_whitespace);
    let mut original_words = split_query_string(query).map(str::to_lowercase).peekable();
    let mut automatons = Vec::new();

    while let Some(word) = original_words.next() {
        let has_following_word = original_words.peek().is_some();
        let not_prefix_dfa = has_following_word || has_end_whitespace || word.chars().all(is_cjk);

        let automaton = if not_prefix_dfa {
            QueryWordAutomaton::exact(word)
        } else {
            QueryWordAutomaton::exact_prefix(word)
        };

        automatons.push(automaton);
    }

    automatons
}
