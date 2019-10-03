use std::time::{Instant, Duration};
use std::ops::Range;
use std::mem;

use fst::{IntoStreamer, Streamer};
use sdset::SetBuf;
use slice_group_by::{GroupBy, GroupByMut};

use crate::automaton::{Automaton, AutomatonProducer, QueryEnhancer};
use crate::raw_document::{RawDocument, raw_documents_from};
use crate::{Document, DocumentId, Highlight, TmpMatch, criterion::Criteria};
use crate::{store, reordered_attrs::ReorderedAttrs};

pub struct QueryBuilder<'a> {
    criteria: Criteria<'a>,
    searchables_attrs: Option<ReorderedAttrs>,
    timeout: Duration,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    synonyms_store: store::Synonyms,
}

fn multiword_rewrite_matches(
    mut matches: Vec<(DocumentId, TmpMatch)>,
    query_enhancer: &QueryEnhancer,
) -> SetBuf<(DocumentId, TmpMatch)>
{
    let mut padded_matches = Vec::with_capacity(matches.len());

    // we sort the matches by word index to make them rewritable
    matches.sort_unstable_by_key(|(id, match_)| (*id, match_.attribute, match_.word_index));

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

    for document_matches in padded_matches.linear_group_by_key_mut(|(id, _)| *id) {
        document_matches.sort_unstable();
    }

    SetBuf::new_unchecked(padded_matches)
}

fn fetch_raw_documents(
    reader: &rkv::Reader,
    automatons: &[Automaton],
    query_enhancer: &QueryEnhancer,
    searchables: Option<&ReorderedAttrs>,
    main_store: &store::Main,
    postings_lists_store: &store::PostingsLists,
) -> Result<Vec<RawDocument>, rkv::StoreError>
{
    let mut matches = Vec::new();
    let mut highlights = Vec::new();

    for automaton in automatons {
        let Automaton { index, is_exact, query_len, .. } = automaton;
        let dfa = automaton.dfa();

        let words = match main_store.words_fst(reader)? {
            Some(words) => words,
            None => return Ok(Vec::new()),
        };

        let mut stream = words.search(&dfa).into_stream();
        while let Some(input) = stream.next() {
            let distance = dfa.eval(input).to_u8();
            let is_exact = *is_exact && distance == 0 && input.len() == *query_len;

            let doc_indexes = match postings_lists_store.postings_list(reader, input)? {
                Some(doc_indexes) => doc_indexes,
                None => continue,
            };

            matches.reserve(doc_indexes.len());
            highlights.reserve(doc_indexes.len());

            for di in doc_indexes.as_ref() {
                let attribute = searchables.map_or(Some(di.attribute), |r| r.get(di.attribute));
                if let Some(attribute) = attribute {
                    let match_ = TmpMatch {
                        query_index: *index as u32,
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

    let matches = multiword_rewrite_matches(matches, &query_enhancer);
    let highlights = {
        highlights.sort_unstable_by_key(|(id, _)| *id);
        SetBuf::new_unchecked(highlights)
    };

    Ok(raw_documents_from(matches, highlights))
}

impl<'a> QueryBuilder<'a> {
    pub fn new(
        main: store::Main,
        postings_lists: store::PostingsLists,
        synonyms: store::Synonyms,
    ) -> QueryBuilder<'a> {
        QueryBuilder {
            criteria: Criteria::default(),
            searchables_attrs: None,
            timeout: Duration::from_secs(1),
            main_store: main,
            postings_lists_store: postings_lists,
            synonyms_store: synonyms,
        }
    }

    pub fn query(
        self,
        reader: &rkv::Reader,
        query: &str,
        range: Range<usize>,
    ) -> Result<Vec<Document>, rkv::StoreError>
    {
        let start_processing = Instant::now();
        let mut raw_documents_processed = Vec::new();

        let (automaton_producer, query_enhancer) = AutomatonProducer::new(reader, query, self.synonyms_store);
        let mut automaton_producer = automaton_producer.into_iter();
        let mut automatons = Vec::new();

        // aggregate automatons groups by groups after time
        while let Some(auts) = automaton_producer.next() {
            automatons.extend(auts);

            // we must retrieve the documents associated
            // with the current automatons
            let mut raw_documents = fetch_raw_documents(
                reader,
                &automatons,
                &query_enhancer,
                self.searchables_attrs.as_ref(),
                &self.main_store,
                &self.postings_lists_store,
            )?;

            let mut groups = vec![raw_documents.as_mut_slice()];

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

                    group.sort_unstable_by(|a, b| criterion.evaluate(a, b));

                    for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b)) {
                        documents_seen += group.len();
                        groups.push(group);

                        // we have sort enough documents if the last document sorted is after
                        // the end of the requested range, we can continue to the next criterion
                        if documents_seen >= range.end { continue 'criteria }
                    }
                }
            }

            // once we classified the documents related to the current
            // automatons we save that as the next valid result
            let iter = raw_documents.into_iter().skip(range.start).take(range.len());
            raw_documents_processed.clear();
            raw_documents_processed.extend(iter);

            // stop processing after there is no time
            if start_processing.elapsed() > self.timeout { break }
        }

        // make real documents now that we know
        // those must be returned
        let documents = raw_documents_processed
            .into_iter()
            .map(|d| Document::from_raw(d))
            .collect();

        Ok(documents)
    }
}
