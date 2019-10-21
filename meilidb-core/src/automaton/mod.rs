mod dfa;
mod query_enhancer;

use std::cmp::Reverse;
use std::vec;

use fst::{IntoStreamer, Streamer};
use levenshtein_automata::DFA;
use meilidb_tokenizer::{is_cjk, split_query_string};

use crate::error::MResult;
use crate::store;

use self::dfa::{build_dfa, build_prefix_dfa};
pub use self::query_enhancer::QueryEnhancer;
use self::query_enhancer::QueryEnhancerBuilder;

const NGRAMS: usize = 3;

pub struct AutomatonProducer {
    automatons: Vec<Vec<Automaton>>,
}

impl AutomatonProducer {
    pub fn new(
        reader: &heed::RoTxn,
        query: &str,
        main_store: store::Main,
        synonyms_store: store::Synonyms,
    ) -> MResult<(AutomatonProducer, QueryEnhancer)> {
        let (automatons, query_enhancer) =
            generate_automatons(reader, query, main_store, synonyms_store)?;

        Ok((AutomatonProducer { automatons }, query_enhancer))
    }

    pub fn into_iter(self) -> vec::IntoIter<Vec<Automaton>> {
        self.automatons.into_iter()
    }
}

#[derive(Debug)]
pub struct Automaton {
    pub index: usize,
    pub ngram: usize,
    pub query_len: usize,
    pub is_exact: bool,
    pub is_prefix: bool,
    pub query: String,
}

impl Automaton {
    pub fn dfa(&self) -> DFA {
        if self.is_prefix {
            build_prefix_dfa(&self.query)
        } else {
            build_dfa(&self.query)
        }
    }

    fn exact(index: usize, ngram: usize, query: &str) -> Automaton {
        Automaton {
            index,
            ngram,
            query_len: query.len(),
            is_exact: true,
            is_prefix: false,
            query: query.to_string(),
        }
    }

    fn prefix_exact(index: usize, ngram: usize, query: &str) -> Automaton {
        Automaton {
            index,
            ngram,
            query_len: query.len(),
            is_exact: true,
            is_prefix: true,
            query: query.to_string(),
        }
    }

    fn non_exact(index: usize, ngram: usize, query: &str) -> Automaton {
        Automaton {
            index,
            ngram,
            query_len: query.len(),
            is_exact: false,
            is_prefix: false,
            query: query.to_string(),
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

fn generate_automatons(
    reader: &heed::RoTxn,
    query: &str,
    main_store: store::Main,
    synonym_store: store::Synonyms,
) -> MResult<(Vec<Vec<Automaton>>, QueryEnhancer)> {
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
    let mut original_automatons = Vec::new();
    let mut original_words = query_words.iter().peekable();
    while let Some(word) = original_words.next() {
        let has_following_word = original_words.peek().is_some();
        let not_prefix_dfa = has_following_word || has_end_whitespace || word.chars().all(is_cjk);

        let automaton = if not_prefix_dfa {
            Automaton::exact(automaton_index, 1, word)
        } else {
            Automaton::prefix_exact(automaton_index, 1, word)
        };
        automaton_index += 1;
        original_automatons.push(automaton);
    }

    automatons.push(original_automatons);

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
                        enhancer_builder.declare(
                            query_range.clone(),
                            real_query_index,
                            &synonyms_words,
                        );

                        for synonym in synonyms_words {
                            let automaton = if nb_synonym_words == 1 {
                                Automaton::exact(automaton_index, n, synonym)
                            } else {
                                Automaton::non_exact(automaton_index, n, synonym)
                            };
                            automaton_index += 1;
                            automatons.push(vec![automaton]);
                        }
                    }
                }
            }

            if n != 1 {
                // automaton of concatenation of query words
                let concat = ngram_slice.concat();
                let normalized = normalize_str(&concat);

                let real_query_index = automaton_index;
                enhancer_builder.declare(query_range.clone(), real_query_index, &[&normalized]);

                let automaton = Automaton::exact(automaton_index, n, &normalized);
                automaton_index += 1;
                automatons.push(vec![automaton]);
            }
        }
    }

    // order automatons, the most important first,
    // we keep the original automatons at the front.
    automatons[1..].sort_by_key(|a| {
        let a = a.first().unwrap();
        (Reverse(a.is_exact), a.ngram)
    });

    Ok((automatons, enhancer_builder.build()))
}
