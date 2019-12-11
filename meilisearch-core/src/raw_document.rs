use compact_arena::SmallArena;
use itertools::EitherOrBoth;
use sdset::SetBuf;

use crate::bucket_sort::{SimpleMatch, BareMatch, QueryWordAutomaton, PostingsListView};

pub struct RawDocument<'a, 'tag> {
    pub id: crate::DocumentId,
    pub raw_matches: &'a mut [BareMatch<'tag>],
    pub processed_matches: Vec<SimpleMatch>,
    /// The list of minimum `distance` found
    pub processed_distances: Vec<Option<u8>>,
}

impl<'a, 'tag> RawDocument<'a, 'tag> {
    pub fn new<'txn>(
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

                    let iter = itertools::merge_join_by(pla.iter(), plb.iter(), |a, b| {
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
            id: raw_matches[0].document_id,
            raw_matches,
            processed_matches: Vec::new(),
            processed_distances: Vec::new(),
        })
    }
}
