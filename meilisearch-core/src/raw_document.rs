use compact_arena::SmallArena;
use itertools::EitherOrBoth;
use sdset::SetBuf;
use crate::DocIndex;
use crate::bucket_sort::{SimpleMatch, BareMatch, QueryWordAutomaton, PostingsListView};
use crate::reordered_attrs::ReorderedAttrs;

pub struct RawDocument<'a, 'tag> {
    pub id: crate::DocumentId,
    pub bare_matches: &'a mut [BareMatch<'tag>],
    pub processed_matches: Vec<SimpleMatch>,
    /// The list of minimum `distance` found
    pub processed_distances: Vec<Option<u8>>,
    /// Does this document contains a field
    /// with one word that is exactly matching
    pub contains_one_word_field: bool,
}

impl<'a, 'tag> RawDocument<'a, 'tag> {
    pub fn new<'txn>(
        bare_matches: &'a mut [BareMatch<'tag>],
        automatons: &[QueryWordAutomaton],
        postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
        searchable_attrs: Option<&ReorderedAttrs>,
    ) -> Option<RawDocument<'a, 'tag>>
    {
        if let Some(reordered_attrs) = searchable_attrs {
            for bm in bare_matches.iter() {
                let postings_list = &postings_lists[bm.postings_list];

                let mut rewritten = Vec::new();
                for di in postings_list.iter() {
                    if let Some(attribute) = reordered_attrs.get(di.attribute) {
                        rewritten.push(DocIndex { attribute, ..*di });
                    }
                }

                let new_postings = SetBuf::from_dirty(rewritten);
                postings_lists[bm.postings_list].rewrite_with(new_postings);
            }
        }

        bare_matches.sort_unstable_by_key(|m| m.query_index);

        let mut previous_word = None;
        for i in 0..bare_matches.len() {
            let a = &bare_matches[i];
            let auta = &automatons[a.query_index as usize];

            match auta.phrase_query {
                Some((0, _)) => {
                    let b = match bare_matches.get(i + 1) {
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

        if bare_matches.iter().all(|rm| postings_lists[rm.postings_list].is_empty()) {
            return None
        }

        Some(RawDocument {
            id: bare_matches[0].document_id,
            bare_matches,
            processed_matches: Vec::new(),
            processed_distances: Vec::new(),
            contains_one_word_field: false,
        })
    }
}
