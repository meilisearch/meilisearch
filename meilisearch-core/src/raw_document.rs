use compact_arena::SmallArena;
use sdset::SetBuf;
use crate::DocIndex;
use crate::bucket_sort::{SimpleMatch, BareMatch, PostingsListView};
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
        postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
        searchable_attrs: Option<&ReorderedAttrs>,
    ) -> RawDocument<'a, 'tag>
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

        RawDocument {
            id: bare_matches[0].document_id,
            bare_matches,
            processed_matches: Vec::new(),
            processed_distances: Vec::new(),
            contains_one_word_field: false,
        }
    }
}
