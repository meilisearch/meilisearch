use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
};

use heed::RoTxn;
use itertools::merge_join_by;
use obkv::KvReader;

use super::{tokenize_document::DocumentTokenizer, SearchableExtractor};
use crate::{
    proximity::{index_proximity, MAX_DISTANCE},
    update::{
        new::{extract::cache::CboCachedSorter, DocumentChange},
        MergeDeladdCboRoaringBitmaps,
    },
    FieldId, GlobalFieldsIdsMap, Index, Result,
};

pub struct WordPairProximityDocidsExtractor;
impl SearchableExtractor for WordPairProximityDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
    }

    /// This case is unreachable because extract_document_change has been reimplemented to not call this function.
    fn build_key<'a>(_field_id: FieldId, _position: u16, _word: &'a str) -> Cow<'a, [u8]> {
        unreachable!()
    }

    // This method is reimplemented to count the number of words in the document in each field
    // and to store the docids of the documents that have a number of words in a given field equal to or under than MAX_COUNTED_WORDS.
    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        document_tokenizer: &DocumentTokenizer,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        cached_sorter: &mut CboCachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()> {
        /// TODO: mutualize those buffers
        let mut key_buffer = Vec::new();
        let mut add_word_pair_proximity = BTreeMap::new();
        let mut del_word_pair_proximity = BTreeMap::new();
        let mut word_positions: VecDeque<(String, u16)> =
            VecDeque::with_capacity(MAX_DISTANCE as usize);

        let docid = document_change.docid();
        match document_change {
            DocumentChange::Deletion(inner) => {
                let document = inner.current(rtxn, index)?.unwrap();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut del_word_pair_proximity,
                )?;
            }
            DocumentChange::Update(inner) => {
                let document = inner.current(rtxn, index)?.unwrap();
                process_document_tokens(
                    &document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut del_word_pair_proximity,
                )?;
                let document = inner.new();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut add_word_pair_proximity,
                )?;
            }
            DocumentChange::Insertion(inner) => {
                let document = inner.new();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut add_word_pair_proximity,
                )?;
            }
        }

        use itertools::EitherOrBoth::*;
        for eob in
            merge_join_by(del_word_pair_proximity.iter(), add_word_pair_proximity.iter(), |d, a| {
                d.cmp(a)
            })
        {
            match eob {
                Left(((w1, w2), prox)) => {
                    let key = build_key(*prox, w1, w2, &mut key_buffer);
                    cached_sorter.insert_del_u32(key, docid).unwrap();
                }
                Right(((w1, w2), prox)) => {
                    let key = build_key(*prox, w1, w2, &mut key_buffer);
                    cached_sorter.insert_add_u32(key, docid).unwrap();
                }
                Both(((w1, w2), del_prox), (_, add_prox)) => {
                    if del_prox != add_prox {
                        let key = build_key(*del_prox, w1, w2, &mut key_buffer);
                        cached_sorter.insert_del_u32(key, docid).unwrap();
                        let key = build_key(*add_prox, w1, w2, &mut key_buffer);
                        cached_sorter.insert_add_u32(key, docid).unwrap();
                    }
                }
            };
        }

        Ok(())
    }
}

fn build_key<'a>(prox: u8, w1: &str, w2: &str, key_buffer: &'a mut Vec<u8>) -> &'a [u8] {
    key_buffer.clear();
    key_buffer.push(prox);
    key_buffer.extend_from_slice(w1.as_bytes());
    key_buffer.push(0);
    key_buffer.extend_from_slice(w2.as_bytes());
    key_buffer.as_slice()
}

fn word_positions_into_word_pair_proximity(
    word_positions: &mut VecDeque<(String, u16)>,
    word_pair_proximity: &mut BTreeMap<(String, String), u8>,
) -> Result<()> {
    let (head_word, head_position) = word_positions.pop_front().unwrap();
    for (word, position) in word_positions.iter() {
        let prox = index_proximity(head_position as u32, *position as u32) as u8;
        if prox > 0 && prox < MAX_DISTANCE as u8 {
            word_pair_proximity
                .entry((head_word.clone(), word.clone()))
                .and_modify(|p| {
                    *p = std::cmp::min(*p, prox);
                })
                .or_insert(prox);
        }
    }
    Ok(())
}

fn process_document_tokens(
    document: &KvReader<FieldId>,
    document_tokenizer: &DocumentTokenizer,
    fields_ids_map: &mut GlobalFieldsIdsMap,
    word_positions: &mut VecDeque<(String, u16)>,
    word_pair_proximity: &mut BTreeMap<(String, String), u8>,
) -> Result<()> {
    let mut token_fn = |fid: FieldId, pos: u16, word: &str| {
        // drain the proximity window until the head word is considered close to the word we are inserting.
        while word_positions
            .front()
            .map_or(false, |(_w, p)| index_proximity(*p as u32, pos as u32) >= MAX_DISTANCE)
        {
            word_positions_into_word_pair_proximity(word_positions, word_pair_proximity)?;
        }

        // insert the new word.
        word_positions.push_back((word.to_string(), pos));
        Ok(())
    };
    document_tokenizer.tokenize_document(document, fields_ids_map, &mut token_fn)?;

    while !word_positions.is_empty() {
        word_positions_into_word_pair_proximity(word_positions, word_pair_proximity)?;
    }

    Ok(())
}
