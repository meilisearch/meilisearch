use std::collections::VecDeque;
use std::rc::Rc;

use heed::RoTxn;
use itertools::merge_join_by;
use obkv::KvReader;

use super::tokenize_document::DocumentTokenizer;
use super::SearchableExtractor;
use crate::proximity::{index_proximity, MAX_DISTANCE};
use crate::update::new::extract::cache::CboCachedSorter;
use crate::update::new::DocumentChange;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{FieldId, GlobalFieldsIdsMap, Index, Result};

pub struct WordPairProximityDocidsExtractor;
impl SearchableExtractor for WordPairProximityDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
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
        let mut key_buffer = Vec::new();
        let mut del_word_pair_proximity = Vec::new();
        let mut add_word_pair_proximity = Vec::new();
        let mut word_positions: VecDeque<(Rc<str>, u16)> =
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
                    &mut |(w1, w2), prox| {
                        del_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
            }
            DocumentChange::Update(inner) => {
                let document = inner.current(rtxn, index)?.unwrap();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        del_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
                let document = inner.new();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        add_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
            }
            DocumentChange::Insertion(inner) => {
                let document = inner.new();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        add_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
            }
        }

        del_word_pair_proximity.sort_unstable();
        del_word_pair_proximity.dedup_by(|(k1, _), (k2, _)| k1 == k2);
        for ((w1, w2), prox) in del_word_pair_proximity.iter() {
            let key = build_key(*prox, w1, w2, &mut key_buffer);
            cached_sorter.insert_del_u32(key, docid)?;
        }

        add_word_pair_proximity.sort_unstable();
        add_word_pair_proximity.dedup_by(|(k1, _), (k2, _)| k1 == k2);
        for ((w1, w2), prox) in add_word_pair_proximity.iter() {
            let key = build_key(*prox, w1, w2, &mut key_buffer);
            cached_sorter.insert_add_u32(key, docid)?;
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
    word_positions: &mut VecDeque<(Rc<str>, u16)>,
    word_pair_proximity: &mut impl FnMut((Rc<str>, Rc<str>), u8),
) -> Result<()> {
    let (head_word, head_position) = word_positions.pop_front().unwrap();
    for (word, position) in word_positions.iter() {
        let prox = index_proximity(head_position as u32, *position as u32) as u8;
        if prox > 0 && prox < MAX_DISTANCE as u8 {
            word_pair_proximity((head_word.clone(), word.clone()), prox);
        }
    }
    Ok(())
}

fn process_document_tokens(
    document: &KvReader<FieldId>,
    document_tokenizer: &DocumentTokenizer,
    fields_ids_map: &mut GlobalFieldsIdsMap,
    word_positions: &mut VecDeque<(Rc<str>, u16)>,
    word_pair_proximity: &mut impl FnMut((Rc<str>, Rc<str>), u8),
) -> Result<()> {
    let mut token_fn = |_fname: &str, _fid: FieldId, pos: u16, word: &str| {
        // drain the proximity window until the head word is considered close to the word we are inserting.
        while word_positions
            .front()
            .map_or(false, |(_w, p)| index_proximity(*p as u32, pos as u32) >= MAX_DISTANCE)
        {
            word_positions_into_word_pair_proximity(word_positions, word_pair_proximity)?;
        }

        // insert the new word.
        word_positions.push_back((Rc::from(word), pos));
        Ok(())
    };
    document_tokenizer.tokenize_document(document, fields_ids_map, &mut token_fn)?;

    while !word_positions.is_empty() {
        word_positions_into_word_pair_proximity(word_positions, word_pair_proximity)?;
    }

    Ok(())
}
