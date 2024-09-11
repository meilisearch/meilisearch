use std::collections::HashMap;

use heed::RoTxn;

use super::tokenize_document::DocumentTokenizer;
use super::SearchableExtractor;
use crate::update::new::extract::cache::CboCachedSorter;
use crate::update::new::DocumentChange;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{FieldId, GlobalFieldsIdsMap, Index, Result};

const MAX_COUNTED_WORDS: usize = 30;

pub struct FidWordCountDocidsExtractor;
impl SearchableExtractor for FidWordCountDocidsExtractor {
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
        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut fid_word_count = HashMap::new();
                let mut token_fn = |_fname: &str, fid: FieldId, _pos: u16, _word: &str| {
                    fid_word_count.entry(fid).and_modify(|count| *count += 1).or_insert(1);
                    Ok(())
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut token_fn,
                )?;

                // The docids of the documents that have a number of words in a given field equal to or under than MAX_COUNTED_WORDS are deleted.
                for (fid, count) in fid_word_count.iter() {
                    if *count <= MAX_COUNTED_WORDS {
                        let key = build_key(*fid, *count as u8, &mut key_buffer);
                        cached_sorter.insert_del_u32(key, inner.docid())?;
                    }
                }
            }
            DocumentChange::Update(inner) => {
                let mut fid_word_count = HashMap::new();
                let mut token_fn = |_fname: &str, fid: FieldId, _pos: u16, _word: &str| {
                    fid_word_count
                        .entry(fid)
                        .and_modify(|(current_count, _new_count)| *current_count += 1)
                        .or_insert((1, 0));
                    Ok(())
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut token_fn,
                )?;

                let mut token_fn = |_fname: &str, fid: FieldId, _pos: u16, _word: &str| {
                    fid_word_count
                        .entry(fid)
                        .and_modify(|(_current_count, new_count)| *new_count += 1)
                        .or_insert((0, 1));
                    Ok(())
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;

                // Only the fields that have a change in the number of words are updated.
                for (fid, (current_count, new_count)) in fid_word_count.iter() {
                    if *current_count != *new_count {
                        if *current_count <= MAX_COUNTED_WORDS {
                            let key = build_key(*fid, *current_count as u8, &mut key_buffer);
                            cached_sorter.insert_del_u32(key, inner.docid())?;
                        }
                        if *new_count <= MAX_COUNTED_WORDS {
                            let key = build_key(*fid, *new_count as u8, &mut key_buffer);
                            cached_sorter.insert_add_u32(key, inner.docid())?;
                        }
                    }
                }
            }
            DocumentChange::Insertion(inner) => {
                let mut fid_word_count = HashMap::new();
                let mut token_fn = |_fname: &str, fid: FieldId, _pos: u16, _word: &str| {
                    fid_word_count.entry(fid).and_modify(|count| *count += 1).or_insert(1);
                    Ok(())
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;

                // The docids of the documents that have a number of words in a given field equal to or under than MAX_COUNTED_WORDS are stored.
                for (fid, count) in fid_word_count.iter() {
                    if *count <= MAX_COUNTED_WORDS {
                        let key = build_key(*fid, *count as u8, &mut key_buffer);
                        cached_sorter.insert_add_u32(key, inner.docid())?;
                    }
                }
            }
        }

        Ok(())
    }
}

fn build_key(fid: FieldId, count: u8, key_buffer: &mut Vec<u8>) -> &[u8] {
    key_buffer.clear();
    key_buffer.extend_from_slice(&fid.to_be_bytes());
    key_buffer.push(count);
    key_buffer.as_slice()
}
