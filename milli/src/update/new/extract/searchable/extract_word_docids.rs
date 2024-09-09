use std::borrow::Cow;

use heed::RoTxn;

use super::{tokenize_document::DocumentTokenizer, SearchableExtractor};
use crate::{
    bucketed_position,
    update::{
        new::{extract::cache::CboCachedSorter, DocumentChange},
        MergeDeladdCboRoaringBitmaps,
    },
    FieldId, GlobalFieldsIdsMap, Index, Result,
};

trait ProtoWordDocidsExtractor {
    fn build_key(field_id: FieldId, position: u16, word: &str) -> Cow<'_, [u8]>;
    fn attributes_to_extract<'a>(
        _rtxn: &'a RoTxn,
        _index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>>;

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>>;
}

impl<T> SearchableExtractor for T
where
    T: ProtoWordDocidsExtractor,
{
    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        document_tokenizer: &DocumentTokenizer,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        cached_sorter: &mut CboCachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()> {
        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut token_fn = |fid, pos: u16, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    cached_sorter.insert_del_u32(&key, inner.docid()).map_err(crate::Error::from)
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Update(inner) => {
                let mut token_fn = |fid, pos, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    cached_sorter.insert_del_u32(&key, inner.docid()).map_err(crate::Error::from)
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut token_fn,
                )?;

                let mut token_fn = |fid, pos, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    cached_sorter.insert_add_u32(&key, inner.docid()).map_err(crate::Error::from)
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;
            }
            DocumentChange::Insertion(inner) => {
                let mut token_fn = |fid, pos, word: &str| {
                    let key = Self::build_key(fid, pos, word);
                    cached_sorter.insert_add_u32(&key, inner.docid()).map_err(crate::Error::from)
                };
                document_tokenizer.tokenize_document(inner.new(), fields_ids_map, &mut token_fn)?;
            }
        }

        Ok(())
    }

    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        Self::attributes_to_extract(rtxn, index)
    }

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>> {
        Self::attributes_to_skip(rtxn, index)
    }
}

pub struct WordDocidsExtractor;
impl ProtoWordDocidsExtractor for WordDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<Vec<&'a str>> {
        // exact attributes must be skipped and stored in a separate DB, see `ExactWordDocidsExtractor`.
        index.exact_attributes(rtxn).map_err(Into::into)
    }

    /// TODO write in an external Vec buffer
    fn build_key(_field_id: FieldId, _position: u16, word: &str) -> Cow<[u8]> {
        Cow::Borrowed(word.as_bytes())
    }
}

pub struct ExactWordDocidsExtractor;
impl ProtoWordDocidsExtractor for ExactWordDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        let exact_attributes = index.exact_attributes(rtxn)?;
        // If there are no user-defined searchable fields, we return all exact attributes.
        // Otherwise, we return the intersection of exact attributes and user-defined searchable fields.
        if let Some(searchable_attributes) = index.user_defined_searchable_fields(rtxn)? {
            let attributes = exact_attributes
                .into_iter()
                .filter(|attr| searchable_attributes.contains(attr))
                .collect();
            Ok(Some(attributes))
        } else {
            Ok(Some(exact_attributes))
        }
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
    }

    fn build_key(_field_id: FieldId, _position: u16, word: &str) -> Cow<[u8]> {
        Cow::Borrowed(word.as_bytes())
    }
}

pub struct WordFidDocidsExtractor;
impl ProtoWordDocidsExtractor for WordFidDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
    }

    fn build_key(field_id: FieldId, _position: u16, word: &str) -> Cow<[u8]> {
        let mut key = Vec::new();
        key.extend_from_slice(word.as_bytes());
        key.push(0);
        key.extend_from_slice(&field_id.to_be_bytes());
        Cow::Owned(key)
    }
}

pub struct WordPositionDocidsExtractor;
impl ProtoWordDocidsExtractor for WordPositionDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
    }

    fn build_key(_field_id: FieldId, position: u16, word: &str) -> Cow<[u8]> {
        // position must be bucketed to reduce the number of keys in the DB.
        let position = bucketed_position(position);
        let mut key = Vec::new();
        key.extend_from_slice(word.as_bytes());
        key.push(0);
        key.extend_from_slice(&position.to_be_bytes());
        Cow::Owned(key)
    }
}
