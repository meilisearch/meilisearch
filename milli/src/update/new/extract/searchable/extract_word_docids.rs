use std::borrow::Cow;

use heed::RoTxn;

use super::SearchableExtractor;
use crate::{bucketed_position, FieldId, Index, Result};

pub struct WordDocidsExtractor;
impl SearchableExtractor for WordDocidsExtractor {
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
    fn build_key<'a>(_field_id: FieldId, _position: u16, word: &'a str) -> Cow<'a, [u8]> {
        Cow::Borrowed(word.as_bytes())
    }
}

pub struct ExactWordDocidsExtractor;
impl SearchableExtractor for ExactWordDocidsExtractor {
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

    fn build_key<'a>(_field_id: FieldId, _position: u16, word: &'a str) -> Cow<'a, [u8]> {
        Cow::Borrowed(word.as_bytes())
    }
}

pub struct WordFidDocidsExtractor;
impl SearchableExtractor for WordFidDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
    }

    fn build_key<'a>(field_id: FieldId, _position: u16, word: &'a str) -> Cow<'a, [u8]> {
        let mut key = Vec::new();
        key.extend_from_slice(word.as_bytes());
        key.push(0);
        key.extend_from_slice(&field_id.to_be_bytes());
        Cow::Owned(key)
    }
}

pub struct WordPositionDocidsExtractor;
impl SearchableExtractor for WordPositionDocidsExtractor {
    fn attributes_to_extract<'a>(
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<Vec<&'a str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'a>(_rtxn: &'a RoTxn, _index: &'a Index) -> Result<Vec<&'a str>> {
        Ok(vec![])
    }

    fn build_key<'a>(_field_id: FieldId, position: u16, word: &'a str) -> Cow<'a, [u8]> {
        // position must be bucketed to reduce the number of keys in the DB.
        let position = bucketed_position(position);
        let mut key = Vec::new();
        key.extend_from_slice(word.as_bytes());
        key.push(0);
        key.extend_from_slice(&position.to_be_bytes());
        Cow::Owned(key)
    }
}
