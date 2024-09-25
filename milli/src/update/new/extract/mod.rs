mod cache;
mod faceted;
mod lru;
mod searchable;

use std::collections::HashMap;
use std::mem;

pub use faceted::*;
use grenad::MergeFunction;
use rayon::iter::IntoParallelIterator;
use rayon::slice::ParallelSliceMut as _;
pub use searchable::*;
use smallvec::SmallVec;

use super::DocumentChange;
use crate::update::{GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{GlobalFieldsIdsMap, Index, Result};

pub trait DocidsExtractor {
    fn run_extraction(
        index: &Index,
        fields_ids_map: &GlobalFieldsIdsMap,
        indexer: GrenadParameters,
        document_changes: impl IntoParallelIterator<Item = Result<DocumentChange>>,
    ) -> Result<HashMapMerger>;
}

pub struct HashMapMerger {
    maps: Vec<HashMap<SmallVec<[u8; cache::KEY_SIZE]>, cache::DelAddRoaringBitmap>>,
}

impl HashMapMerger {
    pub fn new() -> HashMapMerger {
        HashMapMerger { maps: Vec::new() }
    }

    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<
            Item = HashMap<SmallVec<[u8; cache::KEY_SIZE]>, cache::DelAddRoaringBitmap>,
        >,
    {
        self.maps.extend(iter);
    }
}

impl IntoIterator for HashMapMerger {
    type Item = (SmallVec<[u8; 12]>, cache::DelAddRoaringBitmap);
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut entries: Vec<_> = self.maps.into_iter().flat_map(|m| m.into_iter()).collect();
        entries.par_sort_unstable_by(|(ka, _), (kb, _)| ka.cmp(kb));
        IntoIter {
            sorted_entries: entries.into_iter(),
            current_key: None,
            current_deladd: cache::DelAddRoaringBitmap::default(),
        }
    }
}

pub struct IntoIter {
    sorted_entries: std::vec::IntoIter<(SmallVec<[u8; 12]>, cache::DelAddRoaringBitmap)>,
    current_key: Option<SmallVec<[u8; 12]>>,
    current_deladd: cache::DelAddRoaringBitmap,
}

impl Iterator for IntoIter {
    type Item = (SmallVec<[u8; 12]>, cache::DelAddRoaringBitmap);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.sorted_entries.next() {
                Some((k, deladd)) => {
                    if self.current_key.as_deref() == Some(k.as_slice()) {
                        self.current_deladd.merge_with(deladd);
                    } else {
                        let previous_key = self.current_key.replace(k);
                        let previous_deladd = mem::replace(&mut self.current_deladd, deladd);
                        if let Some(previous_key) = previous_key {
                            return Some((previous_key, previous_deladd));
                        }
                    }
                }
                None => {
                    let current_deladd = mem::take(&mut self.current_deladd);
                    return self.current_key.take().map(|ck| (ck, current_deladd));
                }
            }
        }
    }
}

/// TODO move in permissive json pointer
pub mod perm_json_p {
    use serde_json::{Map, Value};

    use crate::Result;
    const SPLIT_SYMBOL: char = '.';

    /// Returns `true` if the `selector` match the `key`.
    ///
    /// ```text
    /// Example:
    /// `animaux`           match `animaux`
    /// `animaux.chien`     match `animaux`
    /// `animaux.chien`     match `animaux`
    /// `animaux.chien.nom` match `animaux`
    /// `animaux.chien.nom` match `animaux.chien`
    /// -----------------------------------------
    /// `animaux`    doesn't match `animaux.chien`
    /// `animaux.`   doesn't match `animaux`
    /// `animaux.ch` doesn't match `animaux.chien`
    /// `animau`     doesn't match `animaux`
    /// ```
    pub fn contained_in(selector: &str, key: &str) -> bool {
        selector.starts_with(key)
            && selector[key.len()..].chars().next().map(|c| c == SPLIT_SYMBOL).unwrap_or(true)
    }

    pub fn seek_leaf_values_in_object(
        value: &Map<String, Value>,
        selectors: Option<&[&str]>,
        skip_selectors: &[&str],
        base_key: &str,
        seeker: &mut impl FnMut(&str, &Value) -> Result<()>,
    ) -> Result<()> {
        if value.is_empty() {
            seeker(&base_key, &Value::Object(Map::with_capacity(0)))?;
        }

        for (key, value) in value.iter() {
            let base_key = if base_key.is_empty() {
                key.to_string()
            } else {
                format!("{}{}{}", base_key, SPLIT_SYMBOL, key)
            };

            // here if the user only specified `doggo` we need to iterate in all the fields of `doggo`
            // so we check the contained_in on both side
            let should_continue = select_field(&base_key, selectors, skip_selectors);
            if should_continue {
                match value {
                    Value::Object(object) => seek_leaf_values_in_object(
                        object,
                        selectors,
                        skip_selectors,
                        &base_key,
                        seeker,
                    ),
                    Value::Array(array) => seek_leaf_values_in_array(
                        array,
                        selectors,
                        skip_selectors,
                        &base_key,
                        seeker,
                    ),
                    value => seeker(&base_key, value),
                }?;
            }
        }

        Ok(())
    }

    pub fn seek_leaf_values_in_array(
        values: &[Value],
        selectors: Option<&[&str]>,
        skip_selectors: &[&str],
        base_key: &str,
        seeker: &mut impl FnMut(&str, &Value) -> Result<()>,
    ) -> Result<()> {
        if values.is_empty() {
            seeker(&base_key, &Value::Array(vec![]))?;
        }

        for value in values {
            match value {
                Value::Object(object) => {
                    seek_leaf_values_in_object(object, selectors, skip_selectors, base_key, seeker)
                }
                Value::Array(array) => {
                    seek_leaf_values_in_array(array, selectors, skip_selectors, base_key, seeker)
                }
                value => seeker(base_key, value),
            }?;
        }

        Ok(())
    }

    pub fn select_field(
        field_name: &str,
        selectors: Option<&[&str]>,
        skip_selectors: &[&str],
    ) -> bool {
        selectors.map_or(true, |selectors| {
            selectors.iter().any(|selector| {
                contained_in(selector, &field_name) || contained_in(&field_name, selector)
            })
        }) && !skip_selectors.iter().any(|skip_selector| {
            contained_in(skip_selector, &field_name) || contained_in(&field_name, skip_selector)
        })
    }
}
