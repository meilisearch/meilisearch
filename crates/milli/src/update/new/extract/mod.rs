mod cache;
mod documents;
mod faceted;
mod geo;
mod searchable;
mod vectors;

pub use cache::{
    merge_caches_sorted, transpose_and_freeze_caches, BalancedCaches, DelAddRoaringBitmap,
};
pub use documents::*;
pub use faceted::*;
pub use geo::*;
pub use searchable::*;
pub use vectors::{EmbeddingExtractor, SettingsChangeEmbeddingExtractor};

/// TODO move in permissive json pointer
pub mod perm_json_p {
    use serde_json::{Map, Value};

    use crate::attribute_patterns::PatternMatch;
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

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Depth {
        /// The perm json ptr is currently on the field of an object
        OnBaseKey,
        /// The perm json ptr is currently inside of an array
        InsideArray,
    }

    pub fn seek_leaf_values_in_object(
        value: &Map<String, Value>,
        base_key: &str,
        base_depth: Depth,
        seeker: &mut impl FnMut(&str, Depth, &Value) -> Result<PatternMatch>,
    ) -> Result<()> {
        if value.is_empty() {
            seeker(base_key, base_depth, &Value::Object(Map::with_capacity(0)))?;
        }

        for (key, value) in value.iter() {
            let base_key = if base_key.is_empty() {
                key.to_string()
            } else {
                format!("{}{}{}", base_key, SPLIT_SYMBOL, key)
            };

            let selection = seeker(&base_key, Depth::OnBaseKey, value)?;
            if selection != PatternMatch::NoMatch {
                match value {
                    Value::Object(object) => {
                        seek_leaf_values_in_object(object, &base_key, Depth::OnBaseKey, seeker)
                    }
                    Value::Array(array) => {
                        seek_leaf_values_in_array(array, &base_key, Depth::OnBaseKey, seeker)
                    }
                    _ => Ok(()),
                }?;
            }
        }

        Ok(())
    }

    pub fn seek_leaf_values_in_array(
        values: &[Value],
        base_key: &str,
        base_depth: Depth,
        seeker: &mut impl FnMut(&str, Depth, &Value) -> Result<PatternMatch>,
    ) -> Result<()> {
        if values.is_empty() {
            seeker(base_key, base_depth, &Value::Array(vec![]))?;
        }

        for value in values {
            match value {
                Value::Object(object) => {
                    seek_leaf_values_in_object(object, base_key, Depth::InsideArray, seeker)
                }
                Value::Array(array) => {
                    seek_leaf_values_in_array(array, base_key, Depth::InsideArray, seeker)
                }
                value => seeker(base_key, Depth::InsideArray, value).map(|_| ()),
            }?;
        }

        Ok(())
    }
}
