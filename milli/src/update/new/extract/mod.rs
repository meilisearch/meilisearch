mod cache;
mod faceted;
mod searchable;

pub use faceted::{
    FacetedExtractor, FieldIdFacetExistsDocidsExtractor, FieldIdFacetIsEmptyDocidsExtractor,
    FieldIdFacetIsNullDocidsExtractor, FieldIdFacetNumberDocidsExtractor,
    FieldIdFacetStringDocidsExtractor,
};
pub use searchable::{
    ExactWordDocidsExtractor, SearchableExtractor, WordDocidsExtractor, WordFidDocidsExtractor,
    WordPositionDocidsExtractor,
};

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
