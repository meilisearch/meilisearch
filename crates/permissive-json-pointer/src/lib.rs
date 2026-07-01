#![doc = include_str!("../README.md")]

use std::collections::HashSet;

use serde_json::*;

type Document = Map<String, Value>;

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

/// Given a single permissive JSON pointer `selector`, visit all the selected leaf values.
///
/// ```
/// use serde_json::{Value, json};
/// use permissive_json_pointer::visit_leaf_values;
///
/// let mut value: Value = json!({
///     "jean": {
///         "age": 8,
///     },
///     "jean.age": "young"
/// });
/// let mut age_string = String::new();
/// let mut age_number = 0;
/// visit_leaf_values(
///     value.as_object().unwrap(),
///     "jean.age",
///     &mut |value| match value {
///         Value::String(age) => age_string = age.clone(),
///         Value::Number(age) => age_number = age.as_u64().unwrap(),
///         _ => unreachable!(),
///     },
/// );
/// assert_eq!(
///     age_string,
///     "young"
/// );
/// assert_eq!(
///     age_number,
///     8
/// );
/// ```
pub fn visit_leaf_values<'a, F>(document: &'a Document, selector: &str, visit: &mut F)
where
    F: FnMut(&'a serde_json::Value),
{
    if document.is_empty() {
        return;
    }

    if let Some(value) = document.get(selector) {
        visit(value);
    }

    for (root, suffix) in root_dot_suffixes(selector) {
        match document.get(root) {
            Some(Value::Object(subdocument)) => visit_leaf_values(subdocument, suffix, visit),
            Some(Value::Array(values)) => {
                for subdocument in values {
                    let Value::Object(subdocument) = subdocument else {
                        continue;
                    };
                    visit_leaf_values(subdocument, suffix, visit)
                }
            }
            _ => (),
        };
    }
}

fn root_dot_suffixes(path: &str) -> impl Iterator<Item = (&str, &str)> {
    path.rmatch_indices('.').map(|(index, _)| (&path[0..index], &path[index + 1..]))
}

/// Map the selected leaf values of a json allowing you to update only the fields that were selected.
/// ```
/// use serde_json::{Value, json};
/// use permissive_json_pointer::map_leaf_values;
///
/// let mut value: Value = json!({
///     "jean": {
///         "age": 8,
///         "race": {
///             "name": "bernese mountain",
///             "size": "80cm",
///         }
///     }
/// });
/// map_leaf_values(
///     value.as_object_mut().unwrap(),
///     ["jean.race.name"],
///     |key, _array_indices, value| match (value, key) {
///         (Value::String(name), "jean.race.name") => *name = "patou".to_string(),
///         _ => unreachable!(),
///     },
/// );
/// assert_eq!(
///     value,
///     json!({
///         "jean": {
///             "age": 8,
///             "race": {
///                 "name": "patou",
///                 "size": "80cm",
///             }
///         }
///     })
/// );
/// ```
pub fn map_leaf_values<'a>(
    value: &mut Map<String, Value>,
    selectors: impl IntoIterator<Item = &'a str>,
    mut mapper: impl FnMut(&str, &[usize], &mut Value),
) {
    let selectors: Vec<_> = selectors.into_iter().collect();
    map_leaf_values_in_object(value, &selectors, "", &[], &mut mapper);
}

pub fn map_leaf_values_in_object(
    value: &mut Map<String, Value>,
    selectors: &[&str],
    base_key: &str,
    array_indices: &[usize],
    mapper: &mut impl FnMut(&str, &[usize], &mut Value),
) {
    for (key, value) in value.iter_mut() {
        let base_key = if base_key.is_empty() {
            key.to_string()
        } else {
            format!("{}{}{}", base_key, SPLIT_SYMBOL, key)
        };

        // here if the user only specified `doggo` we need to iterate in all the fields of `doggo`
        // so we check the contained_in on both side
        let should_continue = selectors
            .iter()
            .any(|selector| contained_in(selector, &base_key) || contained_in(&base_key, selector));

        if should_continue {
            match value {
                Value::Object(object) => {
                    map_leaf_values_in_object(object, selectors, &base_key, array_indices, mapper)
                }
                Value::Array(array) => {
                    map_leaf_values_in_array(array, selectors, &base_key, array_indices, mapper)
                }
                value => mapper(&base_key, array_indices, value),
            }
        }
    }
}

pub fn map_leaf_values_in_array(
    values: &mut [Value],
    selectors: &[&str],
    base_key: &str,
    base_array_indices: &[usize],
    mapper: &mut impl FnMut(&str, &[usize], &mut Value),
) {
    // This avoids allocating twice
    let mut array_indices = Vec::with_capacity(base_array_indices.len() + 1);
    array_indices.extend_from_slice(base_array_indices);
    array_indices.push(0);

    for (i, value) in values.iter_mut().enumerate() {
        *array_indices.last_mut().unwrap() = i;
        match value {
            Value::Object(object) => {
                map_leaf_values_in_object(object, selectors, base_key, &array_indices, mapper)
            }
            Value::Array(array) => {
                map_leaf_values_in_array(array, selectors, base_key, &array_indices, mapper)
            }
            value => mapper(base_key, &array_indices, value),
        }
    }
}

/// Permissively selects values in a json with a list of selectors.
/// Returns a new json containing all the selected fields.
/// ```
/// use serde_json::*;
/// use permissive_json_pointer::select_values;
///
/// let value: Value = json!({
///     "name": "peanut",
///     "age": 8,
///     "race": {
///         "name": "bernese mountain",
///         "avg_age": 12,
///         "size": "80cm",
///     },
/// });
/// let value: &Map<String, Value> = value.as_object().unwrap();
///
/// let res: Value = select_values(value.clone(), vec!["name", "race.name"]).into();
/// assert_eq!(
///     res,
///     json!({
///         "name": "peanut",
///         "race": {
///             "name": "bernese mountain",
///         },
///     })
/// );
/// ```
pub fn select_values<'a>(
    value: Document,
    selectors: impl IntoIterator<Item = &'a str>,
) -> Document {
    let selectors = selectors.into_iter().collect();
    create_value(value, selectors)
}

fn create_value(value: Document, mut selectors: HashSet<&str>) -> Document {
    let mut new_value: Document = Map::new();

    for (key, value) in value {
        // We insert all the key at the root level.
        // If the key was simple we can delete it and
        // move to the next key
        if selectors.contains(key.as_str()) && is_simple(&key) {
            selectors.remove(key.as_str());
            new_value.insert(key, value);
            continue;
        }

        // We extract all the sub selectors matching the current field
        // if there was [person.name, person.age] and if we are on the field
        // `person`. Then we generate the following sub selectors: [name, age].
        let sub_selectors: HashSet<&str> = selectors
            .iter()
            .filter(|s| contained_in(s, &key))
            .filter_map(|s| s.trim_start_matches(&key).get(SPLIT_SYMBOL.len_utf8()..))
            .collect();

        if !sub_selectors.is_empty() {
            match value {
                Value::Array(array) => {
                    let array = create_array(array, &sub_selectors);
                    if !array.is_empty() {
                        new_value.insert(key, array.into());
                    } else {
                        new_value.insert(key, Value::Array(vec![]));
                    }
                }
                Value::Object(object) => {
                    let object = create_value(object, sub_selectors);
                    if !object.is_empty() {
                        new_value.insert(key, object.into());
                    } else {
                        new_value.insert(key, Value::Object(Map::new()));
                    }
                }
                _ => (),
            }
        } else if selectors.contains(key.as_str()) {
            // In case the selector directly contains the key
            // and is not part of the any sub-selector
            // we register the entire value
            new_value.insert(key, value);
        }
    }

    new_value
}

fn create_array(array: Vec<Value>, selectors: &HashSet<&str>) -> Vec<Value> {
    let mut res = Vec::new();

    for value in array {
        match value {
            Value::Array(array) => {
                let array = create_array(array, selectors);
                if !array.is_empty() {
                    res.push(array.into());
                } else {
                    res.push(Value::Array(vec![]));
                }
            }
            Value::Object(object) => {
                let object = create_value(object, selectors.clone());
                if !object.is_empty() {
                    res.push(object.into());
                }
            }
            _ => (),
        }
    }

    res
}

fn is_simple(key: impl AsRef<str>) -> bool {
    !key.as_ref().contains(SPLIT_SYMBOL)
}

#[cfg(test)]
mod lib_test;
