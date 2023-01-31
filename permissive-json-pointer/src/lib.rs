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
fn contained_in(selector: &str, key: &str) -> bool {
    selector.starts_with(key)
        && selector[key.len()..].chars().next().map(|c| c == SPLIT_SYMBOL).unwrap_or(true)
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
///     |key, value| match (value, key) {
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
    mut mapper: impl FnMut(&str, &mut Value),
) {
    let selectors: Vec<_> = selectors.into_iter().collect();
    map_leaf_values_in_object(value, &selectors, "", &mut mapper);
}

pub fn map_leaf_values_in_object(
    value: &mut Map<String, Value>,
    selectors: &[&str],
    base_key: &str,
    mapper: &mut impl FnMut(&str, &mut Value),
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
                    map_leaf_values_in_object(object, selectors, &base_key, mapper)
                }
                Value::Array(array) => {
                    map_leaf_values_in_array(array, selectors, &base_key, mapper)
                }
                value => mapper(&base_key, value),
            }
        }
    }
}

pub fn map_leaf_values_in_array(
    values: &mut [Value],
    selectors: &[&str],
    base_key: &str,
    mapper: &mut impl FnMut(&str, &mut Value),
) {
    for value in values.iter_mut() {
        match value {
            Value::Object(object) => map_leaf_values_in_object(object, selectors, base_key, mapper),
            Value::Array(array) => map_leaf_values_in_array(array, selectors, base_key, mapper),
            value => mapper(base_key, value),
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
/// let res: Value = select_values(value, vec!["name", "race.name"]).into();
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
    value: &Map<String, Value>,
    selectors: impl IntoIterator<Item = &'a str>,
) -> Map<String, Value> {
    let selectors = selectors.into_iter().collect();
    create_value(value, selectors)
}

fn create_value(value: &Document, mut selectors: HashSet<&str>) -> Document {
    let mut new_value: Document = Map::new();

    for (key, value) in value.iter() {
        // first we insert all the key at the root level
        if selectors.contains(key as &str) {
            new_value.insert(key.to_string(), value.clone());
            // if the key was simple we can delete it and move to
            // the next key
            if is_simple(key) {
                selectors.remove(key as &str);
                continue;
            }
        }

        // we extract all the sub selectors matching the current field
        // if there was [person.name, person.age] and if we are on the field
        // `person`. Then we generate the following sub selectors: [name, age].
        let sub_selectors: HashSet<&str> = selectors
            .iter()
            .filter(|s| contained_in(s, key))
            .filter_map(|s| s.trim_start_matches(key).get(SPLIT_SYMBOL.len_utf8()..))
            .collect();

        if !sub_selectors.is_empty() {
            match value {
                Value::Array(array) => {
                    let array = create_array(array, &sub_selectors);
                    if !array.is_empty() {
                        new_value.insert(key.to_string(), array.into());
                    }
                }
                Value::Object(object) => {
                    let object = create_value(object, sub_selectors);
                    if !object.is_empty() {
                        new_value.insert(key.to_string(), object.into());
                    }
                }
                _ => (),
            }
        }
    }

    new_value
}

fn create_array(array: &[Value], selectors: &HashSet<&str>) -> Vec<Value> {
    let mut res = Vec::new();

    for value in array {
        match value {
            Value::Array(array) => {
                let array = create_array(array, selectors);
                if !array.is_empty() {
                    res.push(array.into());
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
mod tests {
    use big_s::S;

    use super::*;

    #[test]
    fn test_contained_in() {
        assert!(contained_in("animaux", "animaux"));
        assert!(contained_in("animaux.chien", "animaux"));
        assert!(contained_in("animaux.chien.race.bouvier bernois.fourrure.couleur", "animaux"));
        assert!(contained_in(
            "animaux.chien.race.bouvier bernois.fourrure.couleur",
            "animaux.chien"
        ));
        assert!(contained_in(
            "animaux.chien.race.bouvier bernois.fourrure.couleur",
            "animaux.chien.race.bouvier bernois"
        ));
        assert!(contained_in(
            "animaux.chien.race.bouvier bernois.fourrure.couleur",
            "animaux.chien.race.bouvier bernois.fourrure"
        ));
        assert!(contained_in(
            "animaux.chien.race.bouvier bernois.fourrure.couleur",
            "animaux.chien.race.bouvier bernois.fourrure.couleur"
        ));

        // -- the wrongs
        assert!(!contained_in("chien", "chat"));
        assert!(!contained_in("animaux", "animaux.chien"));
        assert!(!contained_in("animaux.chien", "animaux.chat"));

        // -- the strange edge cases
        assert!(!contained_in("animaux.chien", "anima"));
        assert!(!contained_in("animaux.chien", "animau"));
        assert!(!contained_in("animaux.chien", "animaux."));
        assert!(!contained_in("animaux.chien", "animaux.c"));
        assert!(!contained_in("animaux.chien", "animaux.ch"));
        assert!(!contained_in("animaux.chien", "animaux.chi"));
        assert!(!contained_in("animaux.chien", "animaux.chie"));
    }

    #[test]
    fn simple_key() {
        let value: Value = json!({
            "name": "peanut",
            "age": 8,
            "race": {
                "name": "bernese mountain",
                "avg_age": 12,
                "size": "80cm",
            }
        });
        let value: &Document = value.as_object().unwrap();

        let res: Value = select_values(value, vec!["name"]).into();
        assert_eq!(
            res,
            json!({
                "name": "peanut",
            })
        );

        let res: Value = select_values(value, vec!["age"]).into();
        assert_eq!(
            res,
            json!({
                "age": 8,
            })
        );

        let res: Value = select_values(value, vec!["name", "age"]).into();
        assert_eq!(
            res,
            json!({
                "name": "peanut",
                "age": 8,
            })
        );

        let res: Value = select_values(value, vec!["race"]).into();
        assert_eq!(
            res,
            json!({
                "race": {
                    "name": "bernese mountain",
                    "avg_age": 12,
                    "size": "80cm",
                }
            })
        );

        let res: Value = select_values(value, vec!["name", "age", "race"]).into();
        assert_eq!(
            res,
            json!({
                "name": "peanut",
                "age": 8,
                "race": {
                    "name": "bernese mountain",
                    "avg_age": 12,
                    "size": "80cm",
                }
            })
        );
    }

    #[test]
    fn complex_key() {
        let value: Value = json!({
            "name": "peanut",
            "age": 8,
            "race": {
                "name": "bernese mountain",
                "avg_age": 12,
                "size": "80cm",
            }
        });
        let value: &Document = value.as_object().unwrap();

        let res: Value = select_values(value, vec!["race"]).into();
        assert_eq!(
            res,
            json!({
                "race": {
                    "name": "bernese mountain",
                    "avg_age": 12,
                    "size": "80cm",
                }
            })
        );

        println!("RIGHT BEFORE");

        let res: Value = select_values(value, vec!["race.name"]).into();
        assert_eq!(
            res,
            json!({
                "race": {
                    "name": "bernese mountain",
                }
            })
        );

        let res: Value = select_values(value, vec!["race.name", "race.size"]).into();
        assert_eq!(
            res,
            json!({
                "race": {
                    "name": "bernese mountain",
                    "size": "80cm",
                }
            })
        );

        let res: Value = select_values(
            value,
            vec!["race.name", "race.size", "race.avg_age", "race.size", "age"],
        )
        .into();
        assert_eq!(
            res,
            json!({
                "age": 8,
                "race": {
                    "name": "bernese mountain",
                    "avg_age": 12,
                    "size": "80cm",
                }
            })
        );

        let res: Value = select_values(value, vec!["race.name", "race"]).into();
        assert_eq!(
            res,
            json!({
                "race": {
                    "name": "bernese mountain",
                    "avg_age": 12,
                    "size": "80cm",
                }
            })
        );

        let res: Value = select_values(value, vec!["race", "race.name"]).into();
        assert_eq!(
            res,
            json!({
                "race": {
                    "name": "bernese mountain",
                    "avg_age": 12,
                    "size": "80cm",
                }
            })
        );
    }

    #[test]
    fn multi_level_nested() {
        let value: Value = json!({
            "jean": {
                "age": 8,
                "race": {
                    "name": "bernese mountain",
                    "size": "80cm",
                }
            }
        });
        let value: &Document = value.as_object().unwrap();

        let res: Value = select_values(value, vec!["jean"]).into();
        assert_eq!(
            res,
            json!({
                "jean": {
                    "age": 8,
                    "race": {
                        "name": "bernese mountain",
                        "size": "80cm",
                    }
                }
            })
        );

        let res: Value = select_values(value, vec!["jean.age"]).into();
        assert_eq!(
            res,
            json!({
                "jean": {
                    "age": 8,
                }
            })
        );

        let res: Value = select_values(value, vec!["jean.race.size"]).into();
        assert_eq!(
            res,
            json!({
                "jean": {
                    "race": {
                        "size": "80cm",
                    }
                }
            })
        );

        let res: Value = select_values(value, vec!["jean.race.name", "jean.age"]).into();
        assert_eq!(
            res,
            json!({
                "jean": {
                    "age": 8,
                    "race": {
                        "name": "bernese mountain",
                    }
                }
            })
        );

        let res: Value = select_values(value, vec!["jean.race"]).into();
        assert_eq!(
            res,
            json!({
                "jean": {
                    "race": {
                        "name": "bernese mountain",
                        "size": "80cm",
                    }
                }
            })
        );
    }

    #[test]
    fn array_and_deep_nested() {
        let value: Value = json!({
            "doggos": [
                {
                    "jean": {
                        "age": 8,
                        "race": {
                            "name": "bernese mountain",
                            "size": "80cm",
                        }
                    }
                },
                {
                    "marc": {
                        "age": 4,
                        "race": {
                            "name": "golden retriever",
                            "size": "60cm",
                        }
                    }
                },
            ]
        });
        let value: &Document = value.as_object().unwrap();

        let res: Value = select_values(value, vec!["doggos.jean"]).into();
        assert_eq!(
            res,
            json!({
                "doggos": [
                    {
                        "jean": {
                            "age": 8,
                            "race": {
                                "name": "bernese mountain",
                                "size": "80cm",
                            }
                        }
                    }
                ]
            })
        );

        let res: Value = select_values(value, vec!["doggos.marc"]).into();
        assert_eq!(
            res,
            json!({
                "doggos": [
                    {
                        "marc": {
                            "age": 4,
                            "race": {
                                "name": "golden retriever",
                                "size": "60cm",
                            }
                        }
                    }
                ]
            })
        );

        let res: Value = select_values(value, vec!["doggos.marc.race"]).into();
        assert_eq!(
            res,
            json!({
                "doggos": [
                    {
                        "marc": {
                            "race": {
                                "name": "golden retriever",
                                "size": "60cm",
                            }
                        }
                    }
                ]
            })
        );

        let res: Value =
            select_values(value, vec!["doggos.marc.race.name", "doggos.marc.age"]).into();

        assert_eq!(
            res,
            json!({
                "doggos": [
                    {
                        "marc": {
                            "age": 4,
                            "race": {
                                "name": "golden retriever",
                            }
                        }
                    }
                ]
            })
        );

        let res: Value = select_values(
            value,
            vec![
                "doggos.marc.race.name",
                "doggos.marc.age",
                "doggos.jean.race.name",
                "other.field",
            ],
        )
        .into();

        assert_eq!(
            res,
            json!({
                "doggos": [
                    {
                        "jean": {
                            "race": {
                                "name": "bernese mountain",
                            }
                        }
                    },
                    {
                        "marc": {
                            "age": 4,
                            "race": {
                                "name": "golden retriever",
                            }
                        }
                    }
                ]
            })
        );
    }

    #[test]
    fn all_conflict_variation() {
        let value: Value = json!({
           "pet.dog.name": "jean",
           "pet.dog": {
             "name": "bob"
           },
           "pet": {
             "dog.name": "michel"
           },
           "pet": {
             "dog": {
               "name": "milan"
             }
           }
        });
        let value: &Document = value.as_object().unwrap();

        let res: Value = select_values(value, vec!["pet.dog.name"]).into();
        assert_eq!(
            res,
            json!({
               "pet.dog.name": "jean",
               "pet.dog": {
                 "name": "bob"
               },
               "pet": {
                 "dog.name": "michel"
               },
               "pet": {
                 "dog": {
                   "name": "milan"
                 }
               }
            })
        );

        let value: Value = json!({
           "pet.dog.name": "jean",
           "pet.dog": {
             "name": "bob",
           },
           "pet": {
             "dog.name": "michel",
             "dog": {
               "name": "milan",
             }
           }
        });
        let value: &Document = value.as_object().unwrap();

        let res: Value = select_values(value, vec!["pet.dog.name", "pet.dog", "pet"]).into();

        assert_eq!(
            res,
            json!({
               "pet.dog.name": "jean",
               "pet.dog": {
                 "name": "bob",
               },
               "pet": {
                 "dog.name": "michel",
                 "dog": {
                   "name": "milan",
                 }
               }
            })
        );
    }

    #[test]
    fn map_object() {
        let mut value: Value = json!({
            "jean": {
                "age": 8,
                "race": {
                    "name": "bernese mountain",
                    "size": "80cm",
                }
            }
        });

        map_leaf_values(value.as_object_mut().unwrap(), ["jean.race.name"], |key, value| {
            match (value, key) {
                (Value::String(name), "jean.race.name") => *name = S("patou"),
                _ => unreachable!(),
            }
        });

        assert_eq!(
            value,
            json!({
                "jean": {
                    "age": 8,
                    "race": {
                        "name": "patou",
                        "size": "80cm",
                    }
                }
            })
        );

        let mut value: Value = json!({
            "jean": {
                "age": 8,
                "race": {
                    "name": "bernese mountain",
                    "size": "80cm",
                }
            },
            "bob": "lolpied",
        });

        let mut calls = 0;
        map_leaf_values(value.as_object_mut().unwrap(), ["jean"], |key, value| {
            calls += 1;
            match (value, key) {
                (Value::String(name), "jean.race.name") => *name = S("patou"),
                _ => println!("Called with {key}"),
            }
        });

        assert_eq!(calls, 3);
        assert_eq!(
            value,
            json!({
                "jean": {
                    "age": 8,
                    "race": {
                        "name": "patou",
                        "size": "80cm",
                    }
                },
                "bob": "lolpied",
            })
        );
    }
}
