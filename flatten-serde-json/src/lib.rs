#![doc = include_str!("../README.md")]

use serde_json::{Map, Value};

pub fn flatten(json: &Map<String, Value>) -> Map<String, Value> {
    let mut obj = Map::new();
    let mut all_keys = vec![];
    insert_object(&mut obj, None, json, &mut all_keys);
    for key in all_keys {
        obj.entry(key).or_insert(Value::Array(vec![]));
    }
    obj
}

fn insert_object(
    base_json: &mut Map<String, Value>,
    base_key: Option<&str>,
    object: &Map<String, Value>,
    all_keys: &mut Vec<String>,
) {
    for (key, value) in object {
        let new_key = base_key.map_or_else(|| key.clone(), |base_key| format!("{base_key}.{key}"));
        all_keys.push(new_key.clone());
        if let Some(array) = value.as_array() {
            insert_array(base_json, &new_key, array, all_keys);
        } else if let Some(object) = value.as_object() {
            insert_object(base_json, Some(&new_key), object, all_keys);
        } else {
            insert_value(base_json, &new_key, value.clone());
        }
    }
}

fn insert_array(
    base_json: &mut Map<String, Value>,
    base_key: &str,
    array: &Vec<Value>,
    all_keys: &mut Vec<String>,
) {
    for value in array {
        if let Some(object) = value.as_object() {
            insert_object(base_json, Some(base_key), object, all_keys);
        } else if let Some(sub_array) = value.as_array() {
            insert_array(base_json, base_key, sub_array, all_keys);
        } else {
            insert_value(base_json, base_key, value.clone());
        }
    }
}

fn insert_value(base_json: &mut Map<String, Value>, key: &str, to_insert: Value) {
    debug_assert!(!to_insert.is_object());
    debug_assert!(!to_insert.is_array());

    // does the field already exists?
    if let Some(value) = base_json.get_mut(key) {
        // is it already an array
        if let Some(array) = value.as_array_mut() {
            array.push(to_insert);
        // or is there a collision
        } else {
            let value = std::mem::take(value);
            base_json[key] = Value::Array(vec![value, to_insert]);
        }
        // if it does not exist we can push the value untouched
    } else {
        base_json.insert(key.to_string(), to_insert);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn no_flattening() {
        let mut base: Value = json!({
          "id": "287947",
          "title": "Shazam!",
          "release_date": 1553299200,
          "genres": [
            "Action",
            "Comedy",
            "Fantasy"
          ]
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        println!(
            "got:\n{}\nexpected:\n{}\n",
            serde_json::to_string_pretty(&flat).unwrap(),
            serde_json::to_string_pretty(&json).unwrap()
        );

        assert_eq!(flat, json);
    }

    #[test]
    fn flatten_object() {
        let mut base: Value = json!({
          "a": {
            "b": "c",
            "d": "e",
            "f": "g"
          }
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a": [],
                "a.b": "c",
                "a.d": "e",
                "a.f": "g"
            })
            .as_object()
            .unwrap()
        );
    }

    #[test]
    fn flatten_array() {
        let mut base: Value = json!({
          "a": [
            1,
            "b",
            [],
            [{}],
            { "b": "c" },
            { "b": "d" },
            { "b": "e" },
          ]
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a": [1, "b"],
                "a.b": ["c", "d", "e"],
            })
            .as_object()
            .unwrap()
        );

        // here we must keep 42 in "a"
        let mut base: Value = json!({
          "a": [
            42,
            { "b": "c" },
            { "b": "d" },
            { "b": "e" },
          ]
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a": 42,
                "a.b": ["c", "d", "e"],
            })
            .as_object()
            .unwrap()
        );

        // here we must keep 42 in "a"
        let mut base: Value = json!({
          "a": [
            { "b": "c" },
            { "b": "d" },
            { "b": "e" },
            null,
          ]
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a": null,
                "a.b": ["c", "d", "e"],
            })
            .as_object()
            .unwrap()
        );
    }

    #[test]
    fn collision_with_object() {
        let mut base: Value = json!({
          "a": {
            "b": "c",
          },
          "a.b": "d",
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a": [],
                "a.b": ["c", "d"],
            })
            .as_object()
            .unwrap()
        );
    }

    #[test]
    fn collision_with_array() {
        let mut base: Value = json!({
          "a": [
            { "b": "c" },
            { "b": "d", "c": "e" },
            [35],
          ],
          "a.b": "f",
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a.b": ["c", "d", "f"],
                "a.c": "e",
                "a": 35,
            })
            .as_object()
            .unwrap()
        );
    }

    #[test]
    fn flatten_nested_arrays() {
        let mut base: Value = json!({
          "a": [
            ["b", "c"],
            { "d": "e" },
            ["f", "g"],
            [
                { "h": "i" },
                { "d": "j" },
            ],
            ["k", "l"],
          ]
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        assert_eq!(
            &flat,
            json!({
                "a": ["b", "c", "f", "g", "k", "l"],
                "a.d": ["e", "j"],
                "a.h": "i",
            })
            .as_object()
            .unwrap()
        );
    }

    #[test]
    fn flatten_nested_arrays_and_objects() {
        let mut base: Value = json!({
          "a": [
            "b",
            ["c", "d"],
            { "e": ["f", "g"] },
            [
                { "h": "i" },
                { "e": ["j", { "z": "y" }] },
            ],
            ["l"],
            "m",
          ]
        });
        let json = std::mem::take(base.as_object_mut().unwrap());
        let flat = flatten(&json);

        println!("{}", serde_json::to_string_pretty(&flat).unwrap());

        assert_eq!(
            &flat,
            json!({
                "a": ["b", "c", "d", "l", "m"],
                "a.e": ["f", "g", "j"],
                "a.h": "i",
                "a.e.z": "y",
            })
            .as_object()
            .unwrap()
        );
    }
}
