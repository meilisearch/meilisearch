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
            "a": {
              "b": "c",
              "d": "e",
              "f": "g"
            },
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
            "a": [42],
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
            "a": [null],
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
            "a": {
                "b": "c"
            },
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
            "a": [35],
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

#[test]
fn flatten_nested_values_keep_original_values() {
    let mut base: Value = json!({
        "tags": {
            "t1": "v1"
        },
        "prices": {
            "p1": [null],
            "p1000": {"tamo": {"le": {}}}
        },
        "kiki": [[]]
    });
    let json = std::mem::take(base.as_object_mut().unwrap());
    let flat = flatten(&json);

    println!("{}", serde_json::to_string_pretty(&flat).unwrap());

    assert_eq!(
        &flat,
        json!({
          "prices": {
            "p1": [null],
            "p1000": {
              "tamo": {
                "le": {}
              }
            }
          },
          "prices.p1": [null],
          "prices.p1000": {
            "tamo": {
              "le": {}
            }
          },
          "prices.p1000.tamo": {
            "le": {}
          },
          "prices.p1000.tamo.le": {},
          "tags": {
            "t1": "v1"
          },
          "tags.t1": "v1",
          "kiki": [[]]
        })
        .as_object()
        .unwrap()
    );
}
