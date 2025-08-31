use milli::Index;
use serde_json::json;

#[test]
fn test_sub_object_filtering() {
    // Create a test index
    let mut index = Index::new();

    // Add test documents
    index.add_documents(vec![
        json!({
            "id": 1,
            "users": [
                { "name": "kero", "age": 28 },
                { "name": "many", "age": 27 },
                { "name": "tamo", "age": 26 }
            ]
        }),
        json!({
            "id": 2,
            "users": [
                { "name": "kero", "age": 40 },
                { "name": "many", "age": 40 },
                { "name": "tamo", "age": 28 }
            ]
        }),
        json!({
            "id": 3,
            "users": [
                { "name": "kero", "age": 40 },
                { "name": "many", "age": 28 },
                { "name": "tamo", "age": 40 }
            ]
        })
    ]).unwrap();

    // Test traditional filtering (returns all docs)
    let traditional_filter = "users.name = kero AND users.age = 28";
    let result = index.search().with_filter(traditional_filter).execute().unwrap();
    assert_eq!(result.document_ids.len(), 3);
    assert!(result.document_ids.contains(&1));
    assert!(result.document_ids.contains(&2));
    assert!(result.document_ids.contains(&3));

    // Test sub-object filtering (returns only doc 1)
    let sub_object_filter = "users { name = kero AND age = 28 }";
    let result = index.search().with_filter(sub_object_filter).execute().unwrap();
    assert_eq!(result.document_ids.len(), 1);
    assert!(result.document_ids.contains(&1));

    // Test multiple conditions
    let complex_filter = "users { name = kero AND age = 28 } OR users { name = many AND age = 27 }";
    let result = index.search().with_filter(complex_filter).execute().unwrap();
    assert_eq!(result.document_ids.len(), 1);
    assert!(result.document_ids.contains(&1));

    // Test non-matching filter
    let non_matching_filter = "users { name = kero AND age = 99 }";
    let result = index.search().with_filter(non_matching_filter).execute().unwrap();
    assert_eq!(result.document_ids.len(), 0);

    // Test empty array field
    index.add_document(json!({
        "id": 4,
        "users": []
    })).unwrap();

    let result = index.search().with_filter(sub_object_filter).execute().unwrap();
    assert_eq!(result.document_ids.len(), 1);
    assert!(result.document_ids.contains(&1));
    assert!(!result.document_ids.contains(&4));

    // Test missing field
    index.add_document(json!({
        "id": 5,
        "other_field": "value"
    })).unwrap();

    let result = index.search().with_filter(sub_object_filter).execute().unwrap();
    assert_eq!(result.document_ids.len(), 1);
    assert!(result.document_ids.contains(&1));
    assert!(!result.document_ids.contains(&5));
}
