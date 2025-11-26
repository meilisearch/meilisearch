use big_s::S;
use heed::types::Bytes;
use maplit::{btreemap, btreeset};
use meili_snap::snapshot;

use super::*;
use crate::error::Error;
use crate::index::tests::TempIndex;
use crate::update::ClearDocuments;
use crate::{db_snap, Criterion, Filter, SearchResult};

#[test]
fn set_and_reset_searchable_fields() {
    let index = TempIndex::new();

    // First we send 3 documents with ids from 1 to 3.
    index
        .add_documents(documents!([
            { "id": 1, "name": "kevin", "age": 23 },
            { "id": 2, "name": "kevina", "age": 21},
            { "id": 3, "name": "benoit", "age": 34 }
        ]))
        .unwrap();

    // We change the searchable fields to be the "name" field only.
    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec!["name".into()]);
        })
        .unwrap();

    db_snap!(index, fields_ids_map, @r###"
    0   id               |
    1   name             |
    2   age              |
    "###);
    db_snap!(index, searchable_fields, @r###"["name"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
    fid weight
    1   0   |
    "###);

    // Check that the searchable field is correctly set to "name" only.
    let rtxn = index.read_txn().unwrap();
    // When we search for something that is not in
    // the searchable fields it must not return any document.
    let result = index.search(&rtxn).query("23").execute().unwrap();
    assert_eq!(result.documents_ids, Vec::<u32>::new());

    // When we search for something that is in the searchable fields
    // we must find the appropriate document.
    let result = index.search(&rtxn).query(r#""kevin""#).execute().unwrap();
    let documents = index.documents(&rtxn, result.documents_ids).unwrap();
    let fid_map = index.fields_ids_map(&rtxn).unwrap();
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].1.get(fid_map.id("name").unwrap()), Some(&br#""kevin""#[..]));
    drop(rtxn);

    // We change the searchable fields to be the "name" field only.
    index
        .update_settings(|settings| {
            settings.reset_searchable_fields();
        })
        .unwrap();

    db_snap!(index, fields_ids_map, @r###"
    0   id               |
    1   name             |
    2   age              |
    "###);
    db_snap!(index, searchable_fields, @r###"["id", "name", "age"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
    fid weight
    0   0   |
    1   0   |
    2   0   |
    "###);

    // Check that the searchable field have been reset and documents are found now.
    let rtxn = index.read_txn().unwrap();
    let fid_map = index.fields_ids_map(&rtxn).unwrap();
    let user_defined_searchable_fields = index.user_defined_searchable_fields(&rtxn).unwrap();
    snapshot!(format!("{user_defined_searchable_fields:?}"), @"None");
    // the searchable fields should contain all the fields
    let searchable_fields = index.searchable_fields(&rtxn).unwrap();
    snapshot!(format!("{searchable_fields:?}"), @r###"["id", "name", "age"]"###);
    let result = index.search(&rtxn).query("23").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
    let documents = index.documents(&rtxn, result.documents_ids).unwrap();
    assert_eq!(documents[0].1.get(fid_map.id("name").unwrap()), Some(&br#""kevin""#[..]));
}

#[test]
fn mixup_searchable_with_displayed_fields() {
    let index = TempIndex::new();

    let mut wtxn = index.write_txn().unwrap();
    // First we send 3 documents with ids from 1 to 3.
    index
        .add_documents_using_wtxn(
            &mut wtxn,
            documents!([
                { "id": 0, "name": "kevin", "age": 23},
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 }
            ]),
        )
        .unwrap();

    // In the same transaction we change the displayed fields to be only the "age".
    // We also change the searchable fields to be the "name" field only.
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_displayed_fields(vec!["age".into()]);
            settings.set_searchable_fields(vec!["name".into()]);
        })
        .unwrap();
    wtxn.commit().unwrap();

    // Check that the displayed fields are correctly set to `None` (default value).
    let rtxn = index.read_txn().unwrap();
    let fields_ids = index.displayed_fields(&rtxn).unwrap();
    assert_eq!(fields_ids.unwrap(), (&["age"][..]));
    drop(rtxn);

    // We change the searchable fields to be the "name" field only.
    index
        .update_settings(|settings| {
            settings.reset_searchable_fields();
        })
        .unwrap();

    // Check that the displayed fields always contains only the "age" field.
    let rtxn = index.read_txn().unwrap();
    let fields_ids = index.displayed_fields(&rtxn).unwrap();
    assert_eq!(fields_ids.unwrap(), &["age"][..]);
}

#[test]
fn default_displayed_fields() {
    let index = TempIndex::new();

    // First we send 3 documents with ids from 1 to 3.
    index
        .add_documents(documents!([
            { "id": 0, "name": "kevin", "age": 23},
            { "id": 1, "name": "kevina", "age": 21 },
            { "id": 2, "name": "benoit", "age": 34 }
        ]))
        .unwrap();

    // Check that the displayed fields are correctly set to `None` (default value).
    let rtxn = index.read_txn().unwrap();
    let fields_ids = index.displayed_fields(&rtxn).unwrap();
    assert_eq!(fields_ids, None);
}

#[test]
fn set_and_reset_displayed_field() {
    let index = TempIndex::new();

    let mut wtxn = index.write_txn().unwrap();
    index
        .add_documents_using_wtxn(
            &mut wtxn,
            documents!([
                { "id": 0, "name": "kevin", "age": 23},
                { "id": 1, "name": "kevina", "age": 21 },
                { "id": 2, "name": "benoit", "age": 34 }
            ]),
        )
        .unwrap();
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_displayed_fields(vec!["age".into()]);
        })
        .unwrap();
    wtxn.commit().unwrap();

    // Check that the displayed fields are correctly set to only the "age" field.
    let rtxn = index.read_txn().unwrap();
    let fields_ids = index.displayed_fields(&rtxn).unwrap();
    assert_eq!(fields_ids.unwrap(), &["age"][..]);
    drop(rtxn);

    // We reset the fields ids to become `None`, the default value.
    index
        .update_settings(|settings| {
            settings.reset_displayed_fields();
        })
        .unwrap();

    // Check that the displayed fields are correctly set to `None` (default value).
    let rtxn = index.read_txn().unwrap();
    let fields_ids = index.displayed_fields(&rtxn).unwrap();
    assert_eq!(fields_ids, None);
}

#[test]
fn set_filterable_fields() {
    let index = TempIndex::new();

    // Set the filterable fields to be the age.
    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("age"))]);
        })
        .unwrap();

    // Then index some documents.
    index
        .add_documents(documents!([
            { "id": 0, "name": "kevin", "age": 23},
            { "id": 1, "name": "kevina", "age": 21 },
            { "id": 2, "name": "benoit", "age": 34 }
        ]))
        .unwrap();

    // Check that the displayed fields are correctly set.
    let rtxn = index.read_txn().unwrap();
    // Only count the field_id 0 and level 0 facet values.
    // TODO we must support typed CSVs for numbers to be understood.
    let fidmap = index.fields_ids_map(&rtxn).unwrap();
    for document in index.all_documents(&rtxn).unwrap() {
        let document = document.unwrap();
        let json =
            crate::obkv_to_json(&fidmap.ids().collect::<Vec<_>>(), &fidmap, document.1).unwrap();
        println!("json: {:?}", json);
    }
    let count = index
        .facet_id_f64_docids
        .remap_key_type::<Bytes>()
        // The faceted field id is 2u16
        .prefix_iter(&rtxn, &[0, 2, 0])
        .unwrap()
        .count();
    assert_eq!(count, 3);
    drop(rtxn);

    // Index a little more documents with new and current facets values.
    index
        .add_documents(documents!([
            { "id": 3, "name": "kevin2", "age": 23},
            { "id": 4, "name": "kevina2", "age": 21 },
            { "id": 5, "name": "benoit", "age": 35 }
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    // Only count the field_id 0 and level 0 facet values.
    let count = index
        .facet_id_f64_docids
        .remap_key_type::<Bytes>()
        .prefix_iter(&rtxn, &[0, 2, 0])
        .unwrap()
        .count();
    assert_eq!(count, 4);

    // Set the filterable fields to be the age and the name.
    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field(S("age")),
                FilterableAttributesRule::Field(S("name")),
            ]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    // Only count the field_id 2 and level 0 facet values.
    let count = index
        .facet_id_f64_docids
        .remap_key_type::<Bytes>()
        .prefix_iter(&rtxn, &[0, 2, 0])
        .unwrap()
        .count();
    assert_eq!(count, 4);

    let rtxn = index.read_txn().unwrap();
    // Only count the field_id 1 and level 0 facet values.
    let count = index
        .facet_id_string_docids
        .remap_key_type::<Bytes>()
        .prefix_iter(&rtxn, &[0, 1])
        .unwrap()
        .count();
    assert_eq!(count, 5);

    // Remove the age from the filterable fields.
    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("name"))]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    // Only count the field_id 2 and level 0 facet values.
    let count = index
        .facet_id_f64_docids
        .remap_key_type::<Bytes>()
        .prefix_iter(&rtxn, &[0, 2, 0])
        .unwrap()
        .count();
    assert_eq!(count, 0);

    let rtxn = index.read_txn().unwrap();
    // Only count the field_id 1 and level 0 facet values.
    let count = index
        .facet_id_string_docids
        .remap_key_type::<Bytes>()
        .prefix_iter(&rtxn, &[0, 1])
        .unwrap()
        .count();
    assert_eq!(count, 5);
}

#[test]
fn set_asc_desc_field() {
    let index = TempIndex::new();

    // Set the filterable fields to be the age.
    index
        .update_settings(|settings| {
            settings.set_displayed_fields(vec![S("name")]);
            settings.set_criteria(vec![Criterion::Asc("age".to_owned())]);
        })
        .unwrap();

    // Then index some documents.
    index
        .add_documents(documents!([
            { "id": 0, "name": "kevin", "age": 23},
            { "id": 1, "name": "kevina", "age": 21 },
            { "id": 2, "name": "benoit", "age": 34 }
        ]))
        .unwrap();

    // Run an empty query just to ensure that the search results are ordered.
    let rtxn = index.read_txn().unwrap();
    let SearchResult { documents_ids, .. } = index.search(&rtxn).execute().unwrap();
    let documents = index.documents(&rtxn, documents_ids).unwrap();

    // Fetch the documents "age" field in the ordre in which the documents appear.
    let age_field_id = index.fields_ids_map(&rtxn).unwrap().id("age").unwrap();
    let iter = documents.into_iter().map(|(_, doc)| {
        let bytes = doc.get(age_field_id).unwrap();
        let string = std::str::from_utf8(bytes).unwrap();
        string.parse::<u32>().unwrap()
    });

    assert_eq!(iter.collect::<Vec<_>>(), vec![21, 23, 34]);
}

#[test]
fn set_distinct_field() {
    let index = TempIndex::new();

    // Set the filterable fields to be the age.
    index
        .update_settings(|settings| {
            // Don't display the generated `id` field.
            settings.set_displayed_fields(vec![S("name"), S("age")]);
            settings.set_distinct_field(S("age"));
        })
        .unwrap();

    // Then index some documents.
    index
        .add_documents(documents!([
            { "id": 0, "name": "kevin",  "age": 23 },
            { "id": 1, "name": "kevina", "age": 21 },
            { "id": 2, "name": "benoit", "age": 34 },
            { "id": 3, "name": "bernard", "age": 34 },
            { "id": 4, "name": "bertrand", "age": 34 },
            { "id": 5, "name": "bernie", "age": 34 },
            { "id": 6, "name": "ben", "age": 34 }
        ]))
        .unwrap();

    // Run an empty query just to ensure that the search results are ordered.
    let rtxn = index.read_txn().unwrap();
    let SearchResult { documents_ids, .. } = index.search(&rtxn).execute().unwrap();

    // There must be at least one document with a 34 as the age.
    assert_eq!(documents_ids.len(), 3);
}

#[test]
fn set_nested_distinct_field() {
    let index = TempIndex::new();

    // Set the filterable fields to be the age.
    index
        .update_settings(|settings| {
            // Don't display the generated `id` field.
            settings.set_displayed_fields(vec![S("person")]);
            settings.set_distinct_field(S("person.age"));
        })
        .unwrap();

    // Then index some documents.
    index
        .add_documents(documents!([
            { "id": 0, "person": { "name": "kevin", "age": 23 }},
            { "id": 1, "person": { "name": "kevina", "age": 21 }},
            { "id": 2, "person": { "name": "benoit", "age": 34 }},
            { "id": 3, "person": { "name": "bernard", "age": 34 }},
            { "id": 4, "person": { "name": "bertrand", "age": 34 }},
            { "id": 5, "person": { "name": "bernie", "age": 34 }},
            { "id": 6, "person": { "name": "ben", "age": 34 }}
        ]))
        .unwrap();

    // Run an empty query just to ensure that the search results are ordered.
    let rtxn = index.read_txn().unwrap();
    let SearchResult { documents_ids, .. } = index.search(&rtxn).execute().unwrap();

    // There must be at least one document with a 34 as the age.
    assert_eq!(documents_ids.len(), 3);
}

#[test]
fn default_stop_words() {
    let index = TempIndex::new();

    // First we send 3 documents with ids from 1 to 3.
    index
        .add_documents(documents!([
            { "id": 0, "name": "kevin", "age": 23},
            { "id": 1, "name": "kevina", "age": 21 },
            { "id": 2, "name": "benoit", "age": 34 }
        ]))
        .unwrap();

    // Ensure there is no stop_words by default
    let rtxn = index.read_txn().unwrap();
    let stop_words = index.stop_words(&rtxn).unwrap();
    assert!(stop_words.is_none());
}

#[test]
fn set_and_reset_stop_words() {
    let index = TempIndex::new();

    let mut wtxn = index.write_txn().unwrap();
    // First we send 3 documents with ids from 1 to 3.
    index
        .add_documents_using_wtxn(
            &mut wtxn,
            documents!([
                { "id": 0, "name": "kevin", "age": 23, "maxim": "I love dogs" },
                { "id": 1, "name": "kevina", "age": 21, "maxim": "Doggos are the best" },
                { "id": 2, "name": "benoit", "age": 34, "maxim": "The crepes are really good" },
            ]),
        )
        .unwrap();

    // In the same transaction we provide some stop_words
    let set = btreeset! { "i".to_string(), "the".to_string(), "are".to_string() };
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_stop_words(set.clone());
        })
        .unwrap();

    wtxn.commit().unwrap();

    // Ensure stop_words are effectively stored
    let rtxn = index.read_txn().unwrap();
    let stop_words = index.stop_words(&rtxn).unwrap();
    assert!(stop_words.is_some()); // at this point the index should return something

    let stop_words = stop_words.unwrap();
    let expected = fst::Set::from_iter(&set).unwrap();
    assert_eq!(stop_words.as_fst().as_bytes(), expected.as_fst().as_bytes());

    // when we search for something that is a non prefix stop_words it should be ignored
    // thus we should get a placeholder search (all the results = 3)
    let result = index.search(&rtxn).query("the ").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 3);
    let result = index.search(&rtxn).query("i ").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 3);
    let result = index.search(&rtxn).query("are ").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 3);

    let result = index.search(&rtxn).query("dog").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 2); // we have two maxims talking about doggos
    let result = index.search(&rtxn).query("benoît").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1); // there is one benoit in our data

    // now we'll reset the stop_words and ensure it's None
    index
        .update_settings(|settings| {
            settings.reset_stop_words();
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    let stop_words = index.stop_words(&rtxn).unwrap();
    assert!(stop_words.is_none());

    // now we can search for the stop words
    let result = index.search(&rtxn).query("the").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 2);
    let result = index.search(&rtxn).query("i").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
    let result = index.search(&rtxn).query("are").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 2);

    // the rest of the search is still not impacted
    let result = index.search(&rtxn).query("dog").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 2); // we have two maxims talking about doggos
    let result = index.search(&rtxn).query("benoît").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1); // there is one benoit in our data
}

#[test]
fn set_and_reset_synonyms() {
    let index = TempIndex::new();

    let mut wtxn = index.write_txn().unwrap();
    // Send 3 documents with ids from 1 to 3.
    index
        .add_documents_using_wtxn(
            &mut wtxn,
            documents!([
                { "id": 0, "name": "kevin", "age": 23, "maxim": "I love dogs"},
                { "id": 1, "name": "kevina", "age": 21, "maxim": "Doggos are the best"},
                { "id": 2, "name": "benoit", "age": 34, "maxim": "The crepes are really good"},
            ]),
        )
        .unwrap();

    // In the same transaction provide some synonyms
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_synonyms(btreemap! {
                "blini".to_string() => vec!["crepes".to_string()],
                "super like".to_string() => vec!["love".to_string()],
                "puppies".to_string() => vec!["dogs".to_string(), "doggos".to_string()]
            });
        })
        .unwrap();
    wtxn.commit().unwrap();

    // Ensure synonyms are effectively stored
    let rtxn = index.read_txn().unwrap();
    let synonyms = index.synonyms(&rtxn).unwrap();
    assert!(!synonyms.is_empty()); // at this point the index should return something

    // Check that we can use synonyms
    let result = index.search(&rtxn).query("blini").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
    let result = index.search(&rtxn).query("super like").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
    let result = index.search(&rtxn).query("puppies").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 2);

    // Reset the synonyms
    index
        .update_settings(|settings| {
            settings.reset_synonyms();
        })
        .unwrap();

    // Ensure synonyms are reset
    let rtxn = index.read_txn().unwrap();
    let synonyms = index.synonyms(&rtxn).unwrap();
    assert!(synonyms.is_empty());

    // Check that synonyms are no longer work
    let result = index.search(&rtxn).query("blini").execute().unwrap();
    assert!(result.documents_ids.is_empty());
    let result = index.search(&rtxn).query("super like").execute().unwrap();
    assert!(result.documents_ids.is_empty());
    let result = index.search(&rtxn).query("puppies").execute().unwrap();
    assert!(result.documents_ids.is_empty());
}

#[test]
fn thai_synonyms() {
    let index = TempIndex::new();

    let mut wtxn = index.write_txn().unwrap();
    // Send 3 documents with ids from 1 to 3.
    index
        .add_documents_using_wtxn(
            &mut wtxn,
            documents!([
                { "id": 0, "name": "ยี่ปุ่น" },
                { "id": 1, "name": "ญี่ปุ่น" },
            ]),
        )
        .unwrap();

    // In the same transaction provide some synonyms
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_synonyms(btreemap! {
                "japanese".to_string() => vec![S("ญี่ปุ่น"), S("ยี่ปุ่น")],
            });
        })
        .unwrap();
    wtxn.commit().unwrap();

    // Ensure synonyms are effectively stored
    let rtxn = index.read_txn().unwrap();
    let synonyms = index.synonyms(&rtxn).unwrap();
    assert!(!synonyms.is_empty()); // at this point the index should return something

    // Check that we can use synonyms
    let result = index.search(&rtxn).query("japanese").execute().unwrap();
    assert_eq!(result.documents_ids.len(), 2);
}

#[test]
fn setting_searchable_recomputes_other_settings() {
    let index = TempIndex::new();

    // Set all the settings except searchable
    index
        .update_settings(|settings| {
            settings.set_displayed_fields(vec!["hello".to_string()]);
            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field(S("age")),
                FilterableAttributesRule::Field(S("toto")),
            ]);
            settings.set_criteria(vec![Criterion::Asc(S("toto"))]);
        })
        .unwrap();

    // check the output
    let rtxn = index.read_txn().unwrap();
    assert_eq!(&["hello"][..], index.displayed_fields(&rtxn).unwrap().unwrap());
    // since no documents have been pushed the primary key is still unset
    assert!(index.primary_key(&rtxn).unwrap().is_none());
    assert_eq!(vec![Criterion::Asc("toto".to_string())], index.criteria(&rtxn).unwrap());
    drop(rtxn);

    // We set toto and age as searchable to force reordering of the fields
    index
        .update_settings(|settings| {
            settings.set_searchable_fields(vec!["toto".to_string(), "age".to_string()]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    assert_eq!(&["hello"][..], index.displayed_fields(&rtxn).unwrap().unwrap());
    assert!(index.primary_key(&rtxn).unwrap().is_none());
    assert_eq!(vec![Criterion::Asc("toto".to_string())], index.criteria(&rtxn).unwrap());
}

#[test]
fn setting_not_filterable_cant_filter() {
    let index = TempIndex::new();

    // Set all the settings except searchable
    index
        .update_settings(|settings| {
            settings.set_displayed_fields(vec!["hello".to_string()]);
            // It is only Asc(toto), there is a facet database but it is denied to filter with toto.
            settings.set_criteria(vec![Criterion::Asc(S("toto"))]);
        })
        .unwrap();

    let rtxn = index.read_txn().unwrap();
    let filter = Filter::from_str("toto = 32").unwrap().unwrap();
    let _ = filter.evaluate(&rtxn, &index).unwrap_err();
}

#[test]
fn setting_primary_key() {
    let index = TempIndex::new();

    let mut wtxn = index.write_txn().unwrap();
    // Set the primary key settings
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_primary_key(S("mykey"));
        })
        .unwrap();
    wtxn.commit().unwrap();
    let mut wtxn = index.write_txn().unwrap();
    assert_eq!(index.primary_key(&wtxn).unwrap(), Some("mykey"));

    // Then index some documents with the "mykey" primary key.
    index
        .add_documents_using_wtxn(
            &mut wtxn,
            documents!([
                { "mykey": 1, "name": "kevin",  "age": 23 },
                { "mykey": 2, "name": "kevina", "age": 21 },
                { "mykey": 3, "name": "benoit", "age": 34 },
                { "mykey": 4, "name": "bernard", "age": 34 },
                { "mykey": 5, "name": "bertrand", "age": 34 },
                { "mykey": 6, "name": "bernie", "age": 34 },
                { "mykey": 7, "name": "ben", "age": 34 }
            ]),
        )
        .unwrap();
    wtxn.commit().unwrap();

    // Updating settings with the same primary key should do nothing
    let mut wtxn = index.write_txn().unwrap();
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_primary_key(S("mykey"));
        })
        .unwrap();
    assert_eq!(index.primary_key(&wtxn).unwrap(), Some("mykey"));
    wtxn.commit().unwrap();

    // Updating the settings with a different (or no) primary key causes an error
    let mut wtxn = index.write_txn().unwrap();
    let error = index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.reset_primary_key();
        })
        .unwrap_err();
    assert!(matches!(error, Error::UserError(UserError::PrimaryKeyCannotBeChanged(_))));
    wtxn.abort();

    // But if we clear the database...
    let mut wtxn = index.write_txn().unwrap();
    let builder = ClearDocuments::new(&mut wtxn, &index);
    builder.execute().unwrap();
    wtxn.commit().unwrap();

    // ...we can change the primary key
    index
        .update_settings(|settings| {
            settings.set_primary_key(S("myid"));
        })
        .unwrap();
}

#[test]
fn setting_impact_relevancy() {
    let index = TempIndex::new();

    // Set the genres setting
    index
        .update_settings(|settings| {
            settings.set_filterable_fields(vec![FilterableAttributesRule::Field(S("genres"))]);
        })
        .unwrap();

    index.add_documents(documents!([
      {
        "id": 11,
        "title": "Star Wars",
        "overview":
          "Princess Leia is captured and held hostage by the evil Imperial forces in their effort to take over the galactic Empire. Venturesome Luke Skywalker and dashing captain Han Solo team together with the loveable robot duo R2-D2 and C-3PO to rescue the beautiful princess and restore peace and justice in the Empire.",
        "genres": ["Adventure", "Action", "Science Fiction"],
        "poster": "https://image.tmdb.org/t/p/w500/6FfCtAuVAW8XJjZ7eWeLibRLWTw.jpg",
        "release_date": 233366400
      },
      {
        "id": 30,
        "title": "Magnetic Rose",
        "overview": "",
        "genres": ["Animation", "Science Fiction"],
        "poster": "https://image.tmdb.org/t/p/w500/gSuHDeWemA1menrwfMRChnSmMVN.jpg",
        "release_date": 819676800
      }
    ])).unwrap();

    let rtxn = index.read_txn().unwrap();
    let SearchResult { documents_ids, .. } = index.search(&rtxn).query("S").execute().unwrap();
    let first_id = documents_ids[0];
    let documents = index.documents(&rtxn, documents_ids).unwrap();
    let (_, content) = documents.iter().find(|(id, _)| *id == first_id).unwrap();

    let fid = index.fields_ids_map(&rtxn).unwrap().id("title").unwrap();
    let line = std::str::from_utf8(content.get(fid).unwrap()).unwrap();
    assert_eq!(line, r#""Star Wars""#);
}

#[test]
fn test_disable_typo() {
    let index = TempIndex::new();

    let mut txn = index.write_txn().unwrap();
    assert!(index.authorize_typos(&txn).unwrap());

    index
        .update_settings_using_wtxn(&mut txn, |settings| {
            settings.set_authorize_typos(false);
        })
        .unwrap();

    assert!(!index.authorize_typos(&txn).unwrap());
}

#[test]
fn update_min_word_len_for_typo() {
    let index = TempIndex::new();

    // Set the genres setting
    index
        .update_settings(|settings| {
            settings.set_min_word_len_one_typo(8);
            settings.set_min_word_len_two_typos(8);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), 8);
    assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), 8);

    index
        .update_settings(|settings| {
            settings.reset_min_word_len_one_typo();
            settings.reset_min_word_len_two_typos();
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_ONE_TYPO);
    assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_TWO_TYPOS);
}

#[test]
fn update_invalid_min_word_len_for_typo() {
    let index = TempIndex::new();

    // Set the genres setting
    index
        .update_settings(|settings| {
            settings.set_min_word_len_one_typo(10);
            settings.set_min_word_len_two_typos(7);
        })
        .unwrap_err();
}

#[test]
fn update_exact_words_normalization() {
    let index = TempIndex::new();

    let mut txn = index.write_txn().unwrap();
    // Set the genres setting
    index
        .update_settings_using_wtxn(&mut txn, |settings| {
            let words = btreeset! { S("Ab"), S("ac") };
            settings.set_exact_words(words);
        })
        .unwrap();

    let exact_words = index.exact_words(&txn).unwrap().unwrap();
    for word in exact_words.into_fst().stream().into_str_vec().unwrap() {
        assert!(word.0 == "ac" || word.0 == "ab");
    }
}

#[test]
fn test_correct_settings_init() {
    let index = TempIndex::new();

    index
        .update_settings(|settings| {
            // we don't actually update the settings, just check their content
            let Settings {
                wtxn: _,
                index: _,
                indexer_config: _,
                searchable_fields,
                displayed_fields,
                filterable_fields,
                sortable_fields,
                foreign_keys,
                criteria,
                stop_words,
                non_separator_tokens,
                separator_tokens,
                dictionary,
                distinct_field,
                synonyms,
                primary_key,
                authorize_typos,
                min_word_len_two_typos,
                min_word_len_one_typo,
                exact_words,
                exact_attributes,
                max_values_per_facet,
                sort_facet_values_by,
                pagination_max_total_hits,
                proximity_precision,
                embedder_settings,
                search_cutoff,
                localized_attributes_rules,
                prefix_search,
                facet_search,
                disable_on_numbers,
                chat,
                vector_store,
            } = settings;
            assert!(matches!(searchable_fields, Setting::NotSet));
            assert!(matches!(displayed_fields, Setting::NotSet));
            assert!(matches!(filterable_fields, Setting::NotSet));
            assert!(matches!(sortable_fields, Setting::NotSet));
            assert!(matches!(foreign_keys, Setting::NotSet));
            assert!(matches!(criteria, Setting::NotSet));
            assert!(matches!(stop_words, Setting::NotSet));
            assert!(matches!(non_separator_tokens, Setting::NotSet));
            assert!(matches!(separator_tokens, Setting::NotSet));
            assert!(matches!(dictionary, Setting::NotSet));
            assert!(matches!(distinct_field, Setting::NotSet));
            assert!(matches!(synonyms, Setting::NotSet));
            assert!(matches!(primary_key, Setting::NotSet));
            assert!(matches!(authorize_typos, Setting::NotSet));
            assert!(matches!(min_word_len_two_typos, Setting::NotSet));
            assert!(matches!(min_word_len_one_typo, Setting::NotSet));
            assert!(matches!(exact_words, Setting::NotSet));
            assert!(matches!(exact_attributes, Setting::NotSet));
            assert!(matches!(max_values_per_facet, Setting::NotSet));
            assert!(matches!(sort_facet_values_by, Setting::NotSet));
            assert!(matches!(pagination_max_total_hits, Setting::NotSet));
            assert!(matches!(proximity_precision, Setting::NotSet));
            assert!(matches!(embedder_settings, Setting::NotSet));
            assert!(matches!(search_cutoff, Setting::NotSet));
            assert!(matches!(localized_attributes_rules, Setting::NotSet));
            assert!(matches!(prefix_search, Setting::NotSet));
            assert!(matches!(facet_search, Setting::NotSet));
            assert!(matches!(disable_on_numbers, Setting::NotSet));
            assert!(matches!(chat, Setting::NotSet));
            assert!(matches!(vector_store, Setting::NotSet));
        })
        .unwrap();
}

#[test]
fn settings_must_ignore_soft_deleted() {
    use serde_json::json;

    let index = TempIndex::new();

    let mut docs = vec![];
    for i in 0..10 {
        docs.push(json!({ "id": i, "title": format!("{:x}", i) }));
    }
    index.add_documents(documents! { docs }).unwrap();

    index.delete_documents((0..5).map(|id| id.to_string()).collect());

    let mut wtxn = index.write_txn().unwrap();
    index
        .update_settings_using_wtxn(&mut wtxn, |settings| {
            settings.set_searchable_fields(vec!["id".to_string()]);
        })
        .unwrap();
    wtxn.commit().unwrap();

    let rtxn = index.write_txn().unwrap();
    let docs: StdResult<Vec<_>, _> = index.all_documents(&rtxn).unwrap().collect();
    let docs = docs.unwrap();
    assert_eq!(docs.len(), 5);
}
