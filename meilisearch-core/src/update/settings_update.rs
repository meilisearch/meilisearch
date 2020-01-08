use std::collections::{HashMap, BTreeMap, BTreeSet};

use heed::Result as ZResult;
use fst::{set::OpBuilder, SetBuilder};
use sdset::SetBuf;

use meilisearch_schema::{Schema, SchemaAttr, diff_transposition, generate_schema};

use crate::database::{MainT, UpdateT};
use crate::settings::{UpdateState, SettingsUpdate};
use crate::update::documents_addition::reindex_all_documents;
use crate::update::{next_update_id, Update};
use crate::{store, MResult};

pub fn push_settings_update(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    settings: SettingsUpdate,
) -> ZResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::settings(settings);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_settings_update(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    settings: SettingsUpdate,
) -> MResult<()> {


    let mut must_reindex = false;

    let old_schema = index.main.schema(writer)?;

    match settings.ranking_rules {
        UpdateState::Update(v) => {
            index.main.put_ranking_rules(writer, v)?;
        },
        UpdateState::Clear => {
            index.main.delete_ranking_rules(writer)?;
        },
        _ => (),
    }
    match settings.ranking_distinct {
        UpdateState::Update(v) => {
            index.main.put_ranking_distinct(writer, v)?;
        },
        UpdateState::Clear => {
            index.main.delete_ranking_distinct(writer)?;
        },
        _ => (),
    }
    let identifier = match settings.attribute_identifier.clone() {
        UpdateState::Update(v) => v,
        _ => {
            old_schema.clone().unwrap().identifier_name().to_owned()
        },
    };
    let attributes_searchable: Vec<String> = match settings.attributes_searchable.clone() {
        UpdateState::Update(v) => v,
        UpdateState::Clear => Vec::new(),
        UpdateState::Nothing => {
            match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_indexed())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            }
        },
        UpdateState::Add(attrs) => {
            let mut old_attrs = match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_indexed())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            };
            for attr in attrs {
                if !old_attrs.contains(&attr) {
                    old_attrs.push(attr);
                }
            }
            old_attrs
        },
        UpdateState::Delete(attrs) => {
            let mut old_attrs = match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_indexed())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            };
            for attr in attrs {
                old_attrs.retain(|x| *x == attr)
            }
            old_attrs
        }
    };
    let attributes_displayed: Vec<String> = match settings.attributes_displayed.clone() {
        UpdateState::Update(v) => v,
        UpdateState::Clear => Vec::new(),
        UpdateState::Nothing => {
            match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_displayed())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            }
        },
        UpdateState::Add(attrs) => {
            let mut old_attrs = match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_displayed())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            };
            for attr in attrs {
                if !old_attrs.contains(&attr) {
                    old_attrs.push(attr);
                }
            }
            old_attrs
        },
        UpdateState::Delete(attrs) => {
            let mut old_attrs = match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_displayed())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            };
            for attr in attrs {
                old_attrs.retain(|x| *x == attr)
            }
            old_attrs
        }
    };
    let attributes_ranked: Vec<String> = match settings.attributes_ranked.clone() {
        UpdateState::Update(v) => v,
        UpdateState::Clear => Vec::new(),
        UpdateState::Nothing => {
            match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_ranked())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            }
        },
        UpdateState::Add(attrs) => {
            let mut old_attrs = match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_ranked())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            };
            for attr in attrs {
                if !old_attrs.contains(&attr) {
                    old_attrs.push(attr);
                }
            }
            old_attrs
        },
        UpdateState::Delete(attrs) => {
            let mut old_attrs = match old_schema.clone() {
                Some(schema) => {
                    schema.into_iter()
                        .filter(|(_, props)| props.is_ranked())
                        .map(|(name, _)| name)
                        .collect()
                },
                None => Vec::new(),
            };
            for attr in attrs {
                old_attrs.retain(|x| *x == attr)
            }
            old_attrs
        }
    };

    let new_schema = generate_schema(identifier, attributes_searchable, attributes_displayed, attributes_ranked);

    index.main.put_schema(writer, &new_schema)?;

    match settings.stop_words {
        UpdateState::Update(stop_words) => {
            if apply_stop_words_update(writer, index, stop_words)? {
                must_reindex = true;
            }
        },
        UpdateState::Clear => {
            if apply_stop_words_update(writer, index, BTreeSet::new())? {
                must_reindex = true;
            }
        },
        _ => (),
    }

    match settings.synonyms {
        UpdateState::Update(synonyms) => apply_synonyms_update(writer, index, synonyms)?,
        UpdateState::Clear => apply_synonyms_update(writer, index, BTreeMap::new())?,
        _ => (),
    }

    let main_store = index.main;
    let documents_fields_store = index.documents_fields;
    let documents_fields_counts_store = index.documents_fields_counts;
    let postings_lists_store = index.postings_lists;
    let docs_words_store = index.docs_words;

    if settings.attribute_identifier.is_changed() ||
        settings.attributes_ranked.is_changed() ||
        settings.attributes_searchable.is_changed() ||
        settings.attributes_displayed.is_changed()
    {
        if let Some(old_schema) = old_schema {
            rewrite_all_documents(writer, index, &old_schema, &new_schema)?;
            must_reindex = true;
        }
    }
    if must_reindex {
        reindex_all_documents(
            writer,
            main_store,
            documents_fields_store,
            documents_fields_counts_store,
            postings_lists_store,
            docs_words_store,
        )?;
    }
    Ok(())
}

pub fn apply_stop_words_update(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    stop_words: BTreeSet<String>,
) -> MResult<bool> {

    let main_store = index.main;
    let mut must_reindex = false;

    let old_stop_words: BTreeSet<String> = main_store
        .stop_words_fst(writer)?
        .unwrap_or_default()
        .stream()
        .into_strs().unwrap().into_iter().collect();

    let deletion: BTreeSet<String> = old_stop_words.clone().difference(&stop_words).cloned().collect();
    let addition: BTreeSet<String> = stop_words.clone().difference(&old_stop_words).cloned().collect();

    if !addition.is_empty() {
        apply_stop_words_addition(
            writer,
            index,
            addition
        )?;
    }

    if !deletion.is_empty() {
        must_reindex = apply_stop_words_deletion(
            writer,
            index,
            deletion
        )?;
    }

    main_store.put_stop_words(writer, stop_words)?;

    Ok(must_reindex)
}

fn apply_stop_words_addition(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    addition: BTreeSet<String>,
) -> MResult<()> {

    let main_store = index.main;
    let postings_lists_store = index.postings_lists;

    let mut stop_words_builder = SetBuilder::memory();

    for word in addition {
        stop_words_builder.insert(&word).unwrap();
        // we remove every posting list associated to a new stop word
        postings_lists_store.del_postings_list(writer, word.as_bytes())?;
    }

    // create the new delta stop words fst
    let delta_stop_words = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    // we also need to remove all the stop words from the main fst
    if let Some(word_fst) = main_store.words_fst(writer)? {
        let op = OpBuilder::new()
            .add(&word_fst)
            .add(&delta_stop_words)
            .difference();

        let mut word_fst_builder = SetBuilder::memory();
        word_fst_builder.extend_stream(op).unwrap();
        let word_fst = word_fst_builder
            .into_inner()
            .and_then(fst::Set::from_bytes)
            .unwrap();

        main_store.put_words_fst(writer, &word_fst)?;
    }

    // now we add all of these stop words from the main store
    let stop_words_fst = main_store.stop_words_fst(writer)?.unwrap_or_default();

    let op = OpBuilder::new()
        .add(&stop_words_fst)
        .add(&delta_stop_words)
        .r#union();

    let mut stop_words_builder = SetBuilder::memory();
    stop_words_builder.extend_stream(op).unwrap();
    let stop_words_fst = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    main_store.put_stop_words_fst(writer, &stop_words_fst)?;

    Ok(())
}

fn apply_stop_words_deletion(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    deletion: BTreeSet<String>,
) -> MResult<bool> {

    let main_store = index.main;

    let mut stop_words_builder = SetBuilder::memory();

    for word in deletion {
        stop_words_builder.insert(&word).unwrap();
    }

    // create the new delta stop words fst
    let delta_stop_words = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    // now we delete all of these stop words from the main store
    let stop_words_fst = main_store.stop_words_fst(writer)?.unwrap_or_default();

    let op = OpBuilder::new()
        .add(&stop_words_fst)
        .add(&delta_stop_words)
        .difference();

    let mut stop_words_builder = SetBuilder::memory();
    stop_words_builder.extend_stream(op).unwrap();
    let stop_words_fst = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    main_store.put_stop_words_fst(writer, &stop_words_fst)?;

    // now that we have setup the stop words
    // lets reindex everything...
    if let Ok(number) = main_store.number_of_documents(writer) {
        if number > 0 {
            return Ok(true)
        }
    }

    Ok(false)
}

pub fn apply_synonyms_update(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    synonyms: BTreeMap<String, Vec<String>>,
) -> MResult<()> {

    let main_store = index.main;
    let synonyms_store = index.synonyms;

    let mut synonyms_builder = SetBuilder::memory();
    synonyms_store.clear(writer)?;
    for (word, alternatives) in synonyms.clone() {
        synonyms_builder.insert(&word).unwrap();

        let alternatives = {
            let alternatives = SetBuf::from_dirty(alternatives);
            let mut alternatives_builder = SetBuilder::memory();
            alternatives_builder.extend_iter(alternatives).unwrap();
            let bytes = alternatives_builder.into_inner().unwrap();
            fst::Set::from_bytes(bytes).unwrap()
        };

        synonyms_store.put_synonyms(writer, word.as_bytes(), &alternatives)?;
    }

    let synonyms_set = synonyms_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    main_store.put_synonyms_fst(writer, &synonyms_set)?;
    main_store.put_synonyms(writer, synonyms)?;

    Ok(())
}

pub fn rewrite_all_documents(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    old_schema: &Schema,
    new_schema: &Schema,
) -> MResult<()> {

    let mut documents_ids_to_reindex = Vec::new();

    // Retrieve all documents present on the database
    for result in index.documents_fields_counts.documents_ids(writer)? {
        let document_id = result?;
        documents_ids_to_reindex.push(document_id);
    }

    let transpotition = diff_transposition(old_schema, new_schema);

    // Rewrite all documents one by one
    for id in documents_ids_to_reindex {
        let mut document: HashMap<SchemaAttr, Vec<u8>> = HashMap::new();

        // Retrieve the old document
        for item in index.documents_fields.document_fields(writer, id)? {
            if let Ok(item) = item {
                if let Some(pos) = transpotition[(item.0).0 as usize] {
                    // Save the current document with the new SchemaAttr
                    document.insert(SchemaAttr::new(pos), item.1.to_vec());
                }
            }
        }
        // Remove the current document
        index.documents_fields.del_all_document_fields(writer, id)?;

        // Rewrite the new document
        // TODO: use cursor to not do memory jump at each call
        for (key, value) in document {
            index.documents_fields.put_document_field(writer, id, key, &value)?;
        }
    }

    Ok(())
}
