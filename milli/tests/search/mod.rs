use milli::{Criterion, Index, DocumentId};
use milli::update::{IndexDocuments, UpdateFormat, Settings};

use big_s::S;
use heed::EnvOpenOptions;
use maplit::{hashmap, hashset};
use serde::Deserialize;
use slice_group_by::GroupBy;

mod query_criteria;

pub const TEST_QUERY: &'static str = "hello world america";

pub const EXTERNAL_DOCUMENTS_IDS: &[&str; 17] = &["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"];

pub const CONTENT: &str = include_str!("../assets/test_set.ndjson");

pub fn setup_search_index_with_criteria(criteria: &[Criterion]) -> Index {
    let path = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(10 * 1024 * 1024); // 10 MB
    let index = Index::new(options, &path).unwrap();

    let mut wtxn = index.write_txn().unwrap();

    let mut builder = Settings::new(&mut wtxn, &index, 0);

    let criteria = criteria.iter().map(|c| c.to_string()).collect();
    builder.set_criteria(criteria);
    builder.set_filterable_fields(hashset!{
        S("tag"),
        S("unexisting_field"),
        S("asc_desc_rank"),
        S("unexisting_field"),
    });
    builder.set_synonyms(hashmap!{
        S("hello") => vec![S("good morning")],
        S("world") => vec![S("earth")],
        S("america") => vec![S("the united states")],
    });
    builder.set_searchable_fields(vec![S("title"),S("description")]);
    builder.execute(|_, _| ()).unwrap();

    // index documents
    let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
    builder.update_format(UpdateFormat::JsonStream);
    builder.enable_autogenerate_docids();
    builder.execute(CONTENT.as_bytes(), |_, _| ()).unwrap();

    wtxn.commit().unwrap();

    index
}

#[allow(dead_code)]
pub fn external_to_internal_ids(index: &Index, external_ids: &[&str]) -> Vec<DocumentId> {
    let mut rtxn = index.read_txn().unwrap();
    let docid_map = index.external_documents_ids(&mut rtxn).unwrap();
    external_ids.iter().map(|id| docid_map.get(id).unwrap()).collect()
}

pub fn internal_to_external_ids(index: &Index, internal_ids: &[DocumentId]) -> Vec<String> {
    let mut rtxn = index.read_txn().unwrap();
    let docid_map = index.external_documents_ids(&mut rtxn).unwrap();
    let docid_map: std::collections::HashMap<_, _> = EXTERNAL_DOCUMENTS_IDS.iter().map(|id| (docid_map.get(id).unwrap(), id)).collect();
    internal_ids.iter().map(|id| docid_map.get(id).unwrap().to_string()).collect()
}

fn fetch_dataset() -> Vec<TestDocument> {
    serde_json::Deserializer::from_str(CONTENT).into_iter().map(|r| r.unwrap()).collect()
}

pub fn expected_order(criteria: &[Criterion], autorize_typo: bool, optional_words: bool) -> Vec<TestDocument> {
    let dataset = fetch_dataset();
    let mut groups: Vec<Vec<TestDocument>> = vec![dataset];

    for criterion in criteria {
        let mut new_groups = Vec::new();
        for group in groups.iter_mut() {
            match criterion {
                Criterion::Attribute => {
                    group.sort_by_key(|d| d.attribute_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.attribute_rank).map(Vec::from));
                },
                Criterion::Exactness => {
                    group.sort_by_key(|d| d.exact_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.exact_rank).map(Vec::from));
                },
                Criterion::Proximity => {
                    group.sort_by_key(|d| d.proximity_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.proximity_rank).map(Vec::from));
                },
                Criterion::Typo => {
                    group.sort_by_key(|d| d.typo_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.typo_rank).map(Vec::from));
                },
                Criterion::Words => {
                    group.sort_by_key(|d| d.word_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.word_rank).map(Vec::from));
                },
                Criterion::Asc(_) => {
                    group.sort_by_key(|d| d.asc_desc_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.asc_desc_rank).map(Vec::from));
                },
                Criterion::Desc(_) => {
                    group.sort_by_key(|d| std::cmp::Reverse(d.asc_desc_rank));
                    new_groups.extend(group.linear_group_by_key(|d| d.asc_desc_rank).map(Vec::from));
                },
            }
        }
        groups = std::mem::take(&mut new_groups);
    }

    if autorize_typo && optional_words {
        groups.into_iter().flatten().collect()
    } else if optional_words {
        groups.into_iter().flatten().filter(|d| d.typo_rank == 0).collect()
    } else if autorize_typo {
        groups.into_iter().flatten().filter(|d| d.word_rank == 0).collect()
    } else {
        groups.into_iter().flatten().filter(|d| d.word_rank == 0 && d.typo_rank == 0).collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestDocument {
    pub id: String,
    pub word_rank: u32,
    pub typo_rank: u32,
    pub proximity_rank: u32,
    pub attribute_rank: u32,
    pub exact_rank: u32,
    pub asc_desc_rank: u32,
    pub title: String,
    pub description: String,
    pub tag: String,
}
