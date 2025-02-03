use std::io::Write;

use big_s::S;
use bumpalo::Bump;
use heed::EnvOpenOptions;
use maplit::{btreemap, hashset};

use crate::progress::Progress;
use crate::update::new::indexer;
use crate::update::{IndexerConfig, Settings};
use crate::vector::EmbeddingConfigs;
use crate::{db_snap, Criterion, Index};
pub const CONTENT: &str = include_str!("../../../../tests/assets/test_set.ndjson");
use crate::constants::RESERVED_GEO_FIELD_NAME;

pub fn setup_search_index_with_criteria(criteria: &[Criterion]) -> Index {
    let path = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(10 * 1024 * 1024); // 10 MB
    let index = Index::new(options, &path, true).unwrap();

    let mut wtxn = index.write_txn().unwrap();
    let config = IndexerConfig::default();

    let mut builder = Settings::new(&mut wtxn, &index, &config);

    builder.set_criteria(criteria.to_vec());
    builder.set_filterable_fields(hashset! {
        S("tag"),
        S("asc_desc_rank"),
        S(RESERVED_GEO_FIELD_NAME),
        S("opt1"),
        S("opt1.opt2"),
        S("tag_in")
    });
    builder.set_sortable_fields(hashset! {
        S("tag"),
        S("asc_desc_rank"),
    });
    builder.set_synonyms(btreemap! {
        S("hello") => vec![S("good morning")],
        S("world") => vec![S("earth")],
        S("america") => vec![S("the united states")],
    });
    builder.set_searchable_fields(vec![S("title"), S("description")]);
    builder.execute(|_| (), || false).unwrap();
    wtxn.commit().unwrap();

    // index documents
    let config = IndexerConfig { max_memory: Some(10 * 1024 * 1024), ..Default::default() };
    let rtxn = index.read_txn().unwrap();
    let mut wtxn = index.write_txn().unwrap();

    let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let mut new_fields_ids_map = db_fields_ids_map.clone();

    let embedders = EmbeddingConfigs::default();
    let mut indexer = indexer::DocumentOperation::new();

    let mut file = tempfile::tempfile().unwrap();
    file.write_all(CONTENT.as_bytes()).unwrap();
    file.sync_all().unwrap();
    let payload = unsafe { memmap2::Mmap::map(&file).unwrap() };

    // index documents
    indexer.replace_documents(&payload).unwrap();

    let indexer_alloc = Bump::new();
    let (document_changes, operation_stats, primary_key) = indexer
        .into_changes(
            &indexer_alloc,
            &index,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
        )
        .unwrap();

    if let Some(error) = operation_stats.into_iter().find_map(|stat| stat.error) {
        panic!("{error}");
    }

    indexer::index(
        &mut wtxn,
        &index,
        &crate::ThreadPoolNoAbortBuilder::new().build().unwrap(),
        config.grenad_parameters(),
        &db_fields_ids_map,
        new_fields_ids_map,
        primary_key,
        &document_changes,
        embedders,
        &|| false,
        &Progress::default(),
    )
    .unwrap();

    wtxn.commit().unwrap();
    drop(rtxn);

    index
}

#[test]
fn snapshot_integration_dataset() {
    let index = setup_search_index_with_criteria(&[Criterion::Attribute]);
    db_snap!(index, word_position_docids, @"3c9347a767bceef3beb31465f1e5f3ae");
}
