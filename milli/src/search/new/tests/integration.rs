use std::io::Cursor;

use big_s::S;
use heed::EnvOpenOptions;
use maplit::{btreemap, hashset};

use crate::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use crate::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use crate::{db_snap, Criterion, Index, Object};
pub const CONTENT: &str = include_str!("../../../../tests/assets/test_set.ndjson");

pub fn setup_search_index_with_criteria(criteria: &[Criterion]) -> Index {
    let path = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(10 * 1024 * 1024); // 10 MB
    let index = Index::new(options, &path).unwrap();

    let mut wtxn = index.write_txn().unwrap();
    let config = IndexerConfig::default();

    let mut builder = Settings::new(&mut wtxn, &index, &config);

    builder.set_criteria(criteria.to_vec());
    builder.set_filterable_fields(hashset! {
        S("tag"),
        S("asc_desc_rank"),
        S("_geo"),
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

    // index documents
    let config = IndexerConfig { max_memory: Some(10 * 1024 * 1024), ..Default::default() };
    let indexing_config = IndexDocumentsConfig::default();

    let builder =
        IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false).unwrap();
    let mut documents_builder = DocumentsBatchBuilder::new(Vec::new());
    let reader = Cursor::new(CONTENT.as_bytes());

    for result in serde_json::Deserializer::from_reader(reader).into_iter::<Object>() {
        let object = result.unwrap();
        documents_builder.append_json_object(&object).unwrap();
    }

    let vector = documents_builder.into_inner().unwrap();

    // index documents
    let content = DocumentsBatchReader::from_reader(Cursor::new(vector)).unwrap();
    let (builder, user_error) = builder.add_documents(content).unwrap();
    user_error.unwrap();
    builder.execute().unwrap();

    wtxn.commit().unwrap();

    index
}

#[test]
fn snapshot_integration_dataset() {
    let index = setup_search_index_with_criteria(&[Criterion::Attribute]);
    db_snap!(index, word_position_docids, @"3c9347a767bceef3beb31465f1e5f3ae");
}
