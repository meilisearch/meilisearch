use big_s::S;
use bumpalo::Bump;
use heed::EnvOpenOptions;
use milli::documents::mmap_from_objects;
use milli::progress::Progress;
use milli::update::new::indexer;
use milli::update::{IndexerConfig, Settings};
use milli::vector::RuntimeEmbedders;
use milli::{FacetDistribution, FilterableAttributesRule, Index, Object, OrderBy};
use serde_json::{from_value, json};

#[test]
fn test_facet_distribution_with_no_facet_values() {
    let path = tempfile::tempdir().unwrap();
    let options = EnvOpenOptions::new();
    let mut options = options.read_txn_without_tls();
    options.map_size(10 * 1024 * 1024); // 10 MB
    let index = Index::new(options, &path, true).unwrap();

    let mut wtxn = index.write_txn().unwrap();
    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut wtxn, &index, &config);

    builder.set_filterable_fields(vec![
        FilterableAttributesRule::Field(S("genres")),
        FilterableAttributesRule::Field(S("tags")),
    ]);
    builder.execute(&|| false, &Progress::default(), Default::default()).unwrap();
    wtxn.commit().unwrap();

    // index documents
    let config = IndexerConfig { max_memory: Some(10 * 1024 * 1024), ..Default::default() };
    let rtxn = index.read_txn().unwrap();
    let mut wtxn = index.write_txn().unwrap();
    let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let mut new_fields_ids_map = db_fields_ids_map.clone();

    let embedders = RuntimeEmbedders::default();
    let mut indexer = indexer::DocumentOperation::new();

    let doc1: Object = from_value(
        json!({ "id": 123, "title": "What a week, hu...", "genres": [], "tags": ["blue"] }),
    )
    .unwrap();
    let doc2: Object =
        from_value(json!({ "id": 345, "title": "I am the pig!", "tags": ["red"] })).unwrap();
    let documents = mmap_from_objects(vec![doc1, doc2]);

    // index documents
    indexer.replace_documents(&documents).unwrap();

    let indexer_alloc = Bump::new();
    let (document_changes, _operation_stats, primary_key) = indexer
        .into_changes(
            &indexer_alloc,
            &index,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
            None,
        )
        .unwrap();

    indexer::index(
        &mut wtxn,
        &index,
        &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
        config.grenad_parameters(),
        &db_fields_ids_map,
        new_fields_ids_map,
        primary_key,
        &document_changes,
        embedders,
        &|| false,
        &Progress::default(),
        &Default::default(),
    )
    .unwrap();

    wtxn.commit().unwrap();

    let rtxn = index.read_txn().unwrap();
    let mut distrib = FacetDistribution::new(&rtxn, &index);
    distrib.facets(vec![("genres", OrderBy::default())]);
    let result = distrib.execute().unwrap();
    assert_eq!(result["genres"].len(), 0);

    let mut distrib = FacetDistribution::new(&rtxn, &index);
    distrib.facets(vec![("tags", OrderBy::default())]);
    let result = distrib.execute().unwrap();
    assert_eq!(result["tags"].len(), 2);
}
