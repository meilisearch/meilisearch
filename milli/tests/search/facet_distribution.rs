use std::io::Cursor;

use big_s::S;
use heed::EnvOpenOptions;
use maplit::hashset;
use milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::{FacetDistribution, Index, Object};
use serde_json::Deserializer;

#[test]
fn test_facet_distribution_with_no_facet_values() {
    let path = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(10 * 1024 * 1024); // 10 MB
    let index = Index::new(options, &path).unwrap();

    let mut wtxn = index.write_txn().unwrap();
    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut wtxn, &index, &config);

    builder.set_filterable_fields(hashset! {
        S("genres"),
        S("tags"),
    });
    builder.execute(|_| (), || false).unwrap();

    // index documents
    let config = IndexerConfig { max_memory: Some(10 * 1024 * 1024), ..Default::default() };
    let indexing_config = IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };

    let builder =
        IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false).unwrap();
    let mut documents_builder = DocumentsBatchBuilder::new(Vec::new());
    let reader = Cursor::new(
        r#"{
            "id": 123,
            "title": "What a week, hu...",
            "genres": [],
            "tags": ["blue"]
        }
        {
            "id": 345,
            "title": "I am the pig!",
            "tags": ["red"]
        }"#,
    );

    for result in Deserializer::from_reader(reader).into_iter::<Object>() {
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

    let txn = index.read_txn().unwrap();
    let mut distrib = FacetDistribution::new(&txn, &index);
    distrib.facets(vec!["genres"]);
    let result = distrib.execute().unwrap();
    assert_eq!(result["genres"].len(), 0);

    let mut distrib = FacetDistribution::new(&txn, &index);
    distrib.facets(vec!["tags"]);
    let result = distrib.execute().unwrap();
    assert_eq!(result["tags"].len(), 2);
}
