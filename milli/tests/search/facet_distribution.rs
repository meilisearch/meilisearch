use std::io::Cursor;

use big_s::S;
use heed::EnvOpenOptions;
use maplit::hashset;
use milli::documents::{DocumentBatchBuilder, DocumentBatchReader};
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::{FacetDistribution, Index};

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
    builder.execute(|_| ()).unwrap();

    // index documents
    let config = IndexerConfig { max_memory: Some(10 * 1024 * 1024), ..Default::default() };
    let indexing_config = IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };

    let mut builder =
        IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ()).unwrap();
    let mut cursor = Cursor::new(Vec::new());
    let mut documents_builder = DocumentBatchBuilder::new(&mut cursor).unwrap();
    let reader = Cursor::new(
        r#"[
        {
            "id": 123,
            "title": "What a week, hu...",
            "genres": [],
            "tags": ["blue"]
        },
        {
            "id": 345,
            "title": "I am the pig!",
            "tags": ["red"]
        }
    ]"#,
    );

    for doc in serde_json::Deserializer::from_reader(reader).into_iter::<serde_json::Value>() {
        let doc = Cursor::new(serde_json::to_vec(&doc.unwrap()).unwrap());
        documents_builder.extend_from_json(doc).unwrap();
    }

    documents_builder.finish().unwrap();

    cursor.set_position(0);

    // index documents
    let content = DocumentBatchReader::from_reader(cursor).unwrap();
    builder.add_documents(content).unwrap();
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
