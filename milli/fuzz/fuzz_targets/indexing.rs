#![no_main]

use std::collections::HashSet;
use std::io::{BufWriter, Cursor, Read, Seek, Write};

use anyhow::{bail, Result};
use arbitrary_json::ArbitraryValue;
use heed::EnvOpenOptions;
use libfuzzer_sys::fuzz_target;
use milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::Index;
use serde_json::{Map, Value};

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// reads json from input and write an obkv batch to writer.
pub fn read_json(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let writer = BufWriter::new(writer);
    let mut builder = DocumentsBatchBuilder::new(writer);

    let values: Vec<Map<String, Value>> = serde_json::from_reader(input)?;
    if builder.documents_count() == 0 {
        bail!("Empty payload");
    }

    for object in values {
        builder.append_json_object(&object)?;
    }

    let count = builder.documents_count();
    let vector = builder.into_inner()?;

    Ok(count as usize)
}

fn index_documents(
    index: &mut milli::Index,
    documents: DocumentsBatchReader<Cursor<Vec<u8>>>,
) -> Result<()> {
    let config = IndexerConfig::default();
    let mut wtxn = index.write_txn()?;

    let indexing_config = IndexDocumentsConfig::default();
    let mut builder = IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ())?;
    builder.add_documents(documents)?;
    builder.execute().unwrap();

    wtxn.commit()?;
    Ok(())
}

fn create_index() -> Result<milli::Index> {
    let dir = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(10 * 1024 * 1024 * 1024); // 10 GB
    options.max_readers(1);
    let index = Index::new(options, dir.path())?;

    let config = IndexerConfig::default();
    let mut wtxn = index.write_txn().unwrap();

    let mut builder = Settings::new(&mut wtxn, &index, &config);

    let displayed_fields =
        ["id", "title", "album", "artist", "genre", "country", "released", "duration"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_displayed_fields(displayed_fields);

    let searchable_fields = ["title", "album", "artist"].iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);

    let faceted_fields: HashSet<String> =
        ["released-timestamp", "duration-float", "genre", "country", "artist"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_filterable_fields(faceted_fields.clone());
    builder.set_sortable_fields(faceted_fields);

    builder.set_distinct_field("same".to_string());

    builder.execute(|_| ()).unwrap();
    wtxn.commit().unwrap();

    Ok(index)
}

fuzz_target!(|batches: Vec<Vec<ArbitraryValue>>| {
    if let Ok(mut index) = create_index() {
        for batch in batches {
            let documents: Vec<Value> =
                batch.into_iter().map(|value| serde_json::Value::from(value)).collect();
            let json = Value::Array(documents);
            let json = serde_json::to_string(&json).unwrap();

            let mut documents = Cursor::new(Vec::new());

            // We ignore all malformed documents
            if let Ok(_) = read_json(json.as_bytes(), &mut documents) {
                documents.rewind().unwrap();
                let documents = DocumentsBatchReader::from_reader(documents).unwrap();
                // A lot of errors can come out of milli and we don't know which ones are normal or not
                // so we are only going to look for the unexpected panics.
                let _ = index_documents(&mut index, documents);
            }
        }

        index.prepare_for_closing().wait();
    }
});
