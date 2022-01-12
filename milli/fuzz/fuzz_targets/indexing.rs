#![no_main]

use std::io::{BufWriter, Cursor, Read, Seek, Write};

use anyhow::{bail, Result};
use arbitrary_json::ArbitraryValue;
use heed::EnvOpenOptions;
use libfuzzer_sys::fuzz_target;
use milli::documents::{DocumentBatchBuilder, DocumentBatchReader};
use milli::update::UpdateBuilder;
use milli::Index;
use serde_json::Value;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// reads json from input and write an obkv batch to writer.
pub fn read_json(input: impl Read, writer: impl Write + Seek) -> Result<usize> {
    let writer = BufWriter::new(writer);
    let mut builder = DocumentBatchBuilder::new(writer)?;
    builder.extend_from_json(input)?;

    if builder.len() == 0 {
        bail!("Empty payload");
    }

    let count = builder.finish()?;

    Ok(count)
}

fn index_documents(
    index: &mut milli::Index,
    documents: DocumentBatchReader<Cursor<Vec<u8>>>,
) -> Result<()> {
    let update_builder = UpdateBuilder::new();
    let mut wtxn = index.write_txn()?;
    let builder = update_builder.index_documents(&mut wtxn, &index);

    builder.execute(documents, |_| ())?;
    wtxn.commit()?;
    Ok(())
}

fn create_index() -> Result<milli::Index> {
    let dir = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(1);
    Ok(Index::new(options, dir.path())?)
}

fuzz_target!(|batches: Vec<Vec<ArbitraryValue>>| {
    if let Ok(mut index) = create_index() {
        for batch in batches {
            let documents: Vec<Value> =
                batch.into_iter().map(|value| serde_json::Value::from(value)).collect();
            let json = Value::Array(documents);
            let json = serde_json::to_string(&json).unwrap();

            let mut documents = Cursor::new(Vec::new());

            // We ignore all badly generated documents
            if let Ok(_count) = read_json(json.as_bytes(), &mut documents) {
                let documents = DocumentBatchReader::from_reader(documents).unwrap();
                match index_documents(&mut index, documents) {
                    // Err(e @ InternalError(_) | e @ IoError(_)) => panic!("{:?}", e),
                    _ => (),
                }
            }
        }

        index.prepare_for_closing().wait();
    }
});
