//! The breaking changes that happened between the v1.11 and the v1.12 are:
//! - The new indexer changed the update files format from OBKV to ndjson. https://github.com/meilisearch/meilisearch/pull/4900

use std::{io::BufWriter, path::Path};

use anyhow::Context;
use file_store::FileStore;
use indexmap::IndexMap;
use meilisearch_types::milli::documents::DocumentsBatchReader;
use serde_json::value::RawValue;
use tempfile::NamedTempFile;

pub fn v1_11_to_v1_12(db_path: &Path) -> anyhow::Result<()> {
    println!("Upgrading from v1.11.0 to v1.12.0");

    convert_update_files(db_path)?;

    Ok(())
}

/// Convert the update files from OBKV to ndjson format.
///
/// 1) List all the update files using the file store.
/// 2) For each update file, read the update file into a DocumentsBatchReader.
/// 3) For each document in the update file, convert the document to a JSON object.
/// 4) Write the JSON object to a tmp file in the update files directory.
/// 5) Persist the tmp file replacing the old update file.
fn convert_update_files(db_path: &Path) -> anyhow::Result<()> {
    let update_files_dir_path = db_path.join("update_files");
    let file_store = FileStore::new(&update_files_dir_path).with_context(|| {
        format!("while creating file store for update files dir {update_files_dir_path:?}")
    })?;

    for uuid in file_store.all_uuids().context("while retrieving uuids from file store")? {
        let uuid = uuid.context("while retrieving uuid from file store")?;
        let update_file_path = file_store.get_update_path(uuid);
        let update_file = file_store
            .get_update(uuid)
            .with_context(|| format!("while getting update file for uuid {uuid:?}"))?;

        let mut file =
            NamedTempFile::new_in(&update_files_dir_path).map(BufWriter::new).with_context(
                || format!("while creating bufwriter for update file {update_file_path:?}"),
            )?;

        let reader = DocumentsBatchReader::from_reader(update_file).with_context(|| {
            format!("while creating documents batch reader for update file {update_file_path:?}")
        })?;
        let (mut cursor, index) = reader.into_cursor_and_fields_index();

        while let Some(document) = cursor.next_document().with_context(|| {
            format!(
                "while reading documents from batch reader for update file {update_file_path:?}"
            )
        })? {
            let mut json_document = IndexMap::new();
            for (fid, value) in document {
                let field_name = index
                    .name(fid)
                    .with_context(|| format!("while getting field name for fid {fid} for update file {update_file_path:?}"))?;
                let value: &RawValue = serde_json::from_slice(value)?;
                json_document.insert(field_name, value);
            }

            serde_json::to_writer(&mut file, &json_document)?;
        }

        let file = file.into_inner().map_err(|e| e.into_error()).context(format!(
            "while flushing update file bufwriter for update file {update_file_path:?}"
        ))?;
        let _ = file
            .persist(&update_file_path)
            .with_context(|| format!("while persisting update file {update_file_path:?}"))?;
    }

    Ok(())
}
