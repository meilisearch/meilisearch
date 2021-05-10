use heed::EnvOpenOptions;
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use crate::index::Index;
use crate::index_controller::Settings;
use std::{fs::File, path::Path, sync::Arc};

/// Extract Settings from `settings.json` file present at provided `dir_path`
fn import_settings(dir_path: &Path) -> anyhow::Result<Settings> {
    let path = dir_path.join("settings.json");
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let metadata = serde_json::from_reader(reader)?;

    Ok(metadata)
}

pub fn import_index(size: usize, dump_path: &Path, index_path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(&index_path)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(size);
    let index = milli::Index::new(options, index_path)?;
    let index = Index(Arc::new(index));

    // extract `settings.json` file and import content
    let settings = import_settings(&dump_path)?;
    let update_builder = UpdateBuilder::new(0);
    index.update_settings(&settings, update_builder)?;
    dbg!(settings);

    let update_builder = UpdateBuilder::new(1);
    let file = File::open(&dump_path.join("documents.jsonl"))?;
    let reader = std::io::BufReader::new(file);

    index.update_documents(
        UpdateFormat::JsonStream,
        IndexDocumentsMethod::ReplaceDocuments,
        Some(reader),
        update_builder,
        None,
    )?;

    // the last step: we extract the original milli::Index and close it
    Arc::try_unwrap(index.0)
        .map_err(|_e| "[dumps] At this point no one is supposed to have a reference on the index")
        .unwrap()
        .prepare_for_closing()
        .wait();

    Ok(())
}

