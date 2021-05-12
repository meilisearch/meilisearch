use heed::EnvOpenOptions;
use log::info;
use uuid::Uuid;
use crate::{index::Unchecked, index_controller::{UpdateStatus, update_actor::UpdateStore}};
use std::io::BufRead;
use milli::{update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat}};
use crate::index::{Checked, Index};
use crate::index_controller::Settings;
use std::{fs::File, path::Path, sync::Arc};

/// Extract Settings from `settings.json` file present at provided `dir_path`
fn import_settings(dir_path: &Path) -> anyhow::Result<Settings<Checked>> {
    let path = dir_path.join("settings.json");
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let metadata: Settings<Unchecked> = serde_json::from_reader(reader)?;

    println!("Meta: {:?}", metadata);

    Ok(metadata.check())
}

pub fn import_index(size: usize, uuid: Uuid, dump_path: &Path, db_path: &Path, primary_key: Option<&str>) -> anyhow::Result<()> {
    let index_path = db_path.join(&format!("indexes/index-{}", uuid));
    std::fs::create_dir_all(&index_path)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(size);
    let index = milli::Index::new(options, index_path)?;
    let index = Index(Arc::new(index));

    let mut txn = index.write_txn()?;

    info!("importing the settings...");
    // extract `settings.json` file and import content
    let settings = import_settings(&dump_path)?;
    let update_builder = UpdateBuilder::new(0);
    index.update_settings_txn(&mut txn, &settings, update_builder)?;

    // import the documents in the index
    let update_builder = UpdateBuilder::new(1);
    let file = File::open(&dump_path.join("documents.jsonl"))?;
    let reader = std::io::BufReader::new(file);

    info!("importing the documents...");
    // TODO: TAMO: currently we ignore any error caused by the importation of the documents because
    // if there is no documents nor primary key it'll throw an anyhow error, but we must remove
    // this before the merge on main
    index.update_documents_txn(
        &mut txn,
        UpdateFormat::JsonStream,
        IndexDocumentsMethod::ReplaceDocuments,
        Some(reader),
        update_builder,
        primary_key,
    )?;

    txn.commit()?;

    // the last step: we extract the original milli::Index and close it
    Arc::try_unwrap(index.0)
        .map_err(|_e| "[dumps] At this point no one is supposed to have a reference on the index")
        .unwrap()
        .prepare_for_closing()
        .wait();

    info!("importing the updates...");
    import_updates(uuid, dump_path, db_path)
}

fn import_updates(uuid: Uuid, dump_path: &Path, db_path: &Path) -> anyhow::Result<()> {
        let update_path = db_path.join("updates");
        let options = EnvOpenOptions::new();
        // create an UpdateStore to import the updates
        std::fs::create_dir_all(&update_path)?;
        let (update_store, _) = UpdateStore::create(options, &update_path)?;
        let file = File::open(&dump_path.join("updates.jsonl"))?;
        let reader = std::io::BufReader::new(file);

        let mut wtxn = update_store.env.write_txn()?;
        for update in reader.lines() {
            let mut update: UpdateStatus = serde_json::from_str(&update?)?;
            if let Some(path) = update.content_path_mut() {
                *path = update_path.join("update_files").join(&path);
            }
            update_store.register_raw_updates(&mut wtxn, update, uuid)?;
        }
        wtxn.commit()?;
    Ok(())
}
