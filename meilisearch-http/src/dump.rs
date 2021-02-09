use std::fs::{create_dir_all, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::thread;

use actix_web::web;
use chrono::offset::Utc;
use indexmap::IndexMap;
use log::{error, info};
use meilisearch_core::{MainWriter, MainReader, UpdateReader};
use meilisearch_core::settings::Settings;
use meilisearch_core::update::{apply_settings_update, apply_documents_addition};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;

use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::compression;
use crate::routes::index;
use crate::routes::index::IndexResponse;

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum DumpVersion {
    V1,
}

impl DumpVersion {
    const CURRENT: Self = Self::V1;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DumpMetadata {
    indexes: Vec<crate::routes::index::IndexResponse>,
    db_version: String,
    dump_version: DumpVersion,
}

impl DumpMetadata {
    /// Create a DumpMetadata with the current dump version of meilisearch.
    pub fn new(indexes: Vec<crate::routes::index::IndexResponse>, db_version: String) -> Self {
        DumpMetadata {
            indexes,
            db_version,
            dump_version: DumpVersion::CURRENT,
        }
    }

    /// Extract DumpMetadata from `metadata.json` file present at provided `dir_path`
    fn from_path(dir_path: &Path) -> Result<Self, Error> {
        let path = dir_path.join("metadata.json");
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let metadata = serde_json::from_reader(reader)?;

        Ok(metadata)
    }

    /// Write DumpMetadata in `metadata.json` file at provided `dir_path`
    fn to_path(&self, dir_path: &Path) -> Result<(), Error> {
        let path = dir_path.join("metadata.json");
        let file = File::create(path)?;

        serde_json::to_writer(file, &self)?;

        Ok(())
    }
}

/// Extract Settings from `settings.json` file present at provided `dir_path`
fn settings_from_path(dir_path: &Path) -> Result<Settings, Error> {
    let path = dir_path.join("settings.json");
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let metadata = serde_json::from_reader(reader)?;

    Ok(metadata)
}

/// Write Settings in `settings.json` file at provided `dir_path`
fn settings_to_path(settings: &Settings, dir_path: &Path) -> Result<(), Error> {
    let path = dir_path.join("settings.json");
    let file = File::create(path)?;

    serde_json::to_writer(file, settings)?;

    Ok(())
}

/// Import settings and documents of a dump with version `DumpVersion::V1` in specified index.
fn import_index_v1(
    data: &Data,
    dumps_dir: &Path,
    index_uid: &str,
    document_batch_size: usize,
    write_txn: &mut MainWriter,
) -> Result<(), Error> {

    // open index
    let index = data
        .db
        .open_index(index_uid)
        .ok_or(Error::index_not_found(index_uid))?;

    // index dir path in  dump dir
    let index_path = &dumps_dir.join(index_uid);

    // extract `settings.json` file and import content
    let settings = settings_from_path(&index_path)?;
    let settings = settings.to_update().map_err(|e| Error::dump_failed(format!("importing settings for index {}; {}", index_uid, e)))?;
    apply_settings_update(write_txn, &index, settings)?;

    // create iterator over documents in `documents.jsonl` to make batch importation
    // create iterator over documents in `documents.jsonl` to make batch importation
    let documents = {
        let file = File::open(&index_path.join("documents.jsonl"))?;
        let reader = std::io::BufReader::new(file);
        let deserializer = serde_json::Deserializer::from_reader(reader);
        deserializer.into_iter::<IndexMap<String, serde_json::Value>>()
    };

    // batch import document every `document_batch_size`:
    // create a Vec to bufferize documents
    let mut values = Vec::with_capacity(document_batch_size);
    // iterate over documents
    for document in documents {
        // push document in buffer
        values.push(document?);
        // if buffer is full, create and apply a batch, and clean buffer
        if values.len() == document_batch_size {
            let batch = std::mem::replace(&mut values, Vec::with_capacity(document_batch_size));
            apply_documents_addition(write_txn, &index, batch, None)?;
        }
    }

    // apply documents remaining in the buffer
    if !values.is_empty() {
        apply_documents_addition(write_txn, &index, values, None)?;
    }

    // sync index information: stats, updated_at, last_update
    if let Err(e) = crate::index_update_callback_txn(index, index_uid, data, write_txn) {
        return Err(Error::Internal(e));
    }

    Ok(())
}

/// Import dump from `dump_path` in database.
pub fn import_dump(
    data: &Data,
    dump_path: &Path,
    document_batch_size: usize,
) -> Result<(), Error> {
    info!("Importing dump from {:?}...", dump_path);

    // create a temporary directory
    let tmp_dir = TempDir::new()?;
    let tmp_dir_path = tmp_dir.path();

    // extract dump in temporary directory
    compression::from_tar_gz(dump_path, tmp_dir_path)?;

    // read dump metadata
    let metadata = DumpMetadata::from_path(&tmp_dir_path)?;

    // choose importation function from DumpVersion of metadata
    let import_index = match metadata.dump_version {
        DumpVersion::V1 => import_index_v1,
    };

    // remove indexes which have same `uid` than indexes to import and create empty indexes
    let existing_index_uids = data.db.indexes_uids();
    for index in metadata.indexes.iter() {
        if existing_index_uids.contains(&index.uid) {
            data.db.delete_index(index.uid.clone())?;
        }
        index::create_index_sync(&data.db, index.uid.clone(), index.name.clone(), index.primary_key.clone())?;
    }

    // import each indexes content
    data.db.main_write::<_, _, Error>(|mut writer| {
        for index in metadata.indexes {
            import_index(&data, tmp_dir_path, &index.uid, document_batch_size, &mut writer)?;
        }
        Ok(())
    })?;

    info!("Dump importation from {:?} succeed", dump_path);
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DumpStatus {
    Done,
    InProgress,
    Failed,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DumpInfo {
    pub uid: String,
    pub status: DumpStatus,
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub error: Option<serde_json::Value>,

}

impl DumpInfo {
    pub fn new(uid: String, status: DumpStatus) -> Self {
        Self { uid, status, error: None }
    }

    pub fn with_error(mut self, error: ResponseError) -> Self {
        self.status = DumpStatus::Failed;
        self.error = Some(json!(error));

        self
    }

    pub fn dump_already_in_progress(&self) -> bool {
        self.status == DumpStatus::InProgress
    }
}

/// Generate uid from creation date
fn generate_uid() -> String {
    Utc::now().format("%Y%m%d-%H%M%S%3f").to_string()
}

/// Infer dumps_dir from dump_uid
pub fn compressed_dumps_dir(dumps_dir: &Path, dump_uid: &str) -> PathBuf {
    dumps_dir.join(format!("{}.dump", dump_uid))
}

/// Write metadata in dump
fn dump_metadata(data: &web::Data<Data>, dir_path: &Path, indexes: Vec<IndexResponse>) -> Result<(), Error> {
    let (db_major, db_minor, db_patch) = data.db.version();
    let metadata = DumpMetadata::new(indexes, format!("{}.{}.{}", db_major, db_minor, db_patch));

    metadata.to_path(dir_path)
}

/// Export settings of provided index in dump
fn dump_index_settings(data: &web::Data<Data>, reader: &MainReader, dir_path: &Path, index_uid: &str) -> Result<(), Error> {
    let settings = crate::routes::setting::get_all_sync(data, reader, index_uid)?;

    settings_to_path(&settings, dir_path)
}

/// Export updates of provided index in dump
fn dump_index_updates(data: &web::Data<Data>, reader: &UpdateReader, dir_path: &Path, index_uid: &str) -> Result<(), Error> {
    let updates_path = dir_path.join("updates.jsonl");
    let updates = crate::routes::index::get_all_updates_status_sync(data, reader, index_uid)?;

    let file = File::create(updates_path)?;

    for update in updates {
        serde_json::to_writer(&file, &update)?;
        writeln!(&file)?;
    }

    Ok(())
}

/// Export documents of provided index in dump
fn dump_index_documents(data: &web::Data<Data>, reader: &MainReader, dir_path: &Path, index_uid: &str) -> Result<(), Error> {
    let documents_path = dir_path.join("documents.jsonl");
    let file = File::create(documents_path)?;
    let dump_batch_size = data.dump_batch_size;

    let mut offset = 0;
    loop {
        let documents = crate::routes::document::get_all_documents_sync(data, reader, index_uid, offset, dump_batch_size, None)?;
        if documents.is_empty() { break; } else { offset += dump_batch_size; }

        for document in documents {
            serde_json::to_writer(&file, &document)?;
            writeln!(&file)?;
        }
    }

    Ok(())
}

/// Write error with a context.
fn fail_dump_process<E: std::error::Error>(data: &web::Data<Data>, dump_info: DumpInfo, context: &str, error: E) {
        let error_message = format!("{}; {}", context, error);
        error!("Something went wrong during dump process: {}", &error_message);
        data.set_current_dump_info(dump_info.with_error(Error::dump_failed(error_message).into()))
}

/// Main function of dump.
fn dump_process(data: web::Data<Data>, dumps_dir: PathBuf, dump_info: DumpInfo) {
    // open read transaction on Update
    let update_reader = match data.db.update_read_txn() {
        Ok(r) => r,
        Err(e) => {
            fail_dump_process(&data, dump_info, "creating RO transaction on updates", e);
            return ;
        }
    };

    // open read transaction on Main
    let main_reader = match data.db.main_read_txn() {
        Ok(r) => r,
        Err(e) => {
            fail_dump_process(&data, dump_info, "creating RO transaction on main", e);
            return ;
        }
    };

    // create a temporary directory
    let tmp_dir = match TempDir::new() {
        Ok(tmp_dir) => tmp_dir,
        Err(e) => {
            fail_dump_process(&data, dump_info, "creating temporary directory", e);
            return ;
        }
    };
    let tmp_dir_path = tmp_dir.path();

    // fetch indexes
    let indexes = match crate::routes::index::list_indexes_sync(&data, &main_reader) {
        Ok(indexes) => indexes,
        Err(e) => {
            fail_dump_process(&data, dump_info, "listing indexes", e);
            return ;
        }
    };

    // create metadata
    if let Err(e) = dump_metadata(&data, &tmp_dir_path, indexes.clone()) {
        fail_dump_process(&data, dump_info, "generating metadata", e);
        return ;
    }

    // export settings, updates and documents for each indexes
    for index in indexes {
        let index_path = tmp_dir_path.join(&index.uid);

        // create index sub-dircetory
        if let Err(e) = create_dir_all(&index_path) {
            fail_dump_process(&data, dump_info, &format!("creating directory for index {}", &index.uid), e);
            return ;
        }

        // export settings
        if let Err(e) = dump_index_settings(&data, &main_reader, &index_path, &index.uid) {
            fail_dump_process(&data, dump_info, &format!("generating settings for index {}", &index.uid), e);
            return ;
        }

        // export documents
        if let Err(e) = dump_index_documents(&data, &main_reader, &index_path, &index.uid) {
            fail_dump_process(&data, dump_info, &format!("generating documents for index {}", &index.uid), e);
            return ;
        }

        // export updates
        if let Err(e) = dump_index_updates(&data, &update_reader, &index_path, &index.uid) {
            fail_dump_process(&data, dump_info, &format!("generating updates for index {}", &index.uid), e);
            return ;
        }
    }

    // compress dump in a file named `{dump_uid}.dump` in `dumps_dir`
    if let Err(e) = crate::helpers::compression::to_tar_gz(&tmp_dir_path, &compressed_dumps_dir(&dumps_dir, &dump_info.uid)) {
        fail_dump_process(&data, dump_info, "compressing dump", e);
        return ;
    }

    // update dump info to `done`
    let resume = DumpInfo::new(
        dump_info.uid,
        DumpStatus::Done
    );

    data.set_current_dump_info(resume);
}

pub fn init_dump_process(data: &web::Data<Data>, dumps_dir: &Path) -> Result<DumpInfo, Error> {
    create_dir_all(dumps_dir).map_err(|e| Error::dump_failed(format!("creating temporary directory {}", e)))?;

    // check if a dump is already in progress
    if let Some(resume) = data.get_current_dump_info() {
        if resume.dump_already_in_progress() {
            return Err(Error::dump_conflict())
        }
    }

    // generate a new dump info
    let info = DumpInfo::new(
        generate_uid(),
        DumpStatus::InProgress
    );

    data.set_current_dump_info(info.clone());

    let data = data.clone();
    let dumps_dir = dumps_dir.to_path_buf();
    let info_cloned = info.clone();
    // run dump process in a new thread
    thread::spawn(move ||
        dump_process(data, dumps_dir, info_cloned)
    );

    Ok(info)
}
