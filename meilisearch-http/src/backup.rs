use std::fs::{create_dir_all, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;

use actix_web::web;
use chrono::offset::Utc;
use indexmap::IndexMap;
use log::error;
use meilisearch_core::{MainWriter, MainReader, UpdateReader};
use meilisearch_core::settings::Settings;
use meilisearch_core::update::{apply_settings_update, apply_documents_addition};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::Data;
use crate::error::Error;
use crate::helpers::compression;
use crate::routes::index;
use crate::routes::index::IndexResponse;

// Mutex to share backup progress.
static BACKUP_INFO: Lazy<Mutex<Option<BackupInfo>>> = Lazy::new(Mutex::default);

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum BackupVersion {
    V1,
}

impl BackupVersion {
    const CURRENT: Self = Self::V1;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupMetadata {
    indexes: Vec<crate::routes::index::IndexResponse>,
    db_version: String,
    backup_version: BackupVersion,
}

impl BackupMetadata {
    /// Create a BackupMetadata with the current backup version of meilisearch.
    pub fn new(indexes: Vec<crate::routes::index::IndexResponse>, db_version: String) -> Self {
        BackupMetadata {
            indexes,
            db_version,
            backup_version: BackupVersion::CURRENT,
        }
    }

    /// Extract BackupMetadata from `metadata.json` file present at provided `folder_path`
    fn from_path(folder_path: &Path) -> Result<Self, Error> {
        let path = folder_path.join("metadata.json");
        let file = File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let metadata = serde_json::from_reader(reader)?;

        Ok(metadata)
    }

    /// Write BackupMetadata in `metadata.json` file at provided `folder_path`
    fn to_path(&self, folder_path: &Path) -> Result<(), Error> {
        let path = folder_path.join("metadata.json");
        let file = File::create(path)?;

        serde_json::to_writer(file, &self)?;

        Ok(())
    }
}

/// Extract Settings from `settings.json` file present at provided `folder_path`
fn settings_from_path(folder_path: &Path) -> Result<Settings, Error> {
    let path = folder_path.join("settings.json");
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let metadata = serde_json::from_reader(reader)?;

    Ok(metadata)
}

/// Write Settings in `settings.json` file at provided `folder_path`
fn settings_to_path(settings: &Settings, folder_path: &Path) -> Result<(), Error> {
    let path = folder_path.join("settings.json");
    let file = File::create(path)?;

    serde_json::to_writer(file, settings)?;

    Ok(())
}

/// Import settings and documents of a backup with version `BackupVersion::V1` in specified index.
fn import_index_v1(
    data: &Data,
    backup_folder: &Path,
    index_uid: &str,
    document_batch_size: usize,
    write_txn: &mut MainWriter,
) -> Result<(), Error> {

    // open index
    let index = data
        .db
        .open_index(index_uid)
        .ok_or(Error::index_not_found(index_uid))?;

    // index folder path in  backup folder
    let index_path = &backup_folder.join(index_uid);

    // extract `settings.json` file and import content
    let settings = settings_from_path(&index_path)?;
    let settings = settings.to_update().or_else(|_e| Err(Error::backup_failed()))?;
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
            apply_documents_addition(write_txn, &index, batch)?;
        }
    }

    // apply documents remaining in the buffer 
    if !values.is_empty() { 
        apply_documents_addition(write_txn, &index, values)?;
    }

    Ok(())
}

/// Import backup from `backup_folder` in database.
pub fn import_backup(
    data: &Data,
    backup_folder: &Path,
    document_batch_size: usize,
) -> Result<(), Error> {
    // create a temporary directory
    let tmp_dir = TempDir::new()?;
    let tmp_dir_path = tmp_dir.path();

    // extract backup in temporary directory
    compression::from_tar_gz(backup_folder, tmp_dir_path)?;

    // read backup metadata
    let metadata = BackupMetadata::from_path(&tmp_dir_path)?;

    // choose importation function from BackupVersion of metadata
    let import_index = match metadata.backup_version {
        BackupVersion::V1 => import_index_v1,
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

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum BackupStatus {
    Done,
    Processing,
    BackupProcessFailed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BackupInfo {
    pub uid: String,
    pub status: BackupStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl BackupInfo {
    pub fn new(uid: String, status: BackupStatus) -> Self {
        Self { uid, status, error: None }
    }

    pub fn with_error(mut self, error: String) -> Self {
        self.status = BackupStatus::BackupProcessFailed;
        self.error = Some(error);

        self
    }

    pub fn backup_already_in_progress(&self) -> bool {
        self.status == BackupStatus::Processing
    }

    pub fn get_current() -> Option<Self> {
        BACKUP_INFO.lock().unwrap().clone()
    }

    pub fn set_current(&self) {
        *BACKUP_INFO.lock().unwrap() = Some(self.clone());
    }
}

/// Generate uid from creation date
fn generate_uid() -> String {
    Utc::now().format("%Y%m%d-%H%M%S").to_string()
}

/// Infer backup_folder from backup_uid
pub fn compressed_backup_folder(backup_folder: &Path, backup_uid: &str) -> PathBuf {
    backup_folder.join(format!("{}.tar.gz", backup_uid))
}

/// Write metadata in backup
fn backup_metadata(data: &web::Data<Data>, folder_path: &Path, indexes: Vec<IndexResponse>) -> Result<(), Error> {
    let (db_major, db_minor, db_patch) = data.db.version();
    let metadata = BackupMetadata::new(indexes, format!("{}.{}.{}", db_major, db_minor, db_patch));

    metadata.to_path(folder_path)
}

/// Export settings of provided index in backup
fn backup_index_settings(data: &web::Data<Data>, reader: &MainReader, folder_path: &Path, index_uid: &str) -> Result<(), Error> {
    let settings = crate::routes::setting::get_all_sync(data, reader, index_uid)?;

    settings_to_path(&settings, folder_path)
}

/// Export updates of provided index in backup
fn backup_index_updates(data: &web::Data<Data>, reader: &UpdateReader, folder_path: &Path, index_uid: &str) -> Result<(), Error> {
    let updates_path = folder_path.join("updates.jsonl");
    let updates = crate::routes::index::get_all_updates_status_sync(data, reader, index_uid)?;

    let file = File::create(updates_path)?;

    for update in updates {
        serde_json::to_writer(&file, &update)?;
        writeln!(&file)?;
    }

    Ok(())
}

/// Export documents of provided index in backup
fn backup_index_documents(data: &web::Data<Data>, reader: &MainReader, folder_path: &Path, index_uid: &str) -> Result<(), Error> {
    let documents_path = folder_path.join("documents.jsonl");
    let file = File::create(documents_path)?;
    let backup_batch_size = data.backup_batch_size;

    let mut offset = 0;
    loop {
        let documents = crate::routes::document::get_all_documents_sync(data, reader, index_uid, offset, backup_batch_size, None)?;
        if documents.len() == 0 { break; } else { offset += backup_batch_size; }

        for document in documents {
            serde_json::to_writer(&file, &document)?;
            writeln!(&file)?;
        }
    }

    Ok(())
}

/// Write error with a context.
fn fail_backup_process<E: std::error::Error>(backup_info: BackupInfo, context: &str, error: E) {
        let error = format!("Something went wrong during backup process: {}; {}", context, error);
        
        error!("{}", &error);
        backup_info.with_error(error).set_current();
}

/// Main function of backup.
fn backup_process(data: web::Data<Data>, backup_folder: PathBuf, backup_info: BackupInfo) {
    // open read transaction on Update
    let update_reader = match data.db.update_read_txn() {
        Ok(r) => r,
        Err(e) => {
            fail_backup_process(backup_info, "creating RO transaction on updates", e);
            return ;
        }
    };

    // open read transaction on Main
    let main_reader = match data.db.main_read_txn() {
        Ok(r) => r,
        Err(e) => {
            fail_backup_process(backup_info, "creating RO transaction on main", e);
            return ;
        }
    };

    // create a temporary directory
    let tmp_dir = match TempDir::new() {
        Ok(tmp_dir) => tmp_dir,
        Err(e) => {
            fail_backup_process(backup_info, "creating temporary directory", e);
            return ;
        }
    };
    let tmp_dir_path = tmp_dir.path();

    // fetch indexes
    let indexes = match crate::routes::index::list_indexes_sync(&data, &main_reader) {
        Ok(indexes) => indexes,
        Err(e) => {
            fail_backup_process(backup_info, "listing indexes", e);
            return ;
        }
    };

    // create metadata
    if let Err(e) = backup_metadata(&data, &tmp_dir_path, indexes.clone()) {
        fail_backup_process(backup_info, "generating metadata", e);
        return ;
    }

    // export settings, updates and documents for each indexes
    for index in indexes {
        let index_path = tmp_dir_path.join(&index.uid);

        // create index sub-dircetory
        if let Err(e) = create_dir_all(&index_path) {
            fail_backup_process(backup_info, &format!("creating directory for index {}", &index.uid), e);
            return ;
        }

        // export settings
        if let Err(e) = backup_index_settings(&data, &main_reader, &index_path, &index.uid) {
            fail_backup_process(backup_info, &format!("generating settings for index {}", &index.uid), e);
            return ;
        }

        // export documents
        if let Err(e) = backup_index_documents(&data, &main_reader, &index_path, &index.uid) {
            fail_backup_process(backup_info, &format!("generating documents for index {}", &index.uid), e);
            return ;
        }

        // export updates
        if let Err(e) = backup_index_updates(&data, &update_reader, &index_path, &index.uid) {
            fail_backup_process(backup_info, &format!("generating updates for index {}", &index.uid), e);
            return ;
        }
    }

    // compress backup in a file named `{backup_uid}.tar.gz` in `backup_folder`
    if let Err(e) = crate::helpers::compression::to_tar_gz(&tmp_dir_path, &compressed_backup_folder(&backup_folder, &backup_info.uid)) {
        fail_backup_process(backup_info, "compressing backup", e);
        return ;
    }

    // update backup info to `done`
    let resume = BackupInfo::new(
        backup_info.uid,
        BackupStatus::Done
    );

    resume.set_current();
}

pub fn init_backup_process(data: &web::Data<Data>, backup_folder: &Path) -> Result<BackupInfo, Error> {
    create_dir_all(backup_folder).or(Err(Error::backup_failed()))?;

    // check if a backup is already in progress
    if let Some(resume) = BackupInfo::get_current() {
        if resume.backup_already_in_progress() {
            return Err(Error::backup_conflict())
        }
    }

    // generate a new backup info
    let info = BackupInfo::new(
        generate_uid(),
        BackupStatus::Processing
    );

    info.set_current();

    let data = data.clone();
    let backup_folder = backup_folder.to_path_buf();
    let info_cloned = info.clone();
    // run backup process in a new thread
    thread::spawn(move || 
        backup_process(data, backup_folder, info_cloned)
    );

    Ok(info)
}
