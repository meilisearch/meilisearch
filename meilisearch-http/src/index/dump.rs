use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context};
use heed::RoTxn;
use indexmap::IndexMap;
use milli::update::{IndexDocumentsMethod, UpdateFormat::JsonStream};
use serde::{Deserialize, Serialize};

use crate::option::IndexerOpts;

use super::{update_handler::UpdateHandler, Index, Settings, Unchecked};

#[derive(Serialize, Deserialize)]
struct DumpMeta {
    settings: Settings<Unchecked>,
    primary_key: Option<String>,
}

const META_FILE_NAME: &str = "meta.json";
const DATA_FILE_NAME: &str = "documents.jsonl";

impl Index {
    pub fn dump(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        // acquire write txn make sure any ongoing write is finnished before we start.
        let txn = self.env.write_txn()?;

        self.dump_documents(&txn, &path)?;
        self.dump_meta(&txn, &path)?;

        Ok(())
    }

    fn dump_documents(&self, txn: &RoTxn, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let document_file_path = path.as_ref().join(DATA_FILE_NAME);
        let mut document_file = File::create(&document_file_path)?;

        let documents = self.all_documents(txn)?;
        let fields_ids_map = self.fields_ids_map(txn)?;

        // dump documents
        let mut json_map = IndexMap::new();
        for document in documents {
            let (_, reader) = document?;

            for (fid, bytes) in reader.iter() {
                if let Some(name) = fields_ids_map.name(fid) {
                    json_map.insert(name, serde_json::from_slice::<serde_json::Value>(bytes)?);
                }
            }

            serde_json::to_writer(&mut document_file, &json_map)?;
            document_file.write_all(b"\n")?;

            json_map.clear();
        }

        Ok(())
    }

    fn dump_meta(&self, txn: &RoTxn, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let meta_file_path = path.as_ref().join(META_FILE_NAME);
        let mut meta_file = File::create(&meta_file_path)?;

        let settings = self.settings_txn(txn)?.into_unchecked();
        let primary_key = self.primary_key(txn)?.map(String::from);
        let meta = DumpMeta {
            settings,
            primary_key,
        };

        serde_json::to_writer(&mut meta_file, &meta)?;

        Ok(())
    }

    pub fn load_dump(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        size: usize,
        indexing_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        let dir_name = src
            .as_ref()
            .file_name()
            .with_context(|| format!("invalid dump index: {}", src.as_ref().display()))?;
        let dst_dir_path = dst.as_ref().join("indexes").join(dir_name);
        create_dir_all(&dst_dir_path)?;

        let meta_path = src.as_ref().join(META_FILE_NAME);
        let mut meta_file = File::open(meta_path)?;
        let DumpMeta {
            settings,
            primary_key,
        } = serde_json::from_reader(&mut meta_file)?;
        let settings = settings.check();
        let index = Self::open(&dst_dir_path, size)?;
        let mut txn = index.write_txn()?;

        let handler = UpdateHandler::new(&indexing_options)?;

        index.update_settings_txn(&mut txn, &settings, handler.update_builder(0))?;

        let document_file_path = src.as_ref().join(DATA_FILE_NAME);
        let reader = File::open(&document_file_path)?;
        let mut reader = BufReader::new(reader);
        reader.fill_buf()?;
        // If the document file is empty, we don't perform the document addition, to prevent
        // a primary key error to be thrown.
        if !reader.buffer().is_empty() {
            index.update_documents_txn(
                &mut txn,
                JsonStream,
                IndexDocumentsMethod::UpdateDocuments,
                Some(reader),
                handler.update_builder(0),
                primary_key.as_deref(),
            )?;
        }

        txn.commit()?;

        match Arc::try_unwrap(index.0) {
            Ok(inner) => inner.prepare_for_closing().wait(),
            Err(_) => bail!("Could not close index properly."),
        }

        Ok(())
    }
}
