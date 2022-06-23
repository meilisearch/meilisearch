use std::fs::{create_dir_all, File};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::Context;
use indexmap::IndexMap;
use milli::documents::DocumentBatchReader;
use milli::heed::{EnvOpenOptions, RoTxn};
use milli::update::{IndexDocumentsConfig, IndexerConfig};
use serde::{Deserialize, Serialize};

use crate::document_formats::read_ndjson;
use crate::index::updates::apply_settings_to_builder;

use super::error::Result;
use super::{index::Index, Settings, Unchecked};

#[derive(Serialize, Deserialize)]
struct DumpMeta {
    settings: Settings<Unchecked>,
    primary_key: Option<String>,
}

const META_FILE_NAME: &str = "meta.json";
const DATA_FILE_NAME: &str = "documents.jsonl";

impl Index {
    pub fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
        // acquire write txn make sure any ongoing write is finished before we start.
        let txn = self.write_txn()?;
        let path = path.as_ref().join(format!("indexes/{}", self.uuid));

        create_dir_all(&path)?;

        self.dump_documents(&txn, &path)?;
        self.dump_meta(&txn, &path)?;

        Ok(())
    }

    fn dump_documents(&self, txn: &RoTxn, path: impl AsRef<Path>) -> Result<()> {
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

    fn dump_meta(&self, txn: &RoTxn, path: impl AsRef<Path>) -> Result<()> {
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
        indexer_config: &IndexerConfig,
    ) -> anyhow::Result<()> {
        let dir_name = src
            .as_ref()
            .file_name()
            .with_context(|| format!("invalid dump index: {}", src.as_ref().display()))?;

        let dst_dir_path = dst.as_ref().join("indexes").join(dir_name);
        create_dir_all(&dst_dir_path)?;

        let meta_path = src.as_ref().join(META_FILE_NAME);
        let meta_file = File::open(meta_path)?;
        let DumpMeta {
            settings,
            primary_key,
        } = serde_json::from_reader(meta_file)?;
        let settings = settings.check();

        let mut options = EnvOpenOptions::new();
        options.map_size(size);
        let index = milli::Index::new(options, &dst_dir_path)?;

        let mut txn = index.write_txn()?;

        // Apply settings first
        let mut builder = milli::update::Settings::new(&mut txn, &index, indexer_config);

        if let Some(primary_key) = primary_key {
            builder.set_primary_key(primary_key);
        }

        apply_settings_to_builder(&settings, &mut builder);

        builder.execute(|_| ())?;

        let document_file_path = src.as_ref().join(DATA_FILE_NAME);
        let reader = BufReader::new(File::open(&document_file_path)?);

        let mut tmp_doc_file = tempfile::tempfile()?;

        let empty = match read_ndjson(reader, &mut tmp_doc_file) {
            // if there was no document in the file it's because the index was empty
            Ok(0) => true,
            Ok(_) => false,
            Err(e) => return Err(e.into()),
        };

        if !empty {
            tmp_doc_file.seek(SeekFrom::Start(0))?;

            let documents_reader = DocumentBatchReader::from_reader(tmp_doc_file)?;

            //If the document file is empty, we don't perform the document addition, to prevent
            //a primary key error to be thrown.
            let config = IndexDocumentsConfig::default();
            let mut builder = milli::update::IndexDocuments::new(
                &mut txn,
                &index,
                indexer_config,
                config,
                |_| (),
            )?;
            builder.add_documents(documents_reader)?;
            builder.execute()?;
        }

        txn.commit()?;
        index.prepare_for_closing().wait();

        Ok(())
    }
}
