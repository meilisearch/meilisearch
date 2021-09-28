use std::fs::{create_dir_all, File};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::Context;
use heed::{EnvOpenOptions, RoTxn};
use indexmap::IndexMap;
use milli::documents::DocumentBatchReader;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::document_formats::read_jsonl;
use crate::index::update_handler::UpdateHandler;
use crate::index::updates::apply_settings_to_builder;
use crate::index_controller::{asc_ranking_rule, desc_ranking_rule};

use super::error::Result;
use super::{Index, Settings, Unchecked};

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
        let txn = self.env.write_txn()?;
        let path = path
            .as_ref()
            .join(format!("indexes/{}", self.uuid.to_string()));

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
        update_handler: &UpdateHandler,
    ) -> anyhow::Result<()> {
        let dir_name = src
            .as_ref()
            .file_name()
            .with_context(|| format!("invalid dump index: {}", src.as_ref().display()))?;

        let dst_dir_path = dst.as_ref().join("indexes").join(dir_name);
        create_dir_all(&dst_dir_path)?;

        let meta_path = src.as_ref().join(META_FILE_NAME);
        let mut meta_file = File::open(meta_path)?;

        // We first deserialize the dump meta into a serde_json::Value and change
        // the custom ranking rules settings from the old format to the new format.
        let mut meta: Value = serde_json::from_reader(&mut meta_file)?;
        if let Some(ranking_rules) = meta.pointer_mut("/settings/rankingRules") {
            convert_custom_ranking_rules(ranking_rules);
        }

        // Then we serialize it back into a vec to deserialize it
        // into a `DumpMeta` struct with the newly patched `rankingRules` format.
        let patched_meta = serde_json::to_vec(&meta)?;

        let DumpMeta {
            settings,
            primary_key,
        } = serde_json::from_slice(&patched_meta)?;
        let settings = settings.check();

        let mut options = EnvOpenOptions::new();
        options.map_size(size);
        let index = milli::Index::new(options, &dst_dir_path)?;

        let mut txn = index.write_txn()?;

        // Apply settings first
        let builder = update_handler.update_builder(0);
        let mut builder = builder.settings(&mut txn, &index);

        if let Some(primary_key) = primary_key {
            builder.set_primary_key(primary_key);
        }

        apply_settings_to_builder(&settings, &mut builder);

        builder.execute(|_, _| ())?;

        let document_file_path = src.as_ref().join(DATA_FILE_NAME);
        let reader = BufReader::new(File::open(&document_file_path)?);

        let mut tmp_doc_file = tempfile::tempfile()?;

        read_jsonl(reader, &mut tmp_doc_file)?;

        tmp_doc_file.seek(SeekFrom::Start(0))?;

        let documents_reader = DocumentBatchReader::from_reader(tmp_doc_file)?;

        //If the document file is empty, we don't perform the document addition, to prevent
        //a primary key error to be thrown.
        if !documents_reader.is_empty() {
            let builder = update_handler.update_builder(0).index_documents(&mut txn, &index);
            builder.execute(documents_reader, |_, _| ())?;
        }

        txn.commit()?;

        index.prepare_for_closing().wait();

        Ok(())
    }
}

/// Converts the ranking rules from the format `asc(_)`, `desc(_)` to the format `_:asc`, `_:desc`.
///
/// This is done for compatibility reasons, and to avoid a new dump version,
/// since the new syntax was introduced soon after the new dump version.
fn convert_custom_ranking_rules(ranking_rules: &mut Value) {
    *ranking_rules = match ranking_rules.take() {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| match value {
                Value::String(s) if s.starts_with("asc") => asc_ranking_rule(&s)
                    .map(|f| format!("{}:asc", f))
                    .map(Value::String),
                Value::String(s) if s.starts_with("desc") => desc_ranking_rule(&s)
                    .map(|f| format!("{}:desc", f))
                    .map(Value::String),
                otherwise => Some(otherwise),
            })
            .collect(),
        otherwise => otherwise,
    }
}
