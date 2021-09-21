use std::fs::File;
use std::io::Write;
use std::path::Path;

use heed::RoTxn;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::options::IndexerOpts;

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
        _src: impl AsRef<Path>,
        _dst: impl AsRef<Path>,
        _size: usize,
        _indexing_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        //let dir_name = src
            //.as_ref()
            //.file_name()
            //.with_context(|| format!("invalid dump index: {}", src.as_ref().display()))?;

        //let dst_dir_path = dst.as_ref().join("indexes").join(dir_name);
        //create_dir_all(&dst_dir_path)?;

        //let meta_path = src.as_ref().join(META_FILE_NAME);
        //let mut meta_file = File::open(meta_path)?;

        //// We first deserialize the dump meta into a serde_json::Value and change
        //// the custom ranking rules settings from the old format to the new format.
        //let mut meta: Value = serde_json::from_reader(&mut meta_file)?;
        //if let Some(ranking_rules) = meta.pointer_mut("/settings/rankingRules") {
            //convert_custom_ranking_rules(ranking_rules);
        //}

        //// Then we serialize it back into a vec to deserialize it
        //// into a `DumpMeta` struct with the newly patched `rankingRules` format.
        //let patched_meta = serde_json::to_vec(&meta)?;

        //let DumpMeta {
            //settings,
            //primary_key,
        //} = serde_json::from_slice(&patched_meta)?;
        //let settings = settings.check();
        //let index = Self::open(&dst_dir_path, size)?;
        //let mut txn = index.write_txn()?;

        //let handler = UpdateHandler::new(indexing_options)?;

        //index.update_settings_txn(&mut txn, &settings, handler.update_builder(0))?;

        //let document_file_path = src.as_ref().join(DATA_FILE_NAME);
        //let reader = File::open(&document_file_path)?;
        //let mut reader = BufReader::new(reader);
        //reader.fill_buf()?;
        // If the document file is empty, we don't perform the document addition, to prevent
        // a primary key error to be thrown.

        todo!("fix obk document dumps")
        //if !reader.buffer().is_empty() {
            //index.update_documents_txn(
                //&mut txn,
                //IndexDocumentsMethod::UpdateDocuments,
                //Some(reader),
                //handler.update_builder(0),
                //primary_key.as_deref(),
            //)?;
        //}

        //txn.commit()?;

        //match Arc::try_unwrap(index.0) {
            //Ok(inner) => inner.prepare_for_closing().wait(),
            //Err(_) => bail!("Could not close index properly."),
        //}

        //Ok(())
    }
}

// /// Converts the ranking rules from the format `asc(_)`, `desc(_)` to the format `_:asc`, `_:desc`.
// ///
// /// This is done for compatibility reasons, and to avoid a new dump version,
// /// since the new syntax was introduced soon after the new dump version.
//fn convert_custom_ranking_rules(ranking_rules: &mut Value) {
    //*ranking_rules = match ranking_rules.take() {
        //Value::Array(values) => values
            //.into_iter()
            //.filter_map(|value| match value {
                //Value::String(s) if s.starts_with("asc") => asc_ranking_rule(&s)
                    //.map(|f| format!("{}:asc", f))
                    //.map(Value::String),
                //Value::String(s) if s.starts_with("desc") => desc_ranking_rule(&s)
                    //.map(|f| format!("{}:desc", f))
                    //.map(Value::String),
                //otherwise => Some(otherwise),
            //})
            //.collect(),
        //otherwise => otherwise,
    //}
//}
