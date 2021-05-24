use std::{collections::{BTreeSet, HashSet}, io::Write, marker::PhantomData, path::{Path, PathBuf}};
use std::ops::Deref;
use std::sync::Arc;
use std::fs::File;

use anyhow::{bail, Context};
use heed::RoTxn;
use indexmap::IndexMap;
use milli::obkv_to_json;
use serde_json::{Map, Value};

use crate::helpers::EnvSizer;
pub use search::{SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{Facets, Settings, Checked, Unchecked};
use serde::{de::Deserializer, Deserialize};

mod search;
mod updates;

pub type Document = Map<String, Value>;

#[derive(Clone)]
pub struct Index(pub Arc<milli::Index>);

impl Deref for Index {
    type Target = milli::Index;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

pub fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

impl Index {
    pub fn settings(&self) -> anyhow::Result<Settings<Checked>> {
        let txn = self.read_txn()?;
        self.settings_txn(&txn)
    }

    pub fn settings_txn(&self, txn: &RoTxn) -> anyhow::Result<Settings<Checked>> {
        let displayed_attributes = self
            .displayed_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect());

        let searchable_attributes = self
            .searchable_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect());

        let faceted_attributes = self
            .faceted_fields(&txn)?
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        let criteria = self
            .criteria(&txn)?
            .into_iter()
            .map(|c| c.to_string())
            .collect();

        let stop_words = self
            .stop_words(&txn)?
            .map(|stop_words| -> anyhow::Result<BTreeSet<_>> {
                Ok(stop_words.stream().into_strs()?.into_iter().collect())
            })
            .transpose()?
            .unwrap_or_else(BTreeSet::new);
        let distinct_attribute = self.distinct_attribute(&txn)?.map(String::from);

        Ok(Settings {
            displayed_attributes: Some(displayed_attributes),
            searchable_attributes: Some(searchable_attributes),
            attributes_for_faceting: Some(Some(faceted_attributes)),
            ranking_rules: Some(Some(criteria)),
            stop_words: Some(Some(stop_words)),
            distinct_attribute: Some(distinct_attribute),
            _kind: PhantomData,
        })
    }

    pub fn retrieve_documents<S: AsRef<str>>(
        &self,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Vec<Map<String, Value>>> {
        let txn = self.read_txn()?;

        let fields_ids_map = self.fields_ids_map(&txn)?;
        let fields_to_display =
            self.fields_to_display(&txn, &attributes_to_retrieve, &fields_ids_map)?;

        let iter = self.documents.range(&txn, &(..))?.skip(offset).take(limit);

        let mut documents = Vec::new();

        println!("fields to display: {:?}", fields_to_display);

        for entry in iter {
            let (_id, obkv) = entry?;
            let object = obkv_to_json(&fields_to_display, &fields_ids_map, obkv)?;
            documents.push(object);
        }

        Ok(documents)
    }

    pub fn retrieve_document<S: AsRef<str>>(
        &self,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Map<String, Value>> {
        let txn = self.read_txn()?;

        let fields_ids_map = self.fields_ids_map(&txn)?;

        let fields_to_display =
            self.fields_to_display(&txn, &attributes_to_retrieve, &fields_ids_map)?;

        let internal_id = self
            .external_documents_ids(&txn)?
            .get(doc_id.as_bytes())
            .with_context(|| format!("Document with id {} not found", doc_id))?;

        let document = self
            .documents(&txn, std::iter::once(internal_id))?
            .into_iter()
            .next()
            .map(|(_, d)| d);

        match document {
            Some(document) => Ok(obkv_to_json(&fields_to_display, &fields_ids_map, document)?),
            None => bail!("Document with id {} not found", doc_id),
        }
    }

    pub fn size(&self) -> u64 {
        self.env.size()
    }

    fn fields_to_display<S: AsRef<str>>(
        &self,
        txn: &heed::RoTxn,
        attributes_to_retrieve: &Option<Vec<S>>,
        fields_ids_map: &milli::FieldsIdsMap,
    ) -> anyhow::Result<Vec<u8>> {
        let mut displayed_fields_ids = match self.displayed_fields_ids(&txn)? {
            Some(ids) => ids.into_iter().collect::<Vec<_>>(),
            None => fields_ids_map.iter().map(|(id, _)| id).collect(),
        };

        let attributes_to_retrieve_ids = match attributes_to_retrieve {
            Some(attrs) => attrs
                .iter()
                .filter_map(|f| fields_ids_map.id(f.as_ref()))
                .collect::<HashSet<_>>(),
            None => fields_ids_map.iter().map(|(id, _)| id).collect(),
        };

        displayed_fields_ids.retain(|fid| attributes_to_retrieve_ids.contains(fid));
        Ok(displayed_fields_ids)
    }

    pub fn dump(&self, path: PathBuf) -> anyhow::Result<()> {
        // acquire write txn make sure any ongoing write is finnished before we start.
        let txn = self.env.write_txn()?;

        self.dump_documents(&txn, &path)?;
        self.dump_meta(&txn, &path)?;

        Ok(())
    }

    fn dump_documents(&self, txn: &RoTxn, path: impl AsRef<Path>) -> anyhow::Result<()> {
        println!("dumping documents");
        let document_file_path = path.as_ref().join("documents.jsonl");
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
            document_file.write(b"\n")?;

            json_map.clear();
        }

        Ok(())
    }

    fn dump_meta(&self, txn: &RoTxn, path: impl AsRef<Path>) -> anyhow::Result<()> {
        println!("dumping settings");
        let meta_file_path = path.as_ref().join("meta.json");
        let mut meta_file = File::create(&meta_file_path)?;

        let settings = self.settings_txn(txn)?;
        let json = serde_json::json!({
            "settings": settings,
        });

        serde_json::to_writer(&mut meta_file, &json)?;

        Ok(())
    }
}
