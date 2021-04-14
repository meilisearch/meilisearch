use std::collections::{BTreeSet, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{bail, Context};
use milli::obkv_to_json;
use serde_json::{Map, Value};

use crate::helpers::EnvSizer;
pub use search::{SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{Facets, Settings, UpdateResult};

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

impl Index {
    pub fn settings(&self) -> anyhow::Result<Settings> {
        let txn = self.read_txn()?;

        let displayed_attributes = self
            .displayed_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let searchable_attributes = self
            .searchable_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

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
        let distinct_attribute = self
            .distinct_attribute(&txn)?
            .map(String::from);

        Ok(Settings {
            displayed_attributes: Some(Some(displayed_attributes)),
            searchable_attributes: Some(Some(searchable_attributes)),
            attributes_for_faceting: Some(Some(faceted_attributes)),
            ranking_rules: Some(Some(criteria)),
            stop_words: Some(Some(stop_words)),
            distinct_attribute: Some(distinct_attribute),
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
}
