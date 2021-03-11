mod search;
mod updates;

use std::sync::Arc;
use std::ops::Deref;

use anyhow::{bail, Context};
use serde_json::{Value, Map};
use milli::obkv_to_json;

pub use search::{SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{Settings, Facets, UpdateResult};

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

        Ok(Settings {
            displayed_attributes: Some(Some(displayed_attributes)),
            searchable_attributes: Some(Some(searchable_attributes)),
            faceted_attributes: Some(Some(faceted_attributes)),
            ranking_rules: Some(Some(criteria)),
        })
    }

    pub fn retrieve_documents<S>(
        &self,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Vec<Map<String, Value>>>
    where
        S: AsRef<str> + Send + Sync + 'static,
    {
        let txn = self.read_txn()?;

        let fields_ids_map = self.fields_ids_map(&txn)?;

        let attributes_to_retrieve_ids = match attributes_to_retrieve {
            Some(attrs) => attrs
                .iter()
                .filter_map(|f| fields_ids_map.id(f.as_ref()))
                .collect::<Vec<_>>(),
            None => fields_ids_map.iter().map(|(id, _)| id).collect(),
        };

        let iter = self.documents.range(&txn, &(..))?.skip(offset).take(limit);

        let mut documents = Vec::new();

        for entry in iter {
            let (_id, obkv) = entry?;
            let object = obkv_to_json(&attributes_to_retrieve_ids, &fields_ids_map, obkv)?;
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

            let attributes_to_retrieve_ids = match attributes_to_retrieve {
                Some(attrs) => attrs
                    .iter()
                    .filter_map(|f| fields_ids_map.id(f.as_ref()))
                    .collect::<Vec<_>>(),
                None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            };

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
                Some(document) => Ok(obkv_to_json(
                    &attributes_to_retrieve_ids,
                    &fields_ids_map,
                    document,
                )?),
                None => bail!("Document with id {} not found", doc_id),
            }
    }
}
