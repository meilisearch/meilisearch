use std::collections::{BTreeSet, HashSet};
use std::fs::create_dir_all;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use heed::{EnvOpenOptions, RoTxn};
use milli::update::Setting;
use milli::{obkv_to_json, FieldDistribution, FieldId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::index_controller::update_file_store::UpdateFileStore;
use crate::EnvSizer;

use super::error::IndexError;
use super::error::Result;
use super::update_handler::UpdateHandler;
use super::{Checked, Settings};

pub type Document = Map<String, Value>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub primary_key: Option<String>,
}

impl IndexMeta {
    pub fn new(index: &Index) -> Result<Self> {
        let txn = index.read_txn()?;
        Self::new_txn(index, &txn)
    }

    pub fn new_txn(index: &Index, txn: &heed::RoTxn) -> Result<Self> {
        let created_at = index.created_at(txn)?;
        let updated_at = index.updated_at(txn)?;
        let primary_key = index.primary_key(txn)?.map(String::from);
        Ok(Self {
            created_at,
            updated_at,
            primary_key,
        })
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IndexStats {
    #[serde(skip)]
    pub size: u64,
    pub number_of_documents: u64,
    /// Whether the current index is performing an update. It is initially `None` when the
    /// index returns it, since it is the `UpdateStore` that knows what index is currently indexing. It is
    /// later set to either true or false, we we retrieve the information from the `UpdateStore`
    pub is_indexing: Option<bool>,
    pub field_distribution: FieldDistribution,
}

#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
pub struct Index {
    pub uuid: Uuid,
    #[derivative(Debug = "ignore")]
    pub inner: Arc<milli::Index>,
    #[derivative(Debug = "ignore")]
    pub update_file_store: Arc<UpdateFileStore>,
    #[derivative(Debug = "ignore")]
    pub update_handler: Arc<UpdateHandler>,
}

impl Deref for Index {
    type Target = milli::Index;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl Index {
    pub fn open(
        path: impl AsRef<Path>,
        size: usize,
        update_file_store: Arc<UpdateFileStore>,
        uuid: Uuid,
        update_handler: Arc<UpdateHandler>,
    ) -> Result<Self> {
        create_dir_all(&path)?;
        let mut options = EnvOpenOptions::new();
        options.map_size(size);
        let inner = Arc::new(milli::Index::new(options, &path)?);
        Ok(Index {
            inner,
            update_file_store,
            uuid,
            update_handler,
        })
    }

    pub fn inner(&self) -> &milli::Index {
        &self.inner
    }

    pub fn stats(&self) -> Result<IndexStats> {
        let rtxn = self.read_txn()?;

        Ok(IndexStats {
            size: self.size(),
            number_of_documents: self.number_of_documents(&rtxn)?,
            is_indexing: None,
            field_distribution: self.field_distribution(&rtxn)?,
        })
    }

    pub fn meta(&self) -> Result<IndexMeta> {
        IndexMeta::new(self)
    }
    pub fn settings(&self) -> Result<Settings<Checked>> {
        let txn = self.read_txn()?;
        self.settings_txn(&txn)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn settings_txn(&self, txn: &RoTxn) -> Result<Settings<Checked>> {
        let displayed_attributes = self
            .displayed_fields(txn)?
            .map(|fields| fields.into_iter().map(String::from).collect());

        let searchable_attributes = self
            .searchable_fields(txn)?
            .map(|fields| fields.into_iter().map(String::from).collect());

        let filterable_attributes = self.filterable_fields(txn)?.into_iter().collect();

        let sortable_attributes = self.sortable_fields(txn)?.into_iter().collect();

        let criteria = self
            .criteria(txn)?
            .into_iter()
            .map(|c| c.to_string())
            .collect();

        let stop_words = self
            .stop_words(txn)?
            .map(|stop_words| -> Result<BTreeSet<_>> {
                Ok(stop_words.stream().into_strs()?.into_iter().collect())
            })
            .transpose()?
            .unwrap_or_else(BTreeSet::new);
        let distinct_field = self.distinct_field(txn)?.map(String::from);

        // in milli each word in the synonyms map were split on their separator. Since we lost
        // this information we are going to put space between words.
        let synonyms = self
            .synonyms(txn)?
            .iter()
            .map(|(key, values)| {
                (
                    key.join(" "),
                    values.iter().map(|value| value.join(" ")).collect(),
                )
            })
            .collect();

        Ok(Settings {
            displayed_attributes: match displayed_attributes {
                Some(attrs) => Setting::Set(attrs),
                None => Setting::Reset,
            },
            searchable_attributes: match searchable_attributes {
                Some(attrs) => Setting::Set(attrs),
                None => Setting::Reset,
            },
            filterable_attributes: Setting::Set(filterable_attributes),
            sortable_attributes: Setting::Set(sortable_attributes),
            ranking_rules: Setting::Set(criteria),
            stop_words: Setting::Set(stop_words),
            distinct_attribute: match distinct_field {
                Some(field) => Setting::Set(field),
                None => Setting::Reset,
            },
            synonyms: Setting::Set(synonyms),
            _kind: PhantomData,
        })
    }

    pub fn retrieve_documents<S: AsRef<str>>(
        &self,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> Result<Vec<Map<String, Value>>> {
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
    ) -> Result<Map<String, Value>> {
        let txn = self.read_txn()?;

        let fields_ids_map = self.fields_ids_map(&txn)?;

        let fields_to_display =
            self.fields_to_display(&txn, &attributes_to_retrieve, &fields_ids_map)?;

        let internal_id = self
            .external_documents_ids(&txn)?
            .get(doc_id.as_bytes())
            .ok_or_else(|| IndexError::DocumentNotFound(doc_id.clone()))?;

        let document = self
            .documents(&txn, std::iter::once(internal_id))?
            .into_iter()
            .next()
            .map(|(_, d)| d)
            .ok_or(IndexError::DocumentNotFound(doc_id))?;

        let document = obkv_to_json(&fields_to_display, &fields_ids_map, document)?;

        Ok(document)
    }

    pub fn size(&self) -> u64 {
        self.env.size()
    }

    fn fields_to_display<S: AsRef<str>>(
        &self,
        txn: &heed::RoTxn,
        attributes_to_retrieve: &Option<Vec<S>>,
        fields_ids_map: &milli::FieldsIdsMap,
    ) -> Result<Vec<FieldId>> {
        let mut displayed_fields_ids = match self.displayed_fields_ids(txn)? {
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

    pub fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut dst = path.as_ref().join(format!("indexes/{}/", self.uuid));
        create_dir_all(&dst)?;
        dst.push("data.mdb");
        let _txn = self.write_txn()?;
        self.inner
            .env
            .copy_to_path(dst, heed::CompactionOption::Enabled)?;
        Ok(())
    }
}
