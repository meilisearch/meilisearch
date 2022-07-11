use std::collections::BTreeSet;
use std::fs::create_dir_all;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use walkdir::WalkDir;

use fst::IntoStreamer;
use milli::heed::{CompactionOption, EnvOpenOptions, RoTxn};
use milli::update::{IndexerConfig, Setting};
use milli::{obkv_to_json, FieldDistribution, DEFAULT_VALUES_PER_FACET};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::index::search::DEFAULT_PAGINATION_MAX_TOTAL_HITS;

use super::error::IndexError;
use super::error::Result;
use super::updates::{FacetingSettings, MinWordSizeTyposSetting, PaginationSettings, TypoSettings};
use super::{Checked, Settings};

pub type Document = Map<String, Value>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMeta {
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub primary_key: Option<String>,
}

impl IndexMeta {
    pub fn new(index: &Index) -> Result<Self> {
        let txn = index.read_txn()?;
        Self::new_txn(index, &txn)
    }

    pub fn new_txn(index: &Index, txn: &milli::heed::RoTxn) -> Result<Self> {
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
    pub indexer_config: Arc<IndexerConfig>,
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
        uuid: Uuid,
        update_handler: Arc<IndexerConfig>,
    ) -> Result<Self> {
        log::debug!("opening index in {}", path.as_ref().display());
        create_dir_all(&path)?;
        let mut options = EnvOpenOptions::new();
        options.map_size(size);
        let inner = Arc::new(milli::Index::new(options, &path)?);
        Ok(Index {
            inner,
            uuid,
            indexer_config: update_handler,
        })
    }

    /// Asynchronously close the underlying index
    pub fn close(self) {
        self.inner.as_ref().clone().prepare_for_closing();
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
            .user_defined_searchable_fields(txn)?
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
            .unwrap_or_default();
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

        let min_typo_word_len = MinWordSizeTyposSetting {
            one_typo: Setting::Set(self.min_word_len_one_typo(txn)?),
            two_typos: Setting::Set(self.min_word_len_two_typos(txn)?),
        };

        let disabled_words = match self.exact_words(txn)? {
            Some(fst) => fst.into_stream().into_strs()?.into_iter().collect(),
            None => BTreeSet::new(),
        };

        let disabled_attributes = self
            .exact_attributes(txn)?
            .into_iter()
            .map(String::from)
            .collect();

        let typo_tolerance = TypoSettings {
            enabled: Setting::Set(self.authorize_typos(txn)?),
            min_word_size_for_typos: Setting::Set(min_typo_word_len),
            disable_on_words: Setting::Set(disabled_words),
            disable_on_attributes: Setting::Set(disabled_attributes),
        };

        let faceting = FacetingSettings {
            max_values_per_facet: Setting::Set(
                self.max_values_per_facet(txn)?
                    .unwrap_or(DEFAULT_VALUES_PER_FACET),
            ),
        };

        let pagination = PaginationSettings {
            max_total_hits: Setting::Set(
                self.pagination_max_total_hits(txn)?
                    .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS),
            ),
        };

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
            typo_tolerance: Setting::Set(typo_tolerance),
            faceting: Setting::Set(faceting),
            pagination: Setting::Set(pagination),
            _kind: PhantomData,
        })
    }

    /// Return the total number of documents contained in the index + the selected documents.
    pub fn retrieve_documents<S: AsRef<str>>(
        &self,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> Result<(u64, Vec<Document>)> {
        let txn = self.read_txn()?;

        let fields_ids_map = self.fields_ids_map(&txn)?;
        let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

        let iter = self.all_documents(&txn)?.skip(offset).take(limit);

        let mut documents = Vec::new();

        for entry in iter {
            let (_id, obkv) = entry?;
            let document = obkv_to_json(&all_fields, &fields_ids_map, obkv)?;
            let document = match &attributes_to_retrieve {
                Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
                    &document,
                    attributes_to_retrieve.iter().map(|s| s.as_ref()),
                ),
                None => document,
            };
            documents.push(document);
        }

        let number_of_documents = self.number_of_documents(&txn)?;

        Ok((number_of_documents, documents))
    }

    pub fn retrieve_document<S: AsRef<str>>(
        &self,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> Result<Document> {
        let txn = self.read_txn()?;

        let fields_ids_map = self.fields_ids_map(&txn)?;
        let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

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

        let document = obkv_to_json(&all_fields, &fields_ids_map, document)?;
        let document = match &attributes_to_retrieve {
            Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
                &document,
                attributes_to_retrieve.iter().map(|s| s.as_ref()),
            ),
            None => document,
        };

        Ok(document)
    }

    pub fn size(&self) -> u64 {
        WalkDir::new(self.inner.path())
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len())
    }

    pub fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut dst = path.as_ref().join(format!("indexes/{}/", self.uuid));
        create_dir_all(&dst)?;
        dst.push("data.mdb");
        let _txn = self.write_txn()?;
        self.inner.copy_to_path(dst, CompactionOption::Enabled)?;
        Ok(())
    }
}

/// When running tests, when a server instance is dropped, the environment is not actually closed,
/// leaving a lot of open file descriptors.
impl Drop for Index {
    fn drop(&mut self) {
        // When dropping the last instance of an index, we want to close the index
        // Note that the close is actually performed only if all the instances a effectively
        // dropped

        if Arc::strong_count(&self.inner) == 1 {
            self.inner.as_ref().clone().prepare_for_closing();
        }
    }
}
