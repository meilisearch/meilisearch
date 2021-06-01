use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Context;
use chrono::Utc;
use grenad::CompressionType;
use itertools::Itertools;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
use rayon::ThreadPool;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{FieldsIdsMap, Index};
use crate::criterion::Criterion;
use crate::update::{ClearDocuments, IndexDocuments, UpdateIndexingStep};
use crate::update::index_documents::{IndexDocumentsMethod, Transform};

#[derive(Debug, Clone, PartialEq)]
pub enum Setting<T> {
    Set(T),
    Reset,
    NotSet,
}

impl<T> Default for Setting<T> {
    fn default() -> Self {
        Self::NotSet
    }
}

impl<T> Setting<T> {
    pub const fn is_not_set(&self) -> bool {
        matches!(self, Self::NotSet)
    }
}

impl<T: Serialize> Serialize for Setting<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        match self {
            Self::Set(value) => Some(value),
            // Usually not_set isn't serialized by setting skip_serializing_if field attribute
            Self::NotSet | Self::Reset => None,
        }.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Setting<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        Deserialize::deserialize(deserializer).map(|x| match x {
            Some(x) => Self::Set(x),
            None => Self::Reset, // Reset is forced by sending null value
        })
    }
}

pub struct Settings<'a, 't, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) log_every_n: Option<usize>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) linked_hash_map_size: Option<usize>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    pub(crate) thread_pool: Option<&'a ThreadPool>,
    update_id: u64,

    searchable_fields: Setting<Vec<String>>,
    displayed_fields: Setting<Vec<String>>,
    filterable_fields: Setting<HashSet<String>>,
    criteria: Setting<Vec<String>>,
    stop_words: Setting<BTreeSet<String>>,
    distinct_attribute: Setting<String>,
    synonyms: Setting<HashMap<String, Vec<String>>>,
}

impl<'a, 't, 'u, 'i> Settings<'a, 't, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        update_id: u64,
    ) -> Settings<'a, 't, 'u, 'i> {
        Settings {
            wtxn,
            index,
            log_every_n: None,
            max_nb_chunks: None,
            max_memory: None,
            linked_hash_map_size: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            thread_pool: None,
            searchable_fields: Setting::NotSet,
            displayed_fields: Setting::NotSet,
            filterable_fields: Setting::NotSet,
            criteria: Setting::NotSet,
            stop_words: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            synonyms: Setting::NotSet,
            update_id,
        }
    }

    pub fn reset_searchable_fields(&mut self) {
        self.searchable_fields = Setting::Reset;
    }

    pub fn set_searchable_fields(&mut self, names: Vec<String>) {
        self.searchable_fields = Setting::Set(names);
    }

    pub fn reset_displayed_fields(&mut self) {
        self.displayed_fields = Setting::Reset;
    }

    pub fn set_displayed_fields(&mut self, names: Vec<String>) {
        self.displayed_fields = Setting::Set(names);
    }

    pub fn reset_filterable_fields(&mut self) {
        self.filterable_fields = Setting::Reset;
    }

    pub fn set_filterable_fields(&mut self, names: HashSet<String>) {
        self.filterable_fields = Setting::Set(names);
    }

    pub fn reset_criteria(&mut self) {
        self.criteria = Setting::Reset;
    }

    pub fn set_criteria(&mut self, criteria: Vec<String>) {
        self.criteria = Setting::Set(criteria);
    }

    pub fn reset_stop_words(&mut self) {
        self.stop_words = Setting::Reset;
    }

    pub fn set_stop_words(&mut self, stop_words: BTreeSet<String>) {
        self.stop_words = if stop_words.is_empty() {
            Setting::Reset
        } else {
            Setting::Set(stop_words)
        }
    }

    pub fn reset_distinct_attribute(&mut self) {
        self.distinct_attribute = Setting::Reset;
    }

    pub fn set_distinct_attribute(&mut self, distinct_attribute: String) {
        self.distinct_attribute = Setting::Set(distinct_attribute);
    }

    pub fn reset_synonyms(&mut self) {
        self.synonyms = Setting::Reset;
    }

    pub fn set_synonyms(&mut self, synonyms: HashMap<String, Vec<String>>) {
        self.synonyms = if synonyms.is_empty() {
            Setting::Reset
        } else {
            Setting::Set(synonyms)
        }
    }

    fn reindex<F>(&mut self, cb: &F, old_fields_ids_map: FieldsIdsMap) -> anyhow::Result<()>
        where
            F: Fn(UpdateIndexingStep, u64) + Sync
    {
        let fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
        let update_id = self.update_id;
        let cb = |step| cb(step, update_id);
        // if the settings are set before any document update, we don't need to do anything, and
        // will set the primary key during the first document addition.
        if self.index.number_of_documents(&self.wtxn)? == 0 {
            return Ok(());
        }

        let transform = Transform {
            rtxn: &self.wtxn,
            index: self.index,
            log_every_n: self.log_every_n,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            chunk_fusing_shrink_size: self.chunk_fusing_shrink_size,
            max_nb_chunks: self.max_nb_chunks,
            max_memory: self.max_memory,
            index_documents_method: IndexDocumentsMethod::ReplaceDocuments,
            autogenerate_docids: false,
        };

        // There already has been a document addition, the primary key should be set by now.
        let primary_key = self.index.primary_key(&self.wtxn)?.context("Index must have a primary key")?;

        // We remap the documents fields based on the new `FieldsIdsMap`.
        let output = transform.remap_index_documents(
            primary_key.to_string(),
            old_fields_ids_map,
            fields_ids_map.clone())?;

        // We clear the full database (words-fst, documents ids and documents content).
        ClearDocuments::new(self.wtxn, self.index, self.update_id).execute()?;

        // We index the generated `TransformOutput` which must contain
        // all the documents with fields in the newly defined searchable order.
        let mut indexing_builder = IndexDocuments::new(self.wtxn, self.index, self.update_id);
        indexing_builder.log_every_n = self.log_every_n;
        indexing_builder.max_nb_chunks = self.max_nb_chunks;
        indexing_builder.max_memory = self.max_memory;
        indexing_builder.linked_hash_map_size = self.linked_hash_map_size;
        indexing_builder.chunk_compression_type = self.chunk_compression_type;
        indexing_builder.chunk_compression_level = self.chunk_compression_level;
        indexing_builder.chunk_fusing_shrink_size = self.chunk_fusing_shrink_size;
        indexing_builder.thread_pool = self.thread_pool;
        indexing_builder.execute_raw(output, &cb)?;
        Ok(())
    }

    fn update_displayed(&mut self) -> anyhow::Result<bool> {
        match self.displayed_fields {
            Setting::Set(ref fields) => {
                let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                // fields are deduplicated, only the first occurrence is taken into account
                let names: Vec<_> = fields
                    .iter()
                    .unique()
                    .map(String::as_str)
                    .collect();

                for name in names.iter() {
                    fields_ids_map
                        .insert(name)
                        .context("field id limit exceeded")?;
                }
                self.index.put_displayed_fields(self.wtxn, &names)?;
                self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
            }
            Setting::Reset => { self.index.delete_displayed_fields(self.wtxn)?; }
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    fn update_distinct_attribute(&mut self) -> anyhow::Result<bool> {
        match self.distinct_attribute {
            Setting::Set(ref attr) => {
                let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                fields_ids_map
                    .insert(attr)
                    .context("field id limit exceeded")?;

                self.index.put_distinct_attribute(self.wtxn, &attr)?;
                self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
            }
            Setting::Reset => { self.index.delete_distinct_attribute(self.wtxn)?; },
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    /// Updates the index's searchable attributes. This causes the field map to be recomputed to
    /// reflect the order of the searchable attributes.
    fn update_searchable(&mut self) -> anyhow::Result<bool> {
        match self.searchable_fields {
            Setting::Set(ref fields) => {
                // every time the searchable attributes are updated, we need to update the
                // ids for any settings that uses the facets. (displayed_fields,
                // filterable_fields)
                let old_fields_ids_map = self.index.fields_ids_map(self.wtxn)?;

                let mut new_fields_ids_map = FieldsIdsMap::new();
                // fields are deduplicated, only the first occurrence is taken into account
                let names = fields
                    .iter()
                    .unique()
                    .map(String::as_str)
                    .collect::<Vec<_>>();

                // Add all the searchable attributes to the field map, and then add the
                // remaining fields from the old field map to the new one
                for name in names.iter() {
                    new_fields_ids_map
                        .insert(&name)
                        .context("field id limit exceeded")?;
                }

                for (_, name) in old_fields_ids_map.iter() {
                    new_fields_ids_map
                        .insert(&name)
                        .context("field id limit exceeded")?;
                }

                self.index.put_searchable_fields(self.wtxn, &names)?;
                self.index.put_fields_ids_map(self.wtxn, &new_fields_ids_map)?;
            }
            Setting::Reset => { self.index.delete_searchable_fields(self.wtxn)?; }
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    fn update_stop_words(&mut self) -> anyhow::Result<bool> {
        match self.stop_words {
            Setting::Set(ref stop_words) => {
                let current = self.index.stop_words(self.wtxn)?;
                // since we can't compare a BTreeSet with an FST we are going to convert the
                // BTreeSet to an FST and then compare bytes per bytes the two FSTs.
                let fst = fst::Set::from_iter(stop_words)?;

                // Does the new FST differ from the previous one?
                if current.map_or(true, |current| current.as_fst().as_bytes() != fst.as_fst().as_bytes()) {
                    // we want to re-create our FST.
                    self.index.put_stop_words(self.wtxn, &fst)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_stop_words(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_synonyms(&mut self) -> anyhow::Result<bool> {
        match self.synonyms {
            Setting::Set(ref synonyms) => {
                fn normalize(analyzer: &Analyzer<&[u8]>, text: &str) -> Vec<String> {
                    analyzer
                        .analyze(text)
                        .tokens()
                        .filter_map(|token|
                            if token.is_word() { Some(token.text().to_string()) } else { None }
                        )
                        .collect::<Vec<_>>()
                }

                let mut config = AnalyzerConfig::default();
                let stop_words = self.index.stop_words(self.wtxn)?;
                if let Some(stop_words) = &stop_words {
                    config.stop_words(stop_words);
                }
                let analyzer = Analyzer::new(config);

                let mut new_synonyms = HashMap::new();
                for (word, synonyms) in synonyms {
                    // Normalize both the word and associated synonyms.
                    let normalized_word = normalize(&analyzer, word);
                    let normalized_synonyms = synonyms
                        .iter()
                        .map(|synonym| normalize(&analyzer, synonym));

                    // Store the normalized synonyms under the normalized word,
                    // merging the possible duplicate words.
                    let entry = new_synonyms
                        .entry(normalized_word)
                        .or_insert_with(Vec::new);
                    entry.extend(normalized_synonyms);
                }

                // Make sure that we don't have duplicate synonyms.
                new_synonyms
                    .iter_mut()
                    .for_each(|(_, synonyms)| {
                        synonyms.sort_unstable();
                        synonyms.dedup();
                    });

                let old_synonyms = self.index.synonyms(self.wtxn)?;

                if new_synonyms != old_synonyms {
                    self.index.put_synonyms(self.wtxn, &new_synonyms)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_synonyms(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_facets(&mut self) -> anyhow::Result<bool> {
        match self.filterable_fields {
            Setting::Set(ref fields) => {
                let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                let mut new_facets = HashSet::new();
                for name in fields {
                    fields_ids_map.insert(name).context("field id limit exceeded")?;
                    new_facets.insert(name.clone());
                }
                self.index.put_filterable_fields(self.wtxn, &new_facets)?;
                self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
            }
            Setting::Reset => { self.index.delete_filterable_fields(self.wtxn)?; }
            Setting::NotSet => return Ok(false)
        }
        Ok(true)
    }

    fn update_criteria(&mut self) -> anyhow::Result<()> {
        match self.criteria {
            Setting::Set(ref fields) => {
                let filterable_fields = self.index.filterable_fields(&self.wtxn)?;
                let mut new_criteria = Vec::new();
                for name in fields {
                    let criterion = Criterion::from_str(&filterable_fields, &name)?;
                    new_criteria.push(criterion);
                }
                self.index.put_criteria(self.wtxn, &new_criteria)?;
            }
            Setting::Reset => { self.index.delete_criteria(self.wtxn)?; }
            Setting::NotSet => (),
        }
        Ok(())
    }

    pub fn execute<F>(mut self, progress_callback: F) -> anyhow::Result<()>
        where
            F: Fn(UpdateIndexingStep, u64) + Sync
    {
        self.index.set_updated_at(self.wtxn, &Utc::now())?;
        let old_fields_ids_map = self.index.fields_ids_map(&self.wtxn)?;
        self.update_displayed()?;
        let stop_words_updated = self.update_stop_words()?;
        let facets_updated = self.update_facets()?;
        self.update_distinct_attribute()?;
        // update_criteria MUST be called after update_facets, since criterion fields must be set
        // as facets.
        self.update_criteria()?;
        let synonyms_updated = self.update_synonyms()?;
        let searchable_updated = self.update_searchable()?;

        if stop_words_updated || facets_updated || synonyms_updated || searchable_updated {
            self.reindex(&progress_callback, old_fields_ids_map)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use heed::EnvOpenOptions;
    use heed::types::ByteSlice;
    use maplit::{btreeset, hashmap, hashset};
    use big_s::S;

    use crate::update::{IndexDocuments, UpdateFormat};

    use super::*;

    #[test]
    fn set_and_reset_searchable_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name,age\n0,kevin,23\n1,kevina,21\n2,benoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 1);
        builder.set_searchable_fields(vec!["name".into()]);
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the searchable field is correctly set to "name" only.
        let rtxn = index.read_txn().unwrap();
        // When we search for something that is not in
        // the searchable fields it must not return any document.
        let result = index.search(&rtxn).query("23").execute().unwrap();
        assert!(result.documents_ids.is_empty());

        // When we search for something that is in the searchable fields
        // we must find the appropriate document.
        let result = index.search(&rtxn).query(r#""kevin""#).execute().unwrap();
        let documents = index.documents(&rtxn, result.documents_ids).unwrap();
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].1.get(0), Some(&br#""kevin""#[..]));
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 2);
        builder.reset_searchable_fields();
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the searchable field have been reset and documents are found now.
        let rtxn = index.read_txn().unwrap();
        let searchable_fields = index.searchable_fields(&rtxn).unwrap();
        assert_eq!(searchable_fields, None);
        let result = index.search(&rtxn).query("23").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let documents = index.documents(&rtxn, result.documents_ids).unwrap();
        assert_eq!(documents[0].1.get(0), Some(&br#""kevin""#[..]));
    }

    #[test]
    fn mixup_searchable_with_displayed_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // In the same transaction we change the displayed fields to be only the "age".
        // We also change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 1);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.set_searchable_fields(vec!["name".into()]);
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), (&["age"][..]));
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 2);
        builder.reset_searchable_fields();
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields always contains only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &["age"][..]);
    }

    #[test]
    fn default_displayed_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
    }

    #[test]
    fn set_and_reset_displayed_field() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();

        // In the same transaction we change the displayed fields to be only the age.
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &["age"][..]);
        drop(rtxn);

        // We reset the fields ids to become `None`, the default value.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.reset_displayed_fields();
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
    }

    #[test]
    fn set_filterable_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the age.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_filterable_fields(hashset!{ S("age") });
        builder.execute(|_, _| ()).unwrap();

        // Then index some documents.
        let content = &br#"[
            { "name": "kevin",  "age": 23 },
            { "name": "kevina", "age": 21 },
            { "name": "benoit", "age": 34 }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Json);
        builder.enable_autogenerate_docids();
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.filterable_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashset!{ S("age") });
        // Only count the field_id 0 and level 0 facet values.
        // TODO we must support typed CSVs for numbers to be understood.
        let count = index.facet_id_f64_docids
            .remap_key_type::<ByteSlice>()
            .prefix_iter(&rtxn, &[0, 0]).unwrap().count();
        assert_eq!(count, 3);
        drop(rtxn);

        // Index a little more documents with new and current facets values.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "name": "kevin2",  "age": 23 },
            { "name": "kevina2", "age": 21 },
            { "name": "benoit",  "age": 35 }
        ]"#[..];

        let mut builder = IndexDocuments::new(&mut wtxn, &index, 2);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 0 and level 0 facet values.
        // TODO we must support typed CSVs for numbers to be understood.
        let count = index.facet_id_f64_docids
            .remap_key_type::<ByteSlice>()
            .prefix_iter(&rtxn, &[0, 0]).unwrap().count();
        assert_eq!(count, 4);
    }

    #[test]
    fn default_stop_words() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Ensure there is no stop_words by default
        let rtxn = index.read_txn().unwrap();
        let stop_words = index.stop_words(&rtxn).unwrap();
        assert!(stop_words.is_none());
    }

    #[test]
    fn set_and_reset_stop_words() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age,maxim\nkevin,23,I love dogs\nkevina,21,Doggos are the best\nbenoit,34,The crepes are really good\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();

        // In the same transaction we provide some stop_words
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        let set = btreeset! { "i".to_string(), "the".to_string(), "are".to_string() };
        builder.set_stop_words(set.clone());
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Ensure stop_words are effectively stored
        let rtxn = index.read_txn().unwrap();
        let stop_words = index.stop_words(&rtxn).unwrap();
        assert!(stop_words.is_some()); // at this point the index should return something

        let stop_words = stop_words.unwrap();
        let expected = fst::Set::from_iter(&set).unwrap();
        assert_eq!(stop_words.as_fst().as_bytes(), expected.as_fst().as_bytes());

        // when we search for something that is a non prefix stop_words it should be ignored
        // thus we should get a placeholder search (all the results = 3)
        let result = index.search(&rtxn).query("the ").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 3);
        let result = index.search(&rtxn).query("i ").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 3);
        let result = index.search(&rtxn).query("are ").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 3);

        let result = index.search(&rtxn).query("dog").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2); // we have two maxims talking about doggos
        let result = index.search(&rtxn).query("benoît").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1); // there is one benoit in our data

        // now we'll reset the stop_words and ensure it's None
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.reset_stop_words();
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        let stop_words = index.stop_words(&rtxn).unwrap();
        assert!(stop_words.is_none());

        // now we can search for the stop words
        let result = index.search(&rtxn).query("the").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);
        let result = index.search(&rtxn).query("i").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let result = index.search(&rtxn).query("are").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);

        // the rest of the search is still not impacted
        let result = index.search(&rtxn).query("dog").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2); // we have two maxims talking about doggos
        let result = index.search(&rtxn).query("benoît").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1); // there is one benoit in our data
    }

    #[test]
    fn set_and_reset_synonyms() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age,maxim\nkevin,23,I love dogs\nkevina,21,Doggos are the best\nbenoit,34,The crepes are really good\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();

        // In the same transaction provide some synonyms
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_synonyms(hashmap! {
            "blini".to_string() => vec!["crepes".to_string()],
            "super like".to_string() => vec!["love".to_string()],
            "puppies".to_string() => vec!["dogs".to_string(), "doggos".to_string()]
        });
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Ensure synonyms are effectively stored
        let rtxn = index.read_txn().unwrap();
        let synonyms = index.synonyms(&rtxn).unwrap();
        assert!(!synonyms.is_empty()); // at this point the index should return something

        // Check that we can use synonyms
        let result = index.search(&rtxn).query("blini").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let result = index.search(&rtxn).query("super like").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let result = index.search(&rtxn).query("puppies").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);

        // Reset the synonyms
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.reset_synonyms();
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Ensure synonyms are reset
        let rtxn = index.read_txn().unwrap();
        let synonyms = index.synonyms(&rtxn).unwrap();
        assert!(synonyms.is_empty());

        // Check that synonyms are no longer work
        let result = index.search(&rtxn).query("blini").execute().unwrap();
        assert!(result.documents_ids.is_empty());
        let result = index.search(&rtxn).query("super like").execute().unwrap();
        assert!(result.documents_ids.is_empty());
        let result = index.search(&rtxn).query("puppies").execute().unwrap();
        assert!(result.documents_ids.is_empty());
    }

    #[test]
    fn setting_searchable_recomputes_other_settings() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set all the settings except searchable
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_displayed_fields(vec!["hello".to_string()]);
        builder.set_filterable_fields(hashset!{ S("age"), S("toto") });
        builder.set_criteria(vec!["asc(toto)".to_string()]);
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // check the output
        let rtxn = index.read_txn().unwrap();
        assert_eq!(&["hello"][..], index.displayed_fields(&rtxn).unwrap().unwrap());
        // since no documents have been pushed the primary key is still unset
        assert!(index.primary_key(&rtxn).unwrap().is_none());
        assert_eq!(vec![Criterion::Asc("toto".to_string())], index.criteria(&rtxn).unwrap());
        drop(rtxn);

        // We set toto and age as searchable to force reordering of the fields
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 1);
        builder.set_searchable_fields(vec!["toto".to_string(), "age".to_string()]);
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        assert_eq!(&["hello"][..], index.displayed_fields(&rtxn).unwrap().unwrap());
        assert!(index.primary_key(&rtxn).unwrap().is_none());
        assert_eq!(vec![Criterion::Asc("toto".to_string())], index.criteria(&rtxn).unwrap());
    }
}
