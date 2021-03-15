use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Context;
use chrono::Utc;
use grenad::CompressionType;
use itertools::Itertools;
use rayon::ThreadPool;

use crate::criterion::Criterion;
use crate::facet::FacetType;
use crate::update::index_documents::{Transform, IndexDocumentsMethod};
use crate::update::{ClearDocuments, IndexDocuments, UpdateIndexingStep};
use crate::{Index, FieldsIdsMap};

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

    // If a struct field is set to `None` it means that it hasn't been set by the user,
    // however if it is `Some(None)` it means that the user forced a reset of the setting.
    searchable_fields: Option<Option<Vec<String>>>,
    displayed_fields: Option<Option<Vec<String>>>,
    faceted_fields: Option<Option<HashMap<String, String>>>,
    criteria: Option<Option<Vec<String>>>,
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
            searchable_fields: None,
            displayed_fields: None,
            faceted_fields: None,
            criteria: None,
            update_id,
        }
    }

    pub fn reset_searchable_fields(&mut self) {
        self.searchable_fields = Some(None);
    }

    pub fn set_searchable_fields(&mut self, names: Vec<String>) {
        self.searchable_fields = Some(Some(names));
    }

    pub fn reset_displayed_fields(&mut self) {
        self.displayed_fields = Some(None);
    }

    pub fn set_displayed_fields(&mut self, names: Vec<String>) {
        self.displayed_fields = Some(Some(names));
    }

    pub fn set_faceted_fields(&mut self, names_facet_types: HashMap<String, String>) {
        self.faceted_fields = Some(Some(names_facet_types));
    }

    pub fn reset_faceted_fields(&mut self) {
        self.faceted_fields = Some(None);
    }

    pub fn reset_criteria(&mut self) {
        self.criteria = Some(None);
    }

    pub fn set_criteria(&mut self, criteria: Vec<String>) {
        self.criteria = Some(Some(criteria));
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
            return Ok(())
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
            Some(Some(ref fields)) => {
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
            Some(None) => { self.index.delete_displayed_fields(self.wtxn)?; },
            None => return Ok(false),
        }
        Ok(true)
    }

    /// Udpates the index's searchable attributes. This causes the field map to be recomputed to
    /// reflect the order of the searchable attributes.
    fn update_searchable(&mut self) -> anyhow::Result<bool> {
        match self.searchable_fields {
            Some(Some(ref fields)) => {
                // every time the searchable attributes are updated, we need to update the
                // ids for any settings that uses the facets. (displayed_fields,
                // faceted_fields)
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
            Some(None) => { self.index.delete_searchable_fields(self.wtxn)?; },
            None => return Ok(false),
        }
        Ok(true)
    }

    fn update_facets(&mut self) -> anyhow::Result<bool> {
        match self.faceted_fields {
            Some(Some(ref fields)) => {
                let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                let mut new_facets = HashMap::new();
                for (name, ty) in fields {
                    fields_ids_map.insert(name).context("field id limit exceeded")?;
                    let ty = FacetType::from_str(&ty)?;
                    new_facets.insert(name.clone(), ty);
                }
                self.index.put_faceted_fields(self.wtxn, &new_facets)?;
                self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
            }
            Some(None) => { self.index.delete_faceted_fields(self.wtxn)?; },
            None => return Ok(false)
        }
        Ok(true)
    }

    fn update_criteria(&mut self) -> anyhow::Result<()> {
        match self.criteria {
            Some(Some(ref fields)) => {
                let faceted_fields = self.index.faceted_fields(&self.wtxn)?;
                let mut new_criteria = Vec::new();
                for name in fields {
                    let criterion = Criterion::from_str(&faceted_fields, &name)?;
                    new_criteria.push(criterion);
                }
                self.index.put_criteria(self.wtxn, &new_criteria)?;
            }
            Some(None) => { self.index.delete_criteria(self.wtxn)?; }
            None => (),
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
            let facets_updated = self.update_facets()?;
            // update_criteria MUST be called after update_facets, since criterion fields must be set
            // as facets.
            self.update_criteria()?;
            let searchable_updated = self.update_searchable()?;

            if facets_updated || searchable_updated {
                self.reindex(&progress_callback, old_fields_ids_map)?;
            }
            Ok(())
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    use heed::EnvOpenOptions;
    use maplit::hashmap;

    use crate::facet::FacetType;
    use crate::update::{IndexDocuments, UpdateFormat};

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
        drop(rtxn);
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
        drop(rtxn);
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
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
        drop(rtxn);
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
        drop(rtxn);
    }

    #[test]
    fn set_faceted_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the age.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, 0);
        builder.set_faceted_fields(hashmap!{ "age".into() => "integer".into() });
        builder.execute(|_, _| ()).unwrap();

        // Then index some documents.
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.faceted_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashmap!{ "age".to_string() => FacetType::Integer });
        // Only count the field_id 0 and level 0 facet values.
        let count = index.facet_field_id_value_docids.prefix_iter(&rtxn, &[0, 0]).unwrap().count();
        assert_eq!(count, 3);
        drop(rtxn);

        // Index a little more documents with new and current facets values.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin2,23\nkevina2,21\nbenoit2,35\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 2);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 0 and level 0 facet values.
        let count = index.facet_field_id_value_docids.prefix_iter(&rtxn, &[0, 0]).unwrap().count();
        assert_eq!(count, 4);
        drop(rtxn);
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
        builder.set_faceted_fields(hashmap!{
            "age".into() => "integer".into(),
            "toto".into() => "integer".into(),
        });
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
        drop(rtxn);
    }
}
