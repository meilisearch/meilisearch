use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{ensure, Context};
use grenad::CompressionType;
use rayon::ThreadPool;

use crate::update::index_documents::{Transform, IndexDocumentsMethod};
use crate::update::{ClearDocuments, IndexDocuments, UpdateIndexingStep};
use crate::facet::FacetType;
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

    // If a struct field is set to `None` it means that it hasn't been set by the user,
    // however if it is `Some(None)` it means that the user forced a reset of the setting.
    searchable_fields: Option<Option<Vec<String>>>,
    displayed_fields: Option<Option<Vec<String>>>,
    faceted_fields: Option<HashMap<String, String>>,
}

impl<'a, 't, 'u, 'i> Settings<'a, 't, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> Settings<'a, 't, 'u, 'i> {
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
        self.faceted_fields = Some(names_facet_types);
    }

    pub fn execute<F>(self, progress_callback: F) -> anyhow::Result<()>
    where
        F: Fn(UpdateIndexingStep) + Sync
    {
        let mut updated_searchable_fields = None;
        let mut updated_faceted_fields = None;
        let mut updated_displayed_fields = None;

        // Construct the new FieldsIdsMap based on the searchable fields order.
        let fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
        let mut fields_ids_map = match self.searchable_fields {
            Some(Some(searchable_fields)) => {
                let mut new_fields_ids_map = FieldsIdsMap::new();
                let mut new_searchable_fields = Vec::new();

                for name in searchable_fields {
                    let id = new_fields_ids_map.insert(&name).context("field id limit reached")?;
                    new_searchable_fields.push(id);
                }

                for (_, name) in fields_ids_map.iter() {
                    new_fields_ids_map.insert(name).context("field id limit reached")?;
                }

                updated_searchable_fields = Some(Some(new_searchable_fields));
                new_fields_ids_map
            },
            Some(None) => {
                updated_searchable_fields = Some(None);
                fields_ids_map
            },
            None => fields_ids_map,
        };

        // We compute or generate the new primary key field id.
        // TODO make the primary key settable.
        let primary_key = match self.index.primary_key(&self.wtxn)? {
            Some(id) => {
                let current_fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                let name = current_fields_ids_map.name(id).unwrap();
                fields_ids_map.insert(name).context("field id limit reached")?
            },
            None => fields_ids_map.insert("id").context("field id limit reached")?,
        };

        if let Some(fields_names_facet_types) = self.faceted_fields {
            let current_faceted_fields = self.index.faceted_fields(self.wtxn)?;

            let mut faceted_fields = HashMap::new();
            for (name, sftype) in fields_names_facet_types {
                let ftype = FacetType::from_str(&sftype).with_context(|| format!("parsing facet type {:?}", sftype))?;
                let id = fields_ids_map.insert(&name).context("field id limit reached")?;
                match current_faceted_fields.get(&id) {
                    Some(pftype) => {
                        ensure!(ftype == *pftype, "{} facet type changed from {} to {}", name, ftype, pftype);
                        faceted_fields.insert(id, ftype)
                    },
                    None => faceted_fields.insert(id, ftype),
                };
            }

            updated_faceted_fields = Some(faceted_fields);
        }

        // Check that the displayed attributes have been specified.
        if let Some(value) = self.displayed_fields {
            match value {
                Some(names) => {
                    let mut new_displayed_fields = Vec::new();
                    for name in names {
                        let id = fields_ids_map.insert(&name).context("field id limit reached")?;
                        new_displayed_fields.push(id);
                    }
                    updated_displayed_fields = Some(Some(new_displayed_fields));
                }
                None => updated_displayed_fields = Some(None),
            }
        }

        // If any setting have modified any of the datastructures it means that we need
        // to retrieve the documents and then reindex then with the new settings.
        if updated_searchable_fields.is_some() || updated_faceted_fields.is_some() {
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

            // We remap the documents fields based on the new `FieldsIdsMap`.
            let output = transform.remap_index_documents(primary_key, fields_ids_map.clone())?;

            // We write the new FieldsIdsMap to the database
            // this way next indexing methods will be based on that.
            self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;

            if let Some(faceted_fields) = updated_faceted_fields {
                // We write the faceted_fields fields into the database here.
                self.index.put_faceted_fields(self.wtxn, &faceted_fields)?;
            }

            if let Some(searchable_fields) = updated_searchable_fields {
                // The new searchable fields are also written down to make sure
                // that the IndexDocuments system takes only these ones into account.
                match searchable_fields {
                    Some(fields) => self.index.put_searchable_fields(self.wtxn, &fields)?,
                    None => self.index.delete_searchable_fields(self.wtxn).map(drop)?,
                }
            }

            // We clear the full database (words-fst, documents ids and documents content).
            ClearDocuments::new(self.wtxn, self.index).execute()?;

            // We index the generated `TransformOutput` which must contain
            // all the documents with fields in the newly defined searchable order.
            let mut indexing_builder = IndexDocuments::new(self.wtxn, self.index);
            indexing_builder.log_every_n = self.log_every_n;
            indexing_builder.max_nb_chunks = self.max_nb_chunks;
            indexing_builder.max_memory = self.max_memory;
            indexing_builder.linked_hash_map_size = self.linked_hash_map_size;
            indexing_builder.chunk_compression_type = self.chunk_compression_type;
            indexing_builder.chunk_compression_level = self.chunk_compression_level;
            indexing_builder.chunk_fusing_shrink_size = self.chunk_fusing_shrink_size;
            indexing_builder.thread_pool = self.thread_pool;
            indexing_builder.execute_raw(output, &progress_callback)?;
        }

        if let Some(displayed_fields) = updated_displayed_fields {
            // We write the displayed fields into the database here
            // to make sure that the right fields are displayed.
            match displayed_fields {
                Some(fields) => self.index.put_displayed_fields(self.wtxn, &fields)?,
                None => self.index.delete_displayed_fields(self.wtxn).map(drop)?,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::update::{IndexDocuments, UpdateFormat};
    use heed::EnvOpenOptions;
    use maplit::hashmap;

    #[test]
    fn set_and_reset_searchable_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_searchable_fields(vec!["name".into()]);
        builder.execute(|_| ()).unwrap();
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
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_searchable_fields();
        builder.execute(|_| ()).unwrap();
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
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // In the same transaction we change the displayed fields to be only the "age".
        // We also change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.set_searchable_fields(vec!["name".into()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        let age_id = fields_ids_map.id("age").unwrap();
        assert_eq!(fields_ids, Some(&[age_id][..]));
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_searchable_fields();
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields always contains only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        let age_id = fields_ids_map.id("age").unwrap();
        assert_eq!(fields_ids, Some(&[age_id][..]));
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
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
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
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();

        // In the same transaction we change the displayed fields to be only the age.
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let age_field_id = fields_ids_map.id("age").unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &[age_field_id][..]);
        drop(rtxn);

        // We reset the fields ids to become `None`, the default value.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_displayed_fields();
        builder.execute(|_| ()).unwrap();
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
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_faceted_fields(hashmap!{ "age".into() => "integer".into() });
        builder.execute(|_| ()).unwrap();

        // Then index some documents.
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.faceted_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashmap!{ 1 => FacetType::Integer });
        // Only count the field_id 0 and level 0 facet values.
        let count = index.facet_field_id_value_docids.prefix_iter(&rtxn, &[1, 0]).unwrap().count();
        assert_eq!(count, 3);
        drop(rtxn);

        // Index a little more documents with new and current facets values.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin2,23\nkevina2,21\nbenoit2,35\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 0 and level 0 facet values.
        let count = index.facet_field_id_value_docids.prefix_iter(&rtxn, &[1, 0]).unwrap().count();
        assert_eq!(count, 4);
        drop(rtxn);
    }
}
