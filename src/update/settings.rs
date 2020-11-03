use anyhow::Context;
use grenad::CompressionType;
use rayon::ThreadPool;

use crate::update::index_documents::{Transform, IndexDocumentsMethod};
use crate::update::{ClearDocuments, IndexDocuments};
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

    pub fn execute<F>(self, progress_callback: F) -> anyhow::Result<()>
    where
        F: Fn(usize, usize) + Sync
    {
        // Check that the searchable attributes have been specified.
        if let Some(value) = self.searchable_fields {
            let current_displayed_fields = self.index.displayed_fields(self.wtxn)?;
            let current_fields_ids_map = self.index.fields_ids_map(self.wtxn)?;

            let result = match value {
                Some(fields_names) => {
                    // We create or generate the fields ids corresponding to those names.
                    let mut fields_ids_map = FieldsIdsMap::new();
                    let mut searchable_fields = Vec::new();
                    for name in fields_names {
                        let id = fields_ids_map.insert(&name).context("field id limit reached")?;
                        searchable_fields.push(id);
                    }

                    // We complete the new FieldsIdsMap with the previous names.
                    for (_id, name) in current_fields_ids_map.iter() {
                        fields_ids_map.insert(name).context("field id limit reached")?;
                    }

                    // We must also update the displayed fields according to the new `FieldsIdsMap`.
                    let displayed_fields = match current_displayed_fields {
                        Some(fields) => {
                            let mut displayed_fields = Vec::new();
                            for id in fields {
                                let name = current_fields_ids_map.name(*id).unwrap();
                                let id = fields_ids_map.id(name).context("field id limit reached")?;
                                displayed_fields.push(id);
                            }
                            Some(displayed_fields)
                        },
                        None => None,
                    };

                    (fields_ids_map, Some(searchable_fields), displayed_fields)
                },
                None => (
                    current_fields_ids_map.clone(),
                    None,
                    current_displayed_fields.map(ToOwned::to_owned),
                ),
            };

            let (mut fields_ids_map, searchable_fields, displayed_fields) = result;

            let transform = Transform {
                rtxn: &self.wtxn,
                index: self.index,
                chunk_compression_type: self.chunk_compression_type,
                chunk_compression_level: self.chunk_compression_level,
                chunk_fusing_shrink_size: self.chunk_fusing_shrink_size,
                max_nb_chunks: self.max_nb_chunks,
                max_memory: self.max_memory,
                index_documents_method: IndexDocumentsMethod::ReplaceDocuments,
                autogenerate_docids: false,
            };

            // We compute or generate the new primary key field id.
            let primary_key = match self.index.primary_key(&self.wtxn)? {
                Some(id) => {
                    let name = current_fields_ids_map.name(id).unwrap();
                    fields_ids_map.insert(name).context("field id limit reached")?
                },
                None => fields_ids_map.insert("id").context("field id limit reached")?,
            };

            // We remap the documents fields based on the new `FieldsIdsMap`.
            let output = transform.remap_index_documents(primary_key, fields_ids_map.clone())?;

            // We write the new FieldsIdsMap to the database
            // this way next indexing methods will be based on that.
            self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;

            // The new searchable fields are also written down to make sure
            // that the IndexDocuments system takes only these ones into account.
            match searchable_fields {
                Some(fields) => self.index.put_searchable_fields(self.wtxn, &fields)?,
                None => self.index.delete_searchable_fields(self.wtxn).map(drop)?,
            }

            // We write the displayed fields into the database here
            // to make sure that the right fields are displayed.
            match displayed_fields {
                Some(fields) => self.index.put_displayed_fields(self.wtxn, &fields)?,
                None => self.index.delete_displayed_fields(self.wtxn).map(drop)?,
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
            indexing_builder.execute_raw(output, progress_callback)?;
        }

        // Check that the displayed attributes have been specified.
        if let Some(value) = self.displayed_fields {
            match value {
                // If it has been set, and it was a list of fields names, we create
                // or generate the fields ids corresponds to those names and store them
                // in the database in the order they were specified.
                Some(fields_names) => {
                    let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;

                    // We create or generate the fields ids corresponding to those names.
                    let mut fields_ids = Vec::new();
                    for name in fields_names {
                        let id = fields_ids_map.insert(&name).context("field id limit reached")?;
                        fields_ids.push(id);
                    }

                    self.index.put_displayed_fields(self.wtxn, &fields_ids)?;
                },
                // If it was set to `null` it means that the user wants to get the default behavior
                // which is displaying all the attributes in no specific order (FieldsIdsMap order),
                // we just have to delete the displayed fields.
                None => {
                    self.index.delete_displayed_fields(self.wtxn)?;
                },
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
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();

        // In the same transaction we change the displayed fields to be only the age.
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.execute(|_, _| ()).unwrap();
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
        builder.execute(|_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
        drop(rtxn);
    }
}
