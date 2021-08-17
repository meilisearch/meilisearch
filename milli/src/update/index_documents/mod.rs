mod extract;
mod helpers;
mod transform;
mod typed_chunk;

use std::collections::HashSet;
use std::io::{self, BufRead, BufReader};
use std::iter::FromIterator;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Instant;

use byte_unit::Byte;
use chrono::Utc;
use crossbeam_channel::{Receiver, Sender};
use grenad::{self, CompressionType};
use log::{debug, info};
use rayon::ThreadPool;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use typed_chunk::{write_typed_chunk_into_index, TypedChunk};

pub use self::helpers::{
    create_sorter, create_writer, merge_cbo_roaring_bitmaps, merge_roaring_bitmaps,
    sorter_into_lmdb_database, write_into_lmdb_database, writer_into_reader,
};
use self::helpers::{grenad_obkv_into_chunks, GrenadParameters};
pub use self::transform::{Transform, TransformOutput};
use crate::update::{
    Facets, UpdateBuilder, UpdateIndexingStep, WordPrefixDocids, WordPrefixPairProximityDocids,
    WordsLevelPositions, WordsPrefixesFst,
};
use crate::{Index, Result};

static MERGED_DATABASE_COUNT: usize = 7;
static PREFIX_DATABASE_COUNT: usize = 5;
static TOTAL_POSTING_DATABASE_COUNT: usize = MERGED_DATABASE_COUNT + PREFIX_DATABASE_COUNT;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocumentAdditionResult {
    pub nb_documents: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

#[derive(Debug, Copy, Clone)]
pub enum WriteMethod {
    Append,
    GetMergePut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum UpdateFormat {
    /// The given update is a real **comma seperated** CSV with headers on the first line.
    Csv,
    /// The given update is a JSON array with documents inside.
    Json,
    /// The given update is a JSON stream with a document on each line.
    JsonStream,
}

pub struct IndexDocuments<'t, 'u, 'i, 'a> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) log_every_n: Option<usize>,
    pub(crate) documents_chunk_size: Option<usize>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) thread_pool: Option<&'a ThreadPool>,
    facet_level_group_size: Option<NonZeroUsize>,
    facet_min_level_size: Option<NonZeroUsize>,
    words_prefix_threshold: Option<f64>,
    max_prefix_length: Option<usize>,
    words_positions_level_group_size: Option<NonZeroU32>,
    words_positions_min_level_size: Option<NonZeroU32>,
    update_method: IndexDocumentsMethod,
    update_format: UpdateFormat,
    autogenerate_docids: bool,
    update_id: u64,
}

impl<'t, 'u, 'i, 'a> IndexDocuments<'t, 'u, 'i, 'a> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        update_id: u64,
    ) -> IndexDocuments<'t, 'u, 'i, 'a> {
        IndexDocuments {
            wtxn,
            index,
            log_every_n: None,
            documents_chunk_size: None,
            max_nb_chunks: None,
            max_memory: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            thread_pool: None,
            facet_level_group_size: None,
            facet_min_level_size: None,
            words_prefix_threshold: None,
            max_prefix_length: None,
            words_positions_level_group_size: None,
            words_positions_min_level_size: None,
            update_method: IndexDocumentsMethod::ReplaceDocuments,
            update_format: UpdateFormat::Json,
            autogenerate_docids: false,
            update_id,
        }
    }

    pub fn index_documents_method(&mut self, method: IndexDocumentsMethod) {
        self.update_method = method;
    }

    pub fn update_format(&mut self, format: UpdateFormat) {
        self.update_format = format;
    }

    pub fn enable_autogenerate_docids(&mut self) {
        self.autogenerate_docids = true;
    }

    pub fn disable_autogenerate_docids(&mut self) {
        self.autogenerate_docids = false;
    }

    pub fn execute<R, F>(self, reader: R, progress_callback: F) -> Result<DocumentAdditionResult>
    where
        R: io::Read,
        F: Fn(UpdateIndexingStep, u64) + Sync,
    {
        let mut reader = BufReader::new(reader);
        reader.fill_buf()?;

        // Early return when there is no document to add
        if reader.buffer().is_empty() {
            return Ok(DocumentAdditionResult { nb_documents: 0 });
        }

        self.index.set_updated_at(self.wtxn, &Utc::now())?;
        let before_transform = Instant::now();
        let update_id = self.update_id;
        let progress_callback = |step| progress_callback(step, update_id);
        let transform = Transform {
            rtxn: &self.wtxn,
            index: self.index,
            log_every_n: self.log_every_n,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            max_nb_chunks: self.max_nb_chunks,
            max_memory: self.max_memory,
            index_documents_method: self.update_method,
            autogenerate_docids: self.autogenerate_docids,
        };

        let output = match self.update_format {
            UpdateFormat::Csv => transform.output_from_csv(reader, &progress_callback)?,
            UpdateFormat::Json => transform.output_from_json(reader, &progress_callback)?,
            UpdateFormat::JsonStream => {
                transform.output_from_json_stream(reader, &progress_callback)?
            }
        };

        let nb_documents = output.documents_count;

        info!("Update transformed in {:.02?}", before_transform.elapsed());

        self.execute_raw(output, progress_callback)?;
        Ok(DocumentAdditionResult { nb_documents })
    }

    pub fn execute_raw<F>(self, output: TransformOutput, progress_callback: F) -> Result<()>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let TransformOutput {
            primary_key,
            fields_ids_map,
            field_distribution,
            external_documents_ids,
            new_documents_ids,
            replaced_documents_ids,
            documents_count,
            documents_file,
        } = output;

        // The fields_ids_map is put back to the store now so the rest of the transaction sees an
        // up to date field map.
        self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;

        let backup_pool;
        let pool = match self.thread_pool {
            Some(pool) => pool,
            #[cfg(not(test))]
            None => {
                // We initialize a bakcup pool with the default
                // settings if none have already been set.
                backup_pool = rayon::ThreadPoolBuilder::new().build()?;
                &backup_pool
            }
            #[cfg(test)]
            None => {
                // We initialize a bakcup pool with the default
                // settings if none have already been set.
                backup_pool = rayon::ThreadPoolBuilder::new().num_threads(1).build()?;
                &backup_pool
            }
        };

        let documents_file = grenad::Reader::new(documents_file)?;

        // create LMDB writer channel
        let (lmdb_writer_sx, lmdb_writer_rx): (Sender<TypedChunk>, Receiver<TypedChunk>) =
            crossbeam_channel::unbounded();

        // get searchable fields for word databases
        let searchable_fields =
            self.index.searchable_fields_ids(self.wtxn)?.map(HashSet::from_iter);
        // get filterable fields for facet databases
        let faceted_fields = self.index.faceted_fields_ids(self.wtxn)?;

        // Run extraction pipeline in parallel.
        pool.install(|| {
            let params = GrenadParameters {
                chunk_compression_type: self.chunk_compression_type,
                chunk_compression_level: self.chunk_compression_level,
                max_memory: self.max_memory,
                max_nb_chunks: self.max_nb_chunks, // default value, may be chosen.
            };

            // split obkv file into several chuncks
            let mut chunk_iter = grenad_obkv_into_chunks(
                documents_file,
                params.clone(),
                self.log_every_n,
                Byte::from_bytes(self.documents_chunk_size.unwrap_or(1024 * 1024 * 128) as u64), // 128MiB
            )
            .unwrap();
            // extract all databases from the chunked obkv douments
            extract::data_from_obkv_documents(
                &mut chunk_iter,
                params,
                lmdb_writer_sx,
                searchable_fields,
                faceted_fields,
            )
            .unwrap();
        });

        // We delete the documents that this document addition replaces. This way we are
        // able to simply insert all the documents even if they already exist in the database.
        if !replaced_documents_ids.is_empty() {
            let update_builder = UpdateBuilder {
                log_every_n: self.log_every_n,
                max_nb_chunks: self.max_nb_chunks,
                max_memory: self.max_memory,
                chunk_compression_type: self.chunk_compression_type,
                chunk_compression_level: self.chunk_compression_level,
                thread_pool: self.thread_pool,
                update_id: self.update_id,
            };
            let mut deletion_builder = update_builder.delete_documents(self.wtxn, self.index)?;
            debug!("documents to delete {:?}", replaced_documents_ids);
            deletion_builder.delete_documents(&replaced_documents_ids);
            let deleted_documents_count = deletion_builder.execute()?;
            debug!("{} documents actually deleted", deleted_documents_count);
        }

        let index_documents_ids = self.index.documents_ids(self.wtxn)?;
        let index_is_empty = index_documents_ids.len() == 0;
        let mut final_documents_ids = RoaringBitmap::new();

        let mut databases_seen = 0;
        progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        for typed_chunk in lmdb_writer_rx {
            let (docids, is_merged_database) =
                write_typed_chunk_into_index(typed_chunk, &self.index, self.wtxn, index_is_empty)?;
            if !docids.is_empty() {
                final_documents_ids |= docids;
                let documents_seen_count = final_documents_ids.len();
                progress_callback(UpdateIndexingStep::IndexDocuments {
                    documents_seen: documents_seen_count as usize,
                    total_documents: documents_count,
                });
                debug!(
                    "We have seen {} documents on {} total document so far",
                    documents_seen_count, documents_count
                );
            }
            if is_merged_database {
                databases_seen += 1;
                progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
                    databases_seen: databases_seen,
                    total_databases: TOTAL_POSTING_DATABASE_COUNT,
                });
            }
        }

        // We write the field distribution into the main database
        self.index.put_field_distribution(self.wtxn, &field_distribution)?;

        // We write the primary key field id into the main database
        self.index.put_primary_key(self.wtxn, &primary_key)?;

        // We write the external documents ids into the main database.
        self.index.put_external_documents_ids(self.wtxn, &external_documents_ids)?;

        let all_documents_ids = index_documents_ids | new_documents_ids | replaced_documents_ids;
        self.index.put_documents_ids(self.wtxn, &all_documents_ids)?;

        self.execute_prefix_databases(progress_callback)
    }

    pub fn execute_prefix_databases<F>(self, progress_callback: F) -> Result<()>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        // Merged databases are already been indexed, we start from this count;
        let mut databases_seen = MERGED_DATABASE_COUNT;

        // Run the facets update operation.
        let mut builder = Facets::new(self.wtxn, self.index, self.update_id);
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        if let Some(value) = self.facet_level_group_size {
            builder.level_group_size(value);
        }
        if let Some(value) = self.facet_min_level_size {
            builder.min_level_size(value);
        }
        builder.execute()?;

        databases_seen += 1;
        progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen: databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        // Run the words prefixes update operation.
        let mut builder = WordsPrefixesFst::new(self.wtxn, self.index, self.update_id);
        if let Some(value) = self.words_prefix_threshold {
            builder.threshold(value);
        }
        if let Some(value) = self.max_prefix_length {
            builder.max_prefix_length(value);
        }
        builder.execute()?;

        databases_seen += 1;
        progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen: databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        // Run the word prefix docids update operation.
        let mut builder = WordPrefixDocids::new(self.wtxn, self.index);
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.max_nb_chunks = self.max_nb_chunks;
        builder.max_memory = self.max_memory;
        builder.execute()?;

        databases_seen += 1;
        progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen: databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        // Run the word prefix pair proximity docids update operation.
        let mut builder = WordPrefixPairProximityDocids::new(self.wtxn, self.index);
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.max_nb_chunks = self.max_nb_chunks;
        builder.max_memory = self.max_memory;
        builder.execute()?;

        databases_seen += 1;
        progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen: databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        // Run the words level positions update operation.
        let mut builder = WordsLevelPositions::new(self.wtxn, self.index);
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        if let Some(value) = self.words_positions_level_group_size {
            builder.level_group_size(value);
        }
        if let Some(value) = self.words_positions_min_level_size {
            builder.min_level_size(value);
        }
        builder.execute()?;

        databases_seen += 1;
        progress_callback(UpdateIndexingStep::MergeDataIntoFinalDatabase {
            databases_seen: databases_seen,
            total_databases: TOTAL_POSTING_DATABASE_COUNT,
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use big_s::S;
    use heed::EnvOpenOptions;

    use super::*;
    use crate::update::DeleteDocuments;
    use crate::HashMap;

    #[test]
    fn simple_document_replacement() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name\n1,kevin\n2,kevina\n3,benoit\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);

        // Second we send 1 document with id 1, to erase the previous ones.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name\n1,updated kevin\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is **always** 3 documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);

        // Third we send 3 documents again to replace the existing ones.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name\n1,updated second kevin\n2,updated kevina\n3,updated benoit\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 2);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is **always** 3 documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);
    }

    #[test]
    fn simple_document_merge() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with duplicate ids and
        // change the index method to merge documents.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name\n1,kevin\n1,kevina\n1,benoit\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        builder.index_documents_method(IndexDocumentsMethod::UpdateDocuments);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is only 1 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 1);

        // Check that we get only one document from the database.
        let docs = index.documents(&rtxn, Some(0)).unwrap();
        assert_eq!(docs.len(), 1);
        let (id, doc) = docs[0];
        assert_eq!(id, 0);

        // Check that this document is equal to the last one sent.
        let mut doc_iter = doc.iter();
        assert_eq!(doc_iter.next(), Some((0, &br#""1""#[..])));
        assert_eq!(doc_iter.next(), Some((1, &br#""benoit""#[..])));
        assert_eq!(doc_iter.next(), None);
        drop(rtxn);

        // Second we send 1 document with id 1, to force it to be merged with the previous one.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,age\n1,25\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Csv);
        builder.index_documents_method(IndexDocumentsMethod::UpdateDocuments);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is **always** 1 document.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 1);

        // Check that we get only one document from the database.
        let docs = index.documents(&rtxn, Some(0)).unwrap();
        assert_eq!(docs.len(), 1);
        let (id, doc) = docs[0];
        assert_eq!(id, 0);

        // Check that this document is equal to the last one sent.
        let mut doc_iter = doc.iter();
        assert_eq!(doc_iter.next(), Some((0, &br#""1""#[..])));
        assert_eq!(doc_iter.next(), Some((1, &br#""benoit""#[..])));
        assert_eq!(doc_iter.next(), Some((2, &br#""25""#[..])));
        assert_eq!(doc_iter.next(), None);
        drop(rtxn);
    }

    #[test]
    fn not_auto_generated_csv_documents_ids() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name\nkevin\nkevina\nbenoit\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        assert!(builder.execute(content, |_, _| ()).is_err());
        wtxn.commit().unwrap();

        // Check that there is no document.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 0);
        drop(rtxn);
    }

    #[test]
    fn not_auto_generated_json_documents_ids() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents and 2 without ids.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "name": "kevina", "id": 21 },
            { "name": "kevin" },
            { "name": "benoit" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        assert!(builder.execute(content, |_, _| ()).is_err());
        wtxn.commit().unwrap();

        // Check that there is no document.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 0);
        drop(rtxn);
    }

    #[test]
    fn simple_auto_generated_documents_ids() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name\nkevin\nkevina\nbenoit\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);

        let docs = index.documents(&rtxn, vec![0, 1, 2]).unwrap();
        let (_id, obkv) = docs.iter().find(|(_id, kv)| kv.get(0) == Some(br#""kevin""#)).unwrap();
        let kevin_uuid: String = serde_json::from_slice(&obkv.get(1).unwrap()).unwrap();
        drop(rtxn);

        // Second we send 1 document with the generated uuid, to erase the previous ones.
        let mut wtxn = index.write_txn().unwrap();
        let content = format!("id,name\n{},updated kevin", kevin_uuid);
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content.as_bytes(), |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is **always** 3 documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);

        let docs = index.documents(&rtxn, vec![0, 1, 2]).unwrap();
        let (kevin_id, _) =
            docs.iter().find(|(_, d)| d.get(0).unwrap() == br#""updated kevin""#).unwrap();
        let (id, doc) = docs[*kevin_id as usize];
        assert_eq!(id, *kevin_id);

        // Check that this document is equal to the last
        // one sent and that an UUID has been generated.
        assert_eq!(doc.get(0), Some(&br#""updated kevin""#[..]));
        // This is an UUID, it must be 36 bytes long plus the 2 surrounding string quotes (").
        assert_eq!(doc.get(1).unwrap().len(), 36 + 2);
        drop(rtxn);
    }

    #[test]
    fn reordered_auto_generated_documents_ids() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name\n1,kevin\n2,kevina\n3,benoit\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);

        // Second we send 1 document without specifying the id.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name\nnew kevin"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 4 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 4);
        drop(rtxn);
    }

    #[test]
    fn empty_csv_update() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 0 documents and only headers.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"id,name\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is no documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 0);
        drop(rtxn);
    }

    #[test]
    fn json_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with an id for only one of them.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "name": "kevin" },
            { "name": "kevina", "id": 21 },
            { "name": "benoit" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);
    }

    #[test]
    fn empty_json_update() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 0 documents.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"[]"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is no documents.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 0);
        drop(rtxn);
    }

    #[test]
    fn json_stream_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with an id for only one of them.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"
        { "name": "kevin" }
        { "name": "kevina", "id": 21 }
        { "name": "benoit" }
        "#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.enable_autogenerate_docids();
        builder.update_format(UpdateFormat::JsonStream);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 3);
        drop(rtxn);
    }

    #[test]
    fn invalid_documents_ids() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 1 document with an invalid id.
        let mut wtxn = index.write_txn().unwrap();
        // There is a space in the document id.
        let content = &b"id,name\nbrume bleue,kevin\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        assert!(builder.execute(content, |_, _| ()).is_err());
        wtxn.commit().unwrap();

        // First we send 1 document with a valid id.
        let mut wtxn = index.write_txn().unwrap();
        // There is a space in the document id.
        let content = &b"id,name\n32,kevin\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 1 document now.
        let rtxn = index.read_txn().unwrap();
        let count = index.number_of_documents(&rtxn).unwrap();
        assert_eq!(count, 1);
        drop(rtxn);
    }

    #[test]
    fn complex_json_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with an id for only one of them.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "id": 0, "name": "kevin", "object": { "key1": "value1", "key2": "value2" } },
            { "id": 1, "name": "kevina", "array": ["I", "am", "fine"] },
            { "id": 2, "name": "benoit", "array_of_object": [{ "wow": "amazing" }] }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 1 documents now.
        let rtxn = index.read_txn().unwrap();

        // Search for a sub object value
        let result = index.search(&rtxn).query(r#""value2""#).execute().unwrap();
        assert_eq!(result.documents_ids, vec![0]);

        // Search for a sub array value
        let result = index.search(&rtxn).query(r#""fine""#).execute().unwrap();
        assert_eq!(result.documents_ids, vec![1]);

        // Search for a sub array sub object key
        let result = index.search(&rtxn).query(r#""wow""#).execute().unwrap();
        assert_eq!(result.documents_ids, vec![2]);

        drop(rtxn);
    }

    #[test]
    fn simple_documents_replace() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with an id for only one of them.
        let mut wtxn = index.write_txn().unwrap();
        let documents = &r#"[
          { "id": 2,    "title": "Pride and Prejudice",                    "author": "Jane Austin",              "genre": "romance",    "price": 3.5 },
          { "id": 456,  "title": "Le Petit Prince",                        "author": "Antoine de Saint-Exupéry", "genre": "adventure" , "price": 10.0 },
          { "id": 1,    "title": "Alice In Wonderland",                    "author": "Lewis Carroll",            "genre": "fantasy",    "price": 25.99 },
          { "id": 1344, "title": "The Hobbit",                             "author": "J. R. R. Tolkien",         "genre": "fantasy" },
          { "id": 4,    "title": "Harry Potter and the Half-Blood Prince", "author": "J. K. Rowling",            "genre": "fantasy" },
          { "id": 42,   "title": "The Hitchhiker's Guide to the Galaxy",   "author": "Douglas Adams" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);
        builder.execute(Cursor::new(documents), |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 1);
        builder.update_format(UpdateFormat::Json);
        builder.index_documents_method(IndexDocumentsMethod::UpdateDocuments);
        let documents = &r#"[
          {
            "id": 2,
            "author": "J. Austen",
            "date": "1813"
          }
        ]"#[..];

        builder.execute(Cursor::new(documents), |_, _| ()).unwrap();
        wtxn.commit().unwrap();
    }

    #[test]
    fn delete_documents_then_insert() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "objectId": 123, "title": "Pride and Prejudice", "comment": "A great book" },
            { "objectId": 456, "title": "Le Petit Prince",     "comment": "A french book" },
            { "objectId": 1,   "title": "Alice In Wonderland", "comment": "A weird book" },
            { "objectId": 30,  "title": "Hamlet" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();

        assert_eq!(index.primary_key(&wtxn).unwrap(), Some("objectId"));

        // Delete not all of the documents but some of them.
        let mut builder = DeleteDocuments::new(&mut wtxn, &index, 1).unwrap();
        builder.delete_external_id("30");
        builder.execute().unwrap();

        let external_documents_ids = index.external_documents_ids(&wtxn).unwrap();
        assert!(external_documents_ids.get("30").is_none());

        let content = &br#"[
            { "objectId": 30, "title": "Hamlet" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();

        let external_documents_ids = index.external_documents_ids(&wtxn).unwrap();
        assert!(external_documents_ids.get("30").is_some());

        let content = &br#"[
            { "objectId": 30, "title": "Hamlet" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();

        wtxn.commit().unwrap();
    }

    #[test]
    fn index_more_than_256_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();

        let mut big_object = HashMap::new();
        big_object.insert(S("id"), "wow");
        for i in 0..1000 {
            let key = i.to_string();
            big_object.insert(key, "I am a text!");
        }

        let content = vec![big_object];
        let content = serde_json::to_string(&content).unwrap();

        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(Cursor::new(content), |_, _| ()).unwrap();

        wtxn.commit().unwrap();
    }

    #[test]
    fn index_documents_with_zeroes() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = r#"#id,title,au{hor,genre,price$
2,"Prideand Prejudice","Jane Austin","romance",3.5$
456,"Le Petit Prince","Antoine de Saint-Exupéry","adventure",10.0$
1,Wonderland","Lewis Carroll","fantasy",25.99$
4,"Harry Potter ing","fantasy\0lood Prince","J. K. Rowling","fantasy\0,
"#;

        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content.as_bytes(), |_, _| ()).unwrap();

        wtxn.commit().unwrap();
    }
}
