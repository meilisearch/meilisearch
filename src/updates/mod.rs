mod settings;

pub use settings::{Settings, Facets};

use std::io;
use std::sync::Arc;
use std::ops::Deref;

use anyhow::Result;
use flate2::read::GzDecoder;
use grenad::CompressionType;
use byte_unit::Byte;
use milli::update::{UpdateBuilder, UpdateFormat, IndexDocumentsMethod, UpdateIndexingStep::*};
use milli::{UpdateStore, UpdateHandler as Handler, Index};
use rayon::ThreadPool;
use serde::{Serialize, Deserialize};
use tokio::sync::broadcast;
use structopt::StructOpt;

use crate::option::Opt;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition { method: IndexDocumentsMethod, format: UpdateFormat },
    ClearDocuments,
    Settings(Settings),
    Facets(Facets),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMetaProgress {
    DocumentsAddition {
        step: usize,
        total_steps: usize,
        current: usize,
        total: Option<usize>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum UpdateStatus<M, P, N> {
    Pending { update_id: u64, meta: M },
    Progressing { update_id: u64, meta: P },
    Processed { update_id: u64, meta: N },
    Aborted { update_id: u64, meta: M },
}

#[derive(Clone)]
pub struct UpdateQueue {
    inner: Arc<UpdateStore<UpdateMeta, String>>,
}

impl Deref for UpdateQueue {
    type Target = Arc<UpdateStore<UpdateMeta, String>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, StructOpt)]
pub struct IndexerOpts {
    /// The amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[structopt(long, default_value = "100000")] // 100k
    pub log_every_n: usize,

    /// MTBL max number of chunks in bytes.
    #[structopt(long)]
    pub max_nb_chunks: Option<usize>,

    /// The maximum amount of memory to use for the MTBL buffer. It is recommended
    /// to use something like 80%-90% of the available memory.
    ///
    /// It is automatically split by the number of jobs e.g. if you use 7 jobs
    /// and 7 GB of max memory, each thread will use a maximum of 1 GB.
    #[structopt(long, default_value = "7 GiB")]
    pub max_memory: Byte,

    /// Size of the linked hash map cache when indexing.
    /// The bigger it is, the faster the indexing is but the more memory it takes.
    #[structopt(long, default_value = "500")]
    pub linked_hash_map_size: usize,

    /// The name of the compression algorithm to use when compressing intermediate
    /// chunks during indexing documents.
    ///
    /// Choosing a fast algorithm will make the indexing faster but may consume more memory.
    #[structopt(long, default_value = "snappy", possible_values = &["snappy", "zlib", "lz4", "lz4hc", "zstd"])]
    pub chunk_compression_type: CompressionType,

    /// The level of compression of the chosen algorithm.
    #[structopt(long, requires = "chunk-compression-type")]
    pub chunk_compression_level: Option<u32>,

    /// The number of bytes to remove from the begining of the chunks while reading/sorting
    /// or merging them.
    ///
    /// File fusing must only be enable on file systems that support the `FALLOC_FL_COLLAPSE_RANGE`,
    /// (i.e. ext4 and XFS). File fusing will only work if the `enable-chunk-fusing` is set.
    #[structopt(long, default_value = "4 GiB")]
    pub chunk_fusing_shrink_size: Byte,

    /// Enable the chunk fusing or not, this reduces the amount of disk used by a factor of 2.
    #[structopt(long)]
    pub enable_chunk_fusing: bool,

    /// Number of parallel jobs for indexing, defaults to # of CPUs.
    #[structopt(long)]
    pub indexing_jobs: Option<usize>,
}

type UpdateSender = broadcast::Sender<UpdateStatus<UpdateMeta, UpdateMetaProgress, String>>;

struct UpdateHandler {
    indexes: Arc<Index>,
    max_nb_chunks: Option<usize>,
    chunk_compression_level: Option<u32>,
    thread_pool: ThreadPool,
    log_frequency: usize,
    max_memory: usize,
    linked_hash_map_size: usize,
    chunk_compression_type: CompressionType,
    chunk_fusing_shrink_size: u64,
    update_status_sender: UpdateSender,
}

impl UpdateHandler {
    fn new(
        opt: &IndexerOpts,
        indexes: Arc<Index>,
        update_status_sender: UpdateSender,
    ) -> Result<Self> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opt.indexing_jobs.unwrap_or(0))
            .build()?;
        Ok(Self {
            indexes,
            max_nb_chunks: opt.max_nb_chunks,
            chunk_compression_level: opt.chunk_compression_level,
            thread_pool,
            log_frequency: opt.log_every_n,
            max_memory: opt.max_memory.get_bytes() as usize,
            linked_hash_map_size: opt.linked_hash_map_size,
            chunk_compression_type: opt.chunk_compression_type,
            chunk_fusing_shrink_size: opt.chunk_fusing_shrink_size.get_bytes(),
            update_status_sender,
        })
    }

    fn update_buidler(&self, update_id: u64) -> UpdateBuilder {
        // We prepare the update by using the update builder.
        let mut update_builder = UpdateBuilder::new(update_id);
        if let Some(max_nb_chunks) = self.max_nb_chunks {
            update_builder.max_nb_chunks(max_nb_chunks);
        }
        if let Some(chunk_compression_level) = self.chunk_compression_level {
            update_builder.chunk_compression_level(chunk_compression_level);
        }
        update_builder.thread_pool(&self.thread_pool);
        update_builder.log_every_n(self.log_frequency);
        update_builder.max_memory(self.max_memory);
        update_builder.linked_hash_map_size(self.linked_hash_map_size);
        update_builder.chunk_compression_type(self.chunk_compression_type);
        update_builder.chunk_fusing_shrink_size(self.chunk_fusing_shrink_size);
        update_builder
    }

    fn update_documents(
        &self,
        format: UpdateFormat,
        method: IndexDocumentsMethod,
        content: &[u8],
        update_builder: UpdateBuilder,
    ) -> Result<()> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.indexes.write_txn()?;
        let mut builder = update_builder.index_documents(&mut wtxn, &self.indexes);
        builder.update_format(format);
        builder.index_documents_method(method);

        let gzipped = true;
        let reader = if gzipped {
            Box::new(GzDecoder::new(content))
        } else {
            Box::new(content) as Box<dyn io::Read>
        };

        let result = builder.execute(reader, |indexing_step, update_id| {
            let (current, total) = match indexing_step {
                TransformFromUserIntoGenericFormat { documents_seen } => (documents_seen, None),
                ComputeIdsAndMergeDocuments { documents_seen, total_documents } => (documents_seen, Some(total_documents)),
                IndexDocuments { documents_seen, total_documents } => (documents_seen, Some(total_documents)),
                MergeDataIntoFinalDatabase { databases_seen, total_databases } => (databases_seen, Some(total_databases)),
            };
            let _ = self.update_status_sender.send(UpdateStatus::Progressing {
                update_id,
                meta: UpdateMetaProgress::DocumentsAddition {
                    step: indexing_step.step(),
                    total_steps: indexing_step.number_of_steps(),
                    current,
                    total,
                }
            });
        });

        match result {
            Ok(()) => wtxn.commit().map_err(Into::into),
            Err(e) => Err(e.into())
        }
    }

    fn clear_documents(&self, update_builder: UpdateBuilder) -> Result<()> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.indexes.write_txn()?;
        let builder = update_builder.clear_documents(&mut wtxn, &self.indexes);

        match builder.execute() {
            Ok(_count) => wtxn.commit().map_err(Into::into),
            Err(e) => Err(e.into())
        }
    }

    fn update_settings(&self, settings: Settings, update_builder: UpdateBuilder) -> Result<()> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.indexes.write_txn()?;
        let mut builder = update_builder.settings(&mut wtxn, &self.indexes);

        // We transpose the settings JSON struct into a real setting update.
        if let Some(names) = settings.searchable_attributes {
            match names {
                Some(names) => builder.set_searchable_fields(names),
                None => builder.reset_searchable_fields(),
            }
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(names) = settings.displayed_attributes {
            match names {
                Some(names) => builder.set_displayed_fields(names),
                None => builder.reset_displayed_fields(),
            }
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(facet_types) = settings.faceted_attributes {
            builder.set_faceted_fields(facet_types);
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(criteria) = settings.criteria {
            match criteria {
                Some(criteria) => builder.set_criteria(criteria),
                None => builder.reset_criteria(),
            }
        }

        let result = builder.execute(|indexing_step, update_id| {
            let (current, total) = match indexing_step {
                TransformFromUserIntoGenericFormat { documents_seen } => (documents_seen, None),
                ComputeIdsAndMergeDocuments { documents_seen, total_documents } => (documents_seen, Some(total_documents)),
                IndexDocuments { documents_seen, total_documents } => (documents_seen, Some(total_documents)),
                MergeDataIntoFinalDatabase { databases_seen, total_databases } => (databases_seen, Some(total_databases)),
            };
            let _ = self.update_status_sender.send(UpdateStatus::Progressing {
                update_id,
                meta: UpdateMetaProgress::DocumentsAddition {
                    step: indexing_step.step(),
                    total_steps: indexing_step.number_of_steps(),
                    current,
                    total,
                }
            });
        });

        match result {
            Ok(_count) => wtxn.commit().map_err(Into::into),
            Err(e) => Err(e.into())
        }
    }

    fn update_facets(&self, levels: Facets, update_builder: UpdateBuilder) -> Result<()> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.indexes.write_txn()?;
        let mut builder = update_builder.facets(&mut wtxn, &self.indexes);
        if let Some(value) = levels.level_group_size {
            builder.level_group_size(value);
        }
        if let Some(value) = levels.min_level_size {
            builder.min_level_size(value);
        }
        match builder.execute() {
            Ok(()) => wtxn.commit().map_err(Into::into),
            Err(e) => Err(e.into())
        }
    }
}

impl Handler<UpdateMeta, String> for UpdateHandler {
    fn handle_update(&mut self, update_id: u64, meta: UpdateMeta, content: &[u8]) -> heed::Result<String> {
        use UpdateMeta::*;

        let update_builder = self.update_buidler(update_id);

        let result: anyhow::Result<()> = match meta {
            DocumentsAddition { method, format } => {
                self.update_documents(format, method, content, update_builder)
            },
            ClearDocuments => self.clear_documents(update_builder),
            Settings(settings) => self.update_settings(settings, update_builder),
            Facets(levels) => self.update_facets(levels, update_builder),
        };

        let meta = match result {
            Ok(()) => format!("valid update content"),
            Err(e) => format!("error while processing update content: {:?}", e),
        };

        let processed = UpdateStatus::Processed { update_id, meta: meta.clone() };
        let _ = self.update_status_sender.send(processed);

        Ok(meta)
    }
}

impl UpdateQueue {
    pub fn new(
        opt: &Opt,
        indexes: Arc<Index>,
        ) -> Result<Self> {
        let (sender, _) = broadcast::channel(100);
        let handler = UpdateHandler::new(&opt.indexer_options, indexes, sender)?;
        let size = opt.max_udb_size.get_bytes() as usize;
        let path = opt.db_path.join("updates.mdb");
        let inner = UpdateStore::open(
            Some(size),
            path,
            handler
        )?;
        Ok(Self { inner })
    }
}
