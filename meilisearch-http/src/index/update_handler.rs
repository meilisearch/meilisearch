use std::fs::File;

use crate::index::Index;
use milli::CompressionType;
use milli::update::UpdateBuilder;
use rayon::ThreadPool;

use crate::index_controller::UpdateMeta;
use crate::index_controller::{Failed, Processed, Processing};
use crate::option::IndexerOpts;

pub struct UpdateHandler {
    max_nb_chunks: Option<usize>,
    chunk_compression_level: Option<u32>,
    thread_pool: ThreadPool,
    log_frequency: usize,
    max_memory: usize,
    linked_hash_map_size: usize,
    chunk_compression_type: CompressionType,
    chunk_fusing_shrink_size: u64,
}

impl UpdateHandler {
    pub fn new(opt: &IndexerOpts) -> anyhow::Result<Self> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opt.indexing_jobs.unwrap_or(num_cpus::get() / 2))
            .build()?;
        Ok(Self {
            max_nb_chunks: opt.max_nb_chunks,
            chunk_compression_level: opt.chunk_compression_level,
            thread_pool,
            log_frequency: opt.log_every_n,
            max_memory: opt.max_memory.get_bytes() as usize,
            linked_hash_map_size: opt.linked_hash_map_size,
            chunk_compression_type: opt.chunk_compression_type,
            chunk_fusing_shrink_size: opt.chunk_fusing_shrink_size.get_bytes(),
        })
    }

    pub fn update_builder(&self, update_id: u64) -> UpdateBuilder {
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

    pub fn handle_update(
        &self,
        meta: Processing,
        content: Option<File>,
        index: Index,
    ) -> Result<Processed, Failed> {
        use UpdateMeta::*;

        let update_id = meta.id();

        let update_builder = self.update_builder(update_id);

        let result = match meta.meta() {
            DocumentsAddition {
                method,
                format,
                primary_key,
            } => index.update_documents(
                *format,
                *method,
                content,
                update_builder,
                primary_key.as_deref(),
            ),
            ClearDocuments => index.clear_documents(update_builder),
            DeleteDocuments { ids } => index.delete_documents(ids, update_builder),
            Settings(settings) => index.update_settings(&settings.clone().check(), update_builder),
        };

        match result {
            Ok(result) => Ok(meta.process(result)),
            Err(e) => Err(meta.fail(e.into())),
        }
    }
}
