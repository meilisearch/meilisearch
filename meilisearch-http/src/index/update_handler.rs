use std::fs::File;
use std::sync::{mpsc, Arc};

use crate::index::Index;
use milli::update::UpdateBuilder;
use milli::CompressionType;
use rayon::ThreadPool;

use crate::index_controller::{Aborted, Done, Failed, Processed, Processing};
use crate::index_controller::{UpdateMeta, UpdateResult};
use crate::option::IndexerOpts;

pub struct UpdateHandler {
    max_nb_chunks: Option<usize>,
    chunk_compression_level: Option<u32>,
    thread_pool: ThreadPool,
    log_frequency: usize,
    max_memory: Option<usize>,
    chunk_compression_type: CompressionType,
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
            max_memory: opt.max_memory.map(|m| m.get_bytes() as usize),
            chunk_compression_type: opt.chunk_compression_type,
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
        if let Some(max_memory) = self.max_memory {
            update_builder.max_memory(max_memory);
        }
        update_builder.chunk_compression_type(self.chunk_compression_type);
        update_builder
    }

    pub fn handle_update(
        &self,
        channel: mpsc::Sender<(mpsc::Sender<Hello>, Result<Processed, Failed>)>,
        meta: Processing,
        content: Option<File>,
        index: Index,
    ) -> Result<Done, Aborted> {
        use UpdateMeta::*;

        let update_id = meta.id();

        let update_builder = self.update_builder(update_id);
        let mut wtxn = index.write_txn().unwrap();

        let result = match meta.meta() {
            DocumentsAddition {
                method,
                format,
                primary_key,
            } => index.update_documents(
                &mut wtxn,
                *format,
                *method,
                content,
                update_builder,
                primary_key.as_deref(),
            ),
            ClearDocuments => index.clear_documents(&mut wtxn, update_builder),
            DeleteDocuments { ids } => index.delete_documents(&mut wtxn, ids, update_builder),
            Settings(settings) => {
                index.update_settings(&mut wtxn, &settings.clone().check(), update_builder)
            }
        };

        let result = match result {
            Ok(result) => Ok(meta.process(result)),
            Err(e) => Err(meta.fail(e.into())),
        };


        let (sender, receiver) = mpsc::channel();
        channel.send((sender, result));

        // here we should decide how we want to handle a failure. probably by closing the channel
        // right: for now I'm just going to panic

        let meta = result.unwrap();

        match receiver.recv() {
            Ok(Hello::Abort) => Err(meta.abort()),
            Ok(Hello::Commit) => wtxn
                .commit()
                .map(|ok| meta.commit())
                .map_err(|e| meta.abort()),
            Err(e) => panic!("update actor died {}", e),
        }
    }
}

/// MARIN: I can't find any good name for this and I'm not even sure we need a new enum
pub enum Hello {
    Commit,
    Abort,
}
