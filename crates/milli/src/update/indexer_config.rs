use grenad::CompressionType;

use super::GrenadParameters;
use crate::{thread_pool_no_abort::ThreadPoolNoAbort, ThreadPoolNoAbortBuilder};

#[derive(Debug)]
pub struct IndexerConfig {
    pub log_every_n: Option<usize>,
    pub max_nb_chunks: Option<usize>,
    pub documents_chunk_size: Option<usize>,
    pub max_memory: Option<usize>,
    pub max_threads: Option<usize>,
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: Option<u32>,
    pub thread_pool: ThreadPoolNoAbort,
    pub max_positions_per_attributes: Option<u32>,
    pub skip_index_budget: bool,
}

impl IndexerConfig {
    pub fn grenad_parameters(&self) -> GrenadParameters {
        GrenadParameters {
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            max_memory: self.max_memory,
            max_nb_chunks: self.max_nb_chunks,
        }
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        #[allow(unused_mut)]
        let mut pool_builder = ThreadPoolNoAbortBuilder::new();

        #[cfg(test)]
        {
            pool_builder = pool_builder.num_threads(1);
        }

        let thread_pool = pool_builder
            .thread_name(|index| format!("indexing-thread:{index}"))
            .build()
            .expect("failed to build default rayon thread pool");

        Self {
            log_every_n: None,
            max_nb_chunks: None,
            documents_chunk_size: None,
            max_memory: None,
            max_threads: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_positions_per_attributes: None,
            skip_index_budget: false,
            thread_pool,
        }
    }
}
