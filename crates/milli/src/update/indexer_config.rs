use grenad::CompressionType;

use super::GrenadParameters;
use crate::thread_pool_no_abort::ThreadPoolNoAbort;

#[derive(Debug)]
pub struct IndexerConfig {
    pub log_every_n: Option<usize>,
    pub max_nb_chunks: Option<usize>,
    pub documents_chunk_size: Option<usize>,
    pub max_memory: Option<usize>,
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: Option<u32>,
    pub thread_pool: Option<ThreadPoolNoAbort>,
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
        Self {
            log_every_n: None,
            max_nb_chunks: None,
            documents_chunk_size: None,
            max_memory: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            thread_pool: None,
            max_positions_per_attributes: None,
            skip_index_budget: false,
        }
    }
}
