use grenad::CompressionType;

use super::GrenadParameters;
use crate::thread_pool_no_abort::ThreadPoolNoAbort;
use crate::ThreadPoolNoAbortBuilder;

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
    pub experimental_no_edition_2024_for_settings: bool,
    pub experimental_no_edition_2024_for_dumps: bool,
    pub experimental_no_edition_2024_for_prefix_post_processing: bool,
    pub experimental_no_edition_2024_for_facet_post_processing: bool,
}

impl IndexerConfig {
    pub fn grenad_parameters(&self) -> GrenadParameters {
        GrenadParameters {
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            max_memory: self.max_memory,
            max_nb_chunks: self.max_nb_chunks,
            experimental_no_edition_2024_for_prefix_post_processing: self
                .experimental_no_edition_2024_for_prefix_post_processing,
            experimental_no_edition_2024_for_facet_post_processing: self
                .experimental_no_edition_2024_for_facet_post_processing,
        }
    }
}

/// By default use only 1 thread for indexing in tests
#[cfg(test)]
pub fn default_thread_pool_and_threads() -> (ThreadPoolNoAbort, Option<usize>) {
    let pool = ThreadPoolNoAbortBuilder::new_for_indexing()
        .num_threads(1)
        .build()
        .expect("failed to build default rayon thread pool");

    (pool, Some(1))
}

#[cfg(not(test))]
pub fn default_thread_pool_and_threads() -> (ThreadPoolNoAbort, Option<usize>) {
    let pool = ThreadPoolNoAbortBuilder::new_for_indexing()
        .build()
        .expect("failed to build default rayon thread pool");

    (pool, None)
}

impl Default for IndexerConfig {
    fn default() -> Self {
        let (thread_pool, max_threads) = default_thread_pool_and_threads();

        Self {
            max_threads,
            thread_pool,
            log_every_n: None,
            max_nb_chunks: None,
            documents_chunk_size: None,
            max_memory: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_positions_per_attributes: None,
            skip_index_budget: false,
            experimental_no_edition_2024_for_settings: false,
            experimental_no_edition_2024_for_dumps: false,
            experimental_no_edition_2024_for_prefix_post_processing: false,
            experimental_no_edition_2024_for_facet_post_processing: false,
        }
    }
}
