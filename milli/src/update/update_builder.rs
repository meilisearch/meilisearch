use grenad::CompressionType;
use rayon::ThreadPool;

use super::{ClearDocuments, DeleteDocuments, Facets, IndexDocuments, Settings};
use crate::{Index, Result};

pub struct UpdateBuilder<'a> {
    pub(crate) log_every_n: Option<usize>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) documents_chunk_size: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) thread_pool: Option<&'a ThreadPool>,
    pub(crate) max_positions_per_attributes: Option<u32>,
}

impl<'a> UpdateBuilder<'a> {
    pub fn new() -> UpdateBuilder<'a> {
        UpdateBuilder {
            log_every_n: None,
            max_nb_chunks: None,
            documents_chunk_size: None,
            max_memory: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            thread_pool: None,
            max_positions_per_attributes: None,
        }
    }

    pub fn log_every_n(&mut self, log_every_n: usize) {
        self.log_every_n = Some(log_every_n);
    }

    pub fn max_nb_chunks(&mut self, max_nb_chunks: usize) {
        self.max_nb_chunks = Some(max_nb_chunks);
    }

    pub fn max_memory(&mut self, max_memory: usize) {
        self.max_memory = Some(max_memory);
    }

    pub fn documents_chunk_size(&mut self, documents_chunk_size: usize) {
        self.documents_chunk_size = Some(documents_chunk_size);
    }

    pub fn chunk_compression_type(&mut self, chunk_compression_type: CompressionType) {
        self.chunk_compression_type = chunk_compression_type;
    }

    pub fn chunk_compression_level(&mut self, chunk_compression_level: u32) {
        self.chunk_compression_level = Some(chunk_compression_level);
    }

    pub fn thread_pool(&mut self, thread_pool: &'a ThreadPool) {
        self.thread_pool = Some(thread_pool);
    }

    pub fn max_positions_per_attributes(&mut self, max_positions_per_attributes: u32) {
        self.max_positions_per_attributes = Some(max_positions_per_attributes);
    }

    pub fn clear_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> ClearDocuments<'t, 'u, 'i> {
        ClearDocuments::new(wtxn, index)
    }

    pub fn delete_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> Result<DeleteDocuments<'t, 'u, 'i>> {
        DeleteDocuments::new(wtxn, index)
    }

    pub fn index_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> IndexDocuments<'t, 'u, 'i, 'a> {
        let mut builder = IndexDocuments::new(wtxn, index);

        builder.log_every_n = self.log_every_n;
        builder.max_nb_chunks = self.max_nb_chunks;
        builder.max_memory = self.max_memory;
        builder.documents_chunk_size = self.documents_chunk_size;
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.thread_pool = self.thread_pool;
        builder.max_positions_per_attributes = self.max_positions_per_attributes;

        builder
    }

    pub fn settings<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> Settings<'a, 't, 'u, 'i> {
        let mut builder = Settings::new(wtxn, index);

        builder.log_every_n = self.log_every_n;
        builder.max_nb_chunks = self.max_nb_chunks;
        builder.max_memory = self.max_memory;
        builder.documents_chunk_size = self.documents_chunk_size;
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.thread_pool = self.thread_pool;
        builder.max_positions_per_attributes = self.max_positions_per_attributes;

        builder
    }

    pub fn facets<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> Facets<'t, 'u, 'i> {
        let mut builder = Facets::new(wtxn, index);

        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;

        builder
    }
}
