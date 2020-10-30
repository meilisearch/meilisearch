use grenad::CompressionType;

use crate::Index;
use super::clear_documents::ClearDocuments;
use super::delete_documents::DeleteDocuments;
use super::index_documents::IndexDocuments;

pub struct UpdateBuilder {
    pub(crate) log_every_n: Option<usize>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) linked_hash_map_size: Option<usize>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    pub(crate) indexing_jobs: Option<usize>,
}

impl UpdateBuilder {
    pub fn new() -> UpdateBuilder {
        UpdateBuilder {
            log_every_n: None,
            max_nb_chunks: None,
            max_memory: None,
            linked_hash_map_size: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            indexing_jobs: None,
        }
    }

    pub fn log_every_n(&mut self, log_every_n: usize) -> &mut Self {
        self.log_every_n = Some(log_every_n);
        self
    }

    pub fn max_nb_chunks(&mut self, max_nb_chunks: usize) -> &mut Self {
        self.max_nb_chunks = Some(max_nb_chunks);
        self
    }

    pub fn max_memory(&mut self, max_memory: usize) -> &mut Self {
        self.max_memory = Some(max_memory);
        self
    }

    pub fn linked_hash_map_size(&mut self, linked_hash_map_size: usize) -> &mut Self {
        self.linked_hash_map_size = Some(linked_hash_map_size);
        self
    }

    pub fn chunk_compression_type(&mut self, chunk_compression_type: CompressionType) -> &mut Self {
        self.chunk_compression_type = chunk_compression_type;
        self
    }

    pub fn chunk_compression_level(&mut self, chunk_compression_level: u32) -> &mut Self {
        self.chunk_compression_level = Some(chunk_compression_level);
        self
    }

    pub fn chunk_fusing_shrink_size(&mut self, chunk_fusing_shrink_size: u64) -> &mut Self {
        self.chunk_fusing_shrink_size = Some(chunk_fusing_shrink_size);
        self
    }

    pub fn indexing_jobs(&mut self, indexing_jobs: usize) -> &mut Self {
        self.indexing_jobs = Some(indexing_jobs);
        self
    }

    pub fn clear_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> ClearDocuments<'t, 'u, 'i>
    {
        ClearDocuments::new(wtxn, index)
    }

    pub fn delete_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>>
    {
        DeleteDocuments::new(wtxn, index)
    }

    pub fn index_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> IndexDocuments<'t, 'u, 'i>
    {
        let mut builder = IndexDocuments::new(wtxn, index);

        if let Some(log_every_n) = self.log_every_n {
            builder.log_every_n(log_every_n);
        }
        if let Some(max_nb_chunks) = self.max_nb_chunks {
            builder.max_nb_chunks(max_nb_chunks);
        }
        if let Some(max_memory) = self.max_memory {
            builder.max_memory(max_memory);
        }
        if let Some(linked_hash_map_size) = self.linked_hash_map_size {
            builder.linked_hash_map_size(linked_hash_map_size);
        }

        builder.chunk_compression_type(self.chunk_compression_type);

        if let Some(chunk_compression_level) = self.chunk_compression_level {
            builder.chunk_compression_level(chunk_compression_level);
        }
        if let Some(chunk_fusing_shrink_size) = self.chunk_fusing_shrink_size {
            builder.chunk_fusing_shrink_size(chunk_fusing_shrink_size);
        }
        if let Some(indexing_jobs) = self.indexing_jobs {
            builder.indexing_jobs(indexing_jobs);
        }

        builder
    }
}
