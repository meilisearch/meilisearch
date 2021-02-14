use grenad::CompressionType;
use rayon::ThreadPool;

use crate::Index;
use super::{ClearDocuments, DeleteDocuments, IndexDocuments, Settings, Facets};

pub struct UpdateBuilder<'a> {
    pub(crate) log_every_n: Option<usize>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) linked_hash_map_size: Option<usize>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    pub(crate) thread_pool: Option<&'a ThreadPool>,
    pub(crate) update_id: u64,
}

impl<'a> UpdateBuilder<'a> {
    pub fn new(update_id: u64) -> UpdateBuilder<'a> {
        UpdateBuilder {
            log_every_n: None,
            max_nb_chunks: None,
            max_memory: None,
            linked_hash_map_size: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            thread_pool: None,
            update_id,
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

    pub fn linked_hash_map_size(&mut self, linked_hash_map_size: usize) {
        self.linked_hash_map_size = Some(linked_hash_map_size);
    }

    pub fn chunk_compression_type(&mut self, chunk_compression_type: CompressionType) {
        self.chunk_compression_type = chunk_compression_type;
    }

    pub fn chunk_compression_level(&mut self, chunk_compression_level: u32) {
        self.chunk_compression_level = Some(chunk_compression_level);
    }

    pub fn chunk_fusing_shrink_size(&mut self, chunk_fusing_shrink_size: u64) {
        self.chunk_fusing_shrink_size = Some(chunk_fusing_shrink_size);
    }

    pub fn thread_pool(&mut self, thread_pool: &'a ThreadPool) {
        self.thread_pool = Some(thread_pool);
    }

    pub fn clear_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> ClearDocuments<'t, 'u, 'i>
    {
        ClearDocuments::new(wtxn, index, self.update_id)
    }

    pub fn delete_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>>
    {
        DeleteDocuments::new(wtxn, index, self.update_id)
    }

    pub fn index_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> IndexDocuments<'t, 'u, 'i, 'a>
    {
        let mut builder = IndexDocuments::new(wtxn, index, self.update_id);

        builder.log_every_n = self.log_every_n;
        builder.max_nb_chunks = self.max_nb_chunks;
        builder.max_memory = self.max_memory;
        builder.linked_hash_map_size = self.linked_hash_map_size;
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.chunk_fusing_shrink_size = self.chunk_fusing_shrink_size;
        builder.thread_pool = self.thread_pool;

        builder
    }

    pub fn settings<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> Settings<'a, 't, 'u, 'i>
    {
        let mut builder = Settings::new(wtxn, index, self.update_id);

        builder.log_every_n = self.log_every_n;
        builder.max_nb_chunks = self.max_nb_chunks;
        builder.max_memory = self.max_memory;
        builder.linked_hash_map_size = self.linked_hash_map_size;
        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.chunk_fusing_shrink_size = self.chunk_fusing_shrink_size;
        builder.thread_pool = self.thread_pool;

        builder
    }

    pub fn facets<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> Facets<'t, 'u, 'i>
    {
        let mut builder = Facets::new(wtxn, index, self.update_id);

        builder.chunk_compression_type = self.chunk_compression_type;
        builder.chunk_compression_level = self.chunk_compression_level;
        builder.chunk_fusing_shrink_size = self.chunk_fusing_shrink_size;

        builder
    }
}
