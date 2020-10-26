use std::borrow::Cow;
use std::convert::TryFrom;

use fst::{IntoStreamer, Streamer};
use grenad::CompressionType;
use itertools::Itertools;
use roaring::RoaringBitmap;

use crate::{Index, BEU32};
use super::clear_documents::ClearDocuments;
use super::delete_documents::DeleteDocuments;

pub struct UpdateBuilder {
    log_every_n: usize,
    max_nb_chunks: Option<usize>,
    max_memory: usize,
    linked_hash_map_size: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    chunk_fusing_shrink_size: u64,
    enable_chunk_fusing: bool,
    indexing_jobs: Option<usize>,
}

impl UpdateBuilder {
    pub fn new() -> UpdateBuilder {
        todo!()
    }

    pub fn log_every_n(&mut self, log_every_n: usize) -> &mut Self {
        self.log_every_n = log_every_n;
        self
    }

    pub fn max_nb_chunks(&mut self, max_nb_chunks: usize) -> &mut Self {
        self.max_nb_chunks = Some(max_nb_chunks);
        self
    }

    pub fn max_memory(&mut self, max_memory: usize) -> &mut Self {
        self.max_memory = max_memory;
        self
    }

    pub fn linked_hash_map_size(&mut self, linked_hash_map_size: usize) -> &mut Self {
        self.linked_hash_map_size = linked_hash_map_size;
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
        self.chunk_fusing_shrink_size = chunk_fusing_shrink_size;
        self
    }

    pub fn enable_chunk_fusing(&mut self, enable_chunk_fusing: bool) -> &mut Self {
        self.enable_chunk_fusing = enable_chunk_fusing;
        self
    }

    pub fn indexing_jobs(&mut self, indexing_jobs: usize) -> &mut Self {
        self.indexing_jobs = Some(indexing_jobs);
        self
    }

    pub fn clear_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> ClearDocuments<'t, 'u, 'i>
    {
        ClearDocuments::new(wtxn, index)
    }

    pub fn delete_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> anyhow::Result<DeleteDocuments<'t, 'u, 'i>>
    {
        DeleteDocuments::new(wtxn, index)
    }

    pub fn index_documents<'t, 'u, 'i>(
        self,
        wtxn: &'t mut heed::RwTxn<'u>,
        index: &'i Index,
    ) -> IndexDocuments<'t, 'u, 'i>
    {
        IndexDocuments::new(wtxn, index)
    }
}

pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

pub struct IndexDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'u>,
    index: &'i Index,
    update_method: IndexDocumentsMethod,
}

impl<'t, 'u, 'i> IndexDocuments<'t, 'u, 'i> {
    fn new(wtxn: &'t mut heed::RwTxn<'u>, index: &'i Index) -> IndexDocuments<'t, 'u, 'i> {
        IndexDocuments { wtxn, index, update_method: IndexDocumentsMethod::ReplaceDocuments }
    }

    pub fn index_documents_method(&mut self, method: IndexDocumentsMethod) -> &mut Self {
        self.update_method = method;
        self
    }

    pub fn execute(self) -> anyhow::Result<()> {
        todo!()
    }
}
