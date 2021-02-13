use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use anyhow::Result;
use flate2::read::GzDecoder;
use grenad::CompressionType;
use log::info;
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use milli::Index;
use rayon::ThreadPool;

use super::update_store::HandleUpdate;
use crate::index_controller::updates::{Failed, Processed, Processing};
use crate::index_controller::{Facets, Settings, UpdateMeta, UpdateResult};
use crate::option::IndexerOpts;

pub struct UpdateHandler {
    index: Arc<Index>,
    max_nb_chunks: Option<usize>,
    chunk_compression_level: Option<u32>,
    thread_pool: Arc<ThreadPool>,
    log_frequency: usize,
    max_memory: usize,
    linked_hash_map_size: usize,
    chunk_compression_type: CompressionType,
    chunk_fusing_shrink_size: u64,
}

impl UpdateHandler {
    pub fn new(
        opt: &IndexerOpts,
        index: Arc<Index>,
        thread_pool: Arc<ThreadPool>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            index,
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
        primary_key: Option<&str>,
    ) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.index.write_txn()?;

        // Set the primary key if not set already, ignore if already set.
        match (self.index.primary_key(&wtxn)?, primary_key) {
            (None, Some(ref primary_key)) => {
                self.index.put_primary_key(&mut wtxn, primary_key)?;
            }
            _ => (),
        }

        let mut builder = update_builder.index_documents(&mut wtxn, &self.index);
        builder.update_format(format);
        builder.index_documents_method(method);

        let gzipped = true;
        let reader = if gzipped {
            Box::new(GzDecoder::new(content))
        } else {
            Box::new(content) as Box<dyn io::Read>
        };

        let result = builder.execute(reader, |indexing_step, update_id| {
            info!("update {}: {:?}", update_id, indexing_step)
        });

        match result {
            Ok(addition_result) => wtxn
                .commit()
                .and(Ok(UpdateResult::DocumentsAddition(addition_result)))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
    }

    fn clear_documents(&self, update_builder: UpdateBuilder) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.index.write_txn()?;
        let builder = update_builder.clear_documents(&mut wtxn, &self.index);

        match builder.execute() {
            Ok(_count) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
    }

    fn update_settings(
        &self,
        settings: &Settings,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.index.write_txn()?;
        let mut builder = update_builder.settings(&mut wtxn, &self.index);

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref names) = settings.searchable_attributes {
            match names {
                Some(names) => builder.set_searchable_fields(names.clone()),
                None => builder.reset_searchable_fields(),
            }
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref names) = settings.displayed_attributes {
            match names {
                Some(names) => builder.set_displayed_fields(names.clone()),
                None => builder.reset_displayed_fields(),
            }
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref facet_types) = settings.faceted_attributes {
            let facet_types = facet_types.clone().unwrap_or_else(|| HashMap::new());
            builder.set_faceted_fields(facet_types);
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref criteria) = settings.criteria {
            match criteria {
                Some(criteria) => builder.set_criteria(criteria.clone()),
                None => builder.reset_criteria(),
            }
        }

        let result = builder
            .execute(|indexing_step, update_id| info!("update {}: {:?}", update_id, indexing_step));

        match result {
            Ok(()) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
    }

    fn update_facets(
        &self,
        levels: &Facets,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.index.write_txn()?;
        let mut builder = update_builder.facets(&mut wtxn, &self.index);
        if let Some(value) = levels.level_group_size {
            builder.level_group_size(value);
        }
        if let Some(value) = levels.min_level_size {
            builder.min_level_size(value);
        }
        match builder.execute() {
            Ok(()) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
    }

    fn delete_documents(
        &self,
        document_ids: &[u8],
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        let ids: Vec<String> = serde_json::from_slice(document_ids)?;
        let mut txn = self.index.write_txn()?;
        let mut builder = update_builder.delete_documents(&mut txn, &self.index)?;

        // We ignore unexisting document ids
        ids.iter().for_each(|id| { builder.delete_external_id(id); });

        match builder.execute() {
            Ok(deleted) => txn
                .commit()
                .and(Ok(UpdateResult::DocumentDeletion { deleted }))
                .map_err(Into::into),
            Err(e) => Err(e.into())
        }
    }
}

impl HandleUpdate<UpdateMeta, UpdateResult, String> for UpdateHandler {
    fn handle_update(
        &mut self,
        meta: Processing<UpdateMeta>,
        content: &[u8],
    ) -> Result<Processed<UpdateMeta, UpdateResult>, Failed<UpdateMeta, String>> {
        use UpdateMeta::*;

        let update_id = meta.id();

        let update_builder = self.update_buidler(update_id);

        let result = match meta.meta() {
            DocumentsAddition {
                method,
                format,
                primary_key,
            } => self.update_documents(
                *format,
                *method,
                content,
                update_builder,
                primary_key.as_deref(),
            ),
            ClearDocuments => self.clear_documents(update_builder),
            DeleteDocuments => self.delete_documents(content, update_builder),
            Settings(settings) => self.update_settings(settings, update_builder),
            Facets(levels) => self.update_facets(levels, update_builder),
        };

        match result {
            Ok(result) => Ok(meta.process(result)),
            Err(e) => Err(meta.fail(e.to_string())),
        }
    }
}
