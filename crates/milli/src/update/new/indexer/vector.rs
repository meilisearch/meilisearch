use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hashbrown::HashMap;
use heed::{RoTxn, WithoutTls};
use rayon::iter::IntoParallelIterator as _;
use tracing::Span;

use crate::progress::Progress;
use crate::update::new::channel::{EmbeddingSender, ExtractorBbqueueSender};
use crate::update::new::parallel_iterator_ext::ParallelIteratorExt as _;
use crate::update::new::steps::IndexingStep;
use crate::vector::db::EmbeddingStatus;
use crate::vector::RuntimeEmbedders;
use crate::{DocumentId, Index, InternalError, Result};

// 1. a parallel iterator of visitables
// implement the latter on dump::VectorReader
// add skip vectors to regular indexing ops
// call import vectors
// write vector files

pub trait Visitor {
    type Error: 'static + std::fmt::Debug;

    fn on_current_embedder_change(&mut self, name: &str)
        -> std::result::Result<usize, Self::Error>;
    fn on_current_store_change(
        &mut self,
        name: Option<&str>,
    ) -> std::result::Result<(), Self::Error>;
    fn on_current_docid_change(
        &mut self,
        external_docid: &str,
    ) -> std::result::Result<(), Self::Error>;
    fn on_set_vector(&mut self, v: &[f32]) -> std::result::Result<(), Self::Error>;
    fn on_set_vectors_flat(&mut self, v: &[f32]) -> std::result::Result<(), Self::Error>;
}

pub trait Visitable {
    type Error: std::fmt::Debug;
    fn visit<V: Visitor>(
        &self,
        v: &mut V,
    ) -> std::result::Result<std::result::Result<(), V::Error>, Self::Error>;
}

struct ImportVectorVisitor<'a, 'b, MSP> {
    embedder: Option<EmbedderData>,
    store_id: Option<u8>,
    docid: Option<DocumentId>,
    sender: EmbeddingSender<'a, 'b>,
    rtxn: RoTxn<'a, WithoutTls>,
    index: &'a Index,
    runtimes: &'a RuntimeEmbedders,
    must_stop_processing: MSP,
}

impl<'a, 'b, MSP> ImportVectorVisitor<'a, 'b, MSP>
where
    MSP: Fn() -> bool + Sync,
{
    pub fn new(
        sender: EmbeddingSender<'a, 'b>,
        index: &'a Index,
        rtxn: RoTxn<'a, WithoutTls>,
        runtimes: &'a RuntimeEmbedders,
        must_stop_processing: MSP,
    ) -> Self {
        Self {
            embedder: None,
            store_id: None,
            docid: None,
            sender,
            rtxn,
            index,
            runtimes,
            must_stop_processing,
        }
    }
}

struct EmbedderData {
    id: u8,
    dimensions: usize,
    name: String,
}

impl<MSP> Visitor for ImportVectorVisitor<'_, '_, MSP>
where
    MSP: Fn() -> bool + Sync,
{
    type Error = crate::Error;

    fn on_current_embedder_change(
        &mut self,
        name: &str,
    ) -> std::result::Result<usize, Self::Error> {
        if (self.must_stop_processing)() {
            return Err(InternalError::AbortedIndexation.into());
        }
        let embedder_id = self.index.embedding_configs().embedder_id(&self.rtxn, name)?.unwrap();
        let embedder_name = name.to_string();
        let runtime_embedder = self.runtimes.get(name).unwrap();
        let dimensions = runtime_embedder.embedder.dimensions();
        self.embedder = Some(EmbedderData { id: embedder_id, dimensions, name: embedder_name });
        self.store_id = None;
        self.docid = None;
        Ok(dimensions)
    }

    fn on_current_store_change(
        &mut self,
        name: Option<&str>,
    ) -> std::result::Result<(), Self::Error> {
        if (self.must_stop_processing)() {
            return Err(InternalError::AbortedIndexation.into());
        }
        self.store_id = if let Some(fragment_name) = name {
            let embedder_name = self.embedder.as_ref().map(|e| &e.name).unwrap();
            let fragments = self.runtimes.get(embedder_name).unwrap().fragments();
            Some(
                fragments[fragments
                    .binary_search_by(|fragment| fragment.name.as_str().cmp(fragment_name))
                    .unwrap()]
                .id,
            )
        } else {
            None
        };
        Ok(())
    }

    fn on_current_docid_change(
        &mut self,
        external_docid: &str,
    ) -> std::result::Result<(), Self::Error> {
        if (self.must_stop_processing)() {
            return Err(InternalError::AbortedIndexation.into());
        }
        let docid = self.index.external_documents_ids().get(&self.rtxn, external_docid)?.unwrap();
        self.docid = Some(docid);
        Ok(())
    }

    fn on_set_vector(&mut self, v: &[f32]) -> std::result::Result<(), Self::Error> {
        if (self.must_stop_processing)() {
            return Err(InternalError::AbortedIndexation.into());
        }
        self.sender.set_vector(
            self.docid.unwrap(),
            self.embedder.as_ref().unwrap().id,
            self.store_id.unwrap(),
            Some(v),
        )
    }

    fn on_set_vectors_flat(&mut self, v: &[f32]) -> std::result::Result<(), Self::Error> {
        if (self.must_stop_processing)() {
            return Err(InternalError::AbortedIndexation.into());
        }
        let embedder = self.embedder.as_ref().unwrap();
        self.sender.set_vectors_flat(self.docid.unwrap(), embedder.id, embedder.dimensions, v)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn import_vectors<MSP, V: Visitable + Sync>(
    visitables: &[V],
    statuses: HashMap<String, EmbeddingStatus>,
    must_stop_processing: MSP,
    progress: &Progress,
    indexer_span: Span,
    extractor_sender: ExtractorBbqueueSender,
    finished_extraction: &AtomicBool,
    index: &Index,
    runtimes: &RuntimeEmbedders,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
{
    let span = tracing::trace_span!(target: "indexing::vectors", parent: &indexer_span, "import");
    let _entered = span.enter();
    let rtxn = index.read_txn()?;
    let embedders = index.embedding_configs();
    let embedding_sender = extractor_sender.embeddings();

    for (name, status) in statuses {
        let Some(mut info) = embedders.embedder_info(&rtxn, &name)? else { continue };
        info.embedding_status = status;
        embedding_sender.embedding_status(&name, info)?;
    }

    visitables.into_par_iter().try_arc_for_each_try_init(
        || {
            let rtxn = index.read_txn()?;
            let v = ImportVectorVisitor::new(
                extractor_sender.embeddings(),
                index,
                rtxn,
                runtimes,
                &must_stop_processing,
            );
            Ok(v)
        },
        |context, visitable| visitable.visit(context).unwrap().map_err(Arc::new),
    )?;

    progress.update_progress(IndexingStep::WaitingForDatabaseWrites);
    finished_extraction.store(true, std::sync::atomic::Ordering::Relaxed);

    Result::Ok(())
}
