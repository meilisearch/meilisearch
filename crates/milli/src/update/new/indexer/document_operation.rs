use std::sync::atomic::Ordering;

use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use bumparaw_collections::RawMap;
use hashbrown::hash_map::Entry;
use heed::RoTxn;
use memmap2::Mmap;
use rayon::slice::ParallelSlice;
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;
use serde_json::Deserializer;

use super::super::document_change::DocumentChange;
use super::document_changes::{DocumentChangeContext, DocumentChanges};
use super::guess_primary_key::retrieve_or_guess_primary_key;
use crate::documents::PrimaryKey;
use crate::progress::{AtomicPayloadStep, Progress};
use crate::update::new::document::Versions;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::{Deletion, Insertion, Update};
use crate::update::{AvailableIds, IndexDocumentsMethod};
use crate::{DocumentId, Error, FieldsIdsMap, Index, InternalError, Result, UserError};

#[derive(Default)]
pub struct DocumentOperation<'pl> {
    operations: Vec<Payload<'pl>>,
}

impl<'pl> DocumentOperation<'pl> {
    pub fn new() -> Self {
        Self { operations: Default::default() }
    }

    /// Append a replacement of documents.
    ///
    /// The payload is expected to be in the NDJSON format
    pub fn replace_documents(&mut self, payload: &'pl Mmap) -> Result<()> {
        #[cfg(unix)]
        payload.advise(memmap2::Advice::Sequential)?;
        self.operations.push(Payload::Replace(&payload[..]));
        Ok(())
    }

    /// Append an update of documents.
    ///
    /// The payload is expected to be in the NDJSON format
    pub fn update_documents(&mut self, payload: &'pl Mmap) -> Result<()> {
        #[cfg(unix)]
        payload.advise(memmap2::Advice::Sequential)?;
        self.operations.push(Payload::Update(&payload[..]));
        Ok(())
    }

    /// Append a deletion of documents IDs.
    ///
    /// The list is a set of external documents IDs.
    pub fn delete_documents(&mut self, to_delete: &'pl [&'pl str]) {
        self.operations.push(Payload::Deletion(to_delete))
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::document_operation")]
    pub fn into_changes<MSP>(
        self,
        indexer: &'pl Bump,
        index: &Index,
        rtxn: &'pl RoTxn<'pl>,
        primary_key_from_op: Option<&'pl str>,
        new_fields_ids_map: &mut FieldsIdsMap,
        must_stop_processing: &MSP,
        progress: Progress,
    ) -> Result<(DocumentOperationChanges<'pl>, Vec<PayloadStats>, Option<PrimaryKey<'pl>>)>
    where
        MSP: Fn() -> bool,
    {
        progress.update_progress(IndexingStep::PreparingPayloads);
        let Self { operations } = self;

        let documents_ids = index.documents_ids(rtxn)?;
        let mut operations_stats = Vec::new();
        let mut available_docids = AvailableIds::new(&documents_ids);
        let mut docids_version_offsets = hashbrown::HashMap::new();
        let mut primary_key = None;

        let payload_count = operations.len();
        let (step, progress_step) = AtomicPayloadStep::new(payload_count as u32);
        progress.update_progress(progress_step);

        for (payload_index, operation) in operations.into_iter().enumerate() {
            if must_stop_processing() {
                return Err(InternalError::AbortedIndexation.into());
            }
            step.store(payload_index as u32, Ordering::Relaxed);

            let mut bytes = 0;
            let result = match operation {
                Payload::Replace(payload) => extract_addition_payload_changes(
                    indexer,
                    index,
                    rtxn,
                    primary_key_from_op,
                    &mut primary_key,
                    new_fields_ids_map,
                    &mut available_docids,
                    &mut bytes,
                    &docids_version_offsets,
                    IndexDocumentsMethod::ReplaceDocuments,
                    payload,
                ),
                Payload::Update(payload) => extract_addition_payload_changes(
                    indexer,
                    index,
                    rtxn,
                    primary_key_from_op,
                    &mut primary_key,
                    new_fields_ids_map,
                    &mut available_docids,
                    &mut bytes,
                    &docids_version_offsets,
                    IndexDocumentsMethod::UpdateDocuments,
                    payload,
                ),
                Payload::Deletion(to_delete) => extract_deletion_payload_changes(
                    index,
                    rtxn,
                    &mut available_docids,
                    &docids_version_offsets,
                    to_delete,
                ),
            };

            let mut document_count = 0;
            let error = match result {
                Ok(new_docids_version_offsets) => {
                    document_count = new_docids_version_offsets.len() as u64;
                    // If we don't have any error then we can merge the content of this payload
                    // into to main payload. Else we just drop this payload extraction.
                    merge_version_offsets(&mut docids_version_offsets, new_docids_version_offsets);
                    None
                }
                Err(Error::UserError(user_error)) => Some(user_error),
                Err(e) => return Err(e),
            };
            operations_stats.push(PayloadStats { document_count, bytes, error });
        }
        step.store(payload_count as u32, Ordering::Relaxed);

        // TODO We must drain the HashMap into a Vec because rayon::hash_map::IntoIter: !Clone
        let mut docids_version_offsets: bumpalo::collections::vec::Vec<_> =
            docids_version_offsets.drain().collect_in(indexer);

        // Reorder the offsets to make sure we iterate on the file sequentially
        // And finally sort them. This clearly speeds up reading the update files.
        docids_version_offsets
            .sort_unstable_by_key(|(_, po)| first_update_pointer(&po.operations).unwrap_or(0));

        let docids_version_offsets = docids_version_offsets.into_bump_slice();
        Ok((DocumentOperationChanges { docids_version_offsets }, operations_stats, primary_key))
    }
}

#[allow(clippy::too_many_arguments)]
fn extract_addition_payload_changes<'r, 'pl: 'r>(
    indexer: &'pl Bump,
    index: &Index,
    rtxn: &'r RoTxn<'r>,
    primary_key_from_op: Option<&'r str>,
    primary_key: &mut Option<PrimaryKey<'r>>,
    new_fields_ids_map: &mut FieldsIdsMap,
    available_docids: &mut AvailableIds,
    bytes: &mut u64,
    main_docids_version_offsets: &hashbrown::HashMap<&'pl str, PayloadOperations<'pl>>,
    method: IndexDocumentsMethod,
    payload: &'pl [u8],
) -> Result<hashbrown::HashMap<&'pl str, PayloadOperations<'pl>>> {
    use IndexDocumentsMethod::{ReplaceDocuments, UpdateDocuments};

    let mut new_docids_version_offsets = hashbrown::HashMap::<&str, PayloadOperations<'pl>>::new();

    let mut previous_offset = 0;
    let mut iter = Deserializer::from_slice(payload).into_iter::<&RawValue>();
    while let Some(doc) = iter.next().transpose().map_err(InternalError::SerdeJson)? {
        *bytes = previous_offset as u64;

        // Only guess the primary key if it is the first document
        let retrieved_primary_key = if previous_offset == 0 {
            let doc = RawMap::from_raw_value_and_hasher(doc, FxBuildHasher, indexer)
                .map(Some)
                .map_err(UserError::SerdeJson)?;

            let result = retrieve_or_guess_primary_key(
                rtxn,
                index,
                new_fields_ids_map,
                primary_key_from_op,
                doc,
            );

            let (pk, _has_been_changed) = match result {
                Ok(Ok(pk)) => pk,
                Ok(Err(user_error)) => return Err(Error::UserError(user_error)),
                Err(error) => return Err(error),
            };

            primary_key.get_or_insert(pk)
        } else {
            // primary key was retrieved in the first iteration or in a previous payload
            primary_key.as_ref().unwrap()
        };

        let external_id = match retrieved_primary_key.extract_fields_and_docid(
            doc,
            new_fields_ids_map,
            indexer,
        ) {
            Ok(edi) => edi,
            Err(e) => return Err(e),
        };

        let external_id = external_id.to_de();
        let current_offset = iter.byte_offset();
        let document_offset = DocumentOffset { content: &payload[previous_offset..current_offset] };

        match main_docids_version_offsets.get(external_id) {
            None => {
                match index.external_documents_ids().get(rtxn, external_id) {
                    Ok(Some(docid)) => match new_docids_version_offsets.entry(external_id) {
                        Entry::Occupied(mut entry) => match method {
                            ReplaceDocuments => entry.get_mut().push_replacement(document_offset),
                            UpdateDocuments => entry.get_mut().push_update(document_offset),
                        },
                        Entry::Vacant(entry) => {
                            match method {
                                ReplaceDocuments => {
                                    entry.insert(PayloadOperations::new_replacement(
                                        docid,
                                        false, // is new
                                        document_offset,
                                    ));
                                }
                                UpdateDocuments => {
                                    entry.insert(PayloadOperations::new_update(
                                        docid,
                                        false, // is new
                                        document_offset,
                                    ));
                                }
                            }
                        }
                    },
                    Ok(None) => match new_docids_version_offsets.entry(external_id) {
                        Entry::Occupied(mut entry) => match method {
                            ReplaceDocuments => entry.get_mut().push_replacement(document_offset),
                            UpdateDocuments => entry.get_mut().push_update(document_offset),
                        },
                        Entry::Vacant(entry) => {
                            let docid = match available_docids.next() {
                                Some(docid) => docid,
                                None => return Err(UserError::DocumentLimitReached.into()),
                            };

                            match method {
                                ReplaceDocuments => {
                                    entry.insert(PayloadOperations::new_replacement(
                                        docid,
                                        true, // is new
                                        document_offset,
                                    ));
                                }
                                UpdateDocuments => {
                                    entry.insert(PayloadOperations::new_update(
                                        docid,
                                        true, // is new
                                        document_offset,
                                    ));
                                }
                            }
                        }
                    },
                    Err(e) => return Err(e.into()),
                }
            }
            Some(payload_operations) => match new_docids_version_offsets.entry(external_id) {
                Entry::Occupied(mut entry) => match method {
                    ReplaceDocuments => entry.get_mut().push_replacement(document_offset),
                    UpdateDocuments => entry.get_mut().push_update(document_offset),
                },
                Entry::Vacant(entry) => match method {
                    ReplaceDocuments => {
                        entry.insert(PayloadOperations::new_replacement(
                            payload_operations.docid,
                            payload_operations.is_new,
                            document_offset,
                        ));
                    }
                    UpdateDocuments => {
                        entry.insert(PayloadOperations::new_update(
                            payload_operations.docid,
                            payload_operations.is_new,
                            document_offset,
                        ));
                    }
                },
            },
        }

        previous_offset = iter.byte_offset();
    }

    if payload.is_empty() {
        let result = retrieve_or_guess_primary_key(
            rtxn,
            index,
            new_fields_ids_map,
            primary_key_from_op,
            None,
        );
        match result {
            Ok(Ok((pk, _))) => {
                primary_key.get_or_insert(pk);
            }
            Ok(Err(UserError::NoPrimaryKeyCandidateFound)) => (),
            Ok(Err(user_error)) => return Err(Error::UserError(user_error)),
            Err(error) => return Err(error),
        };
    }

    Ok(new_docids_version_offsets)
}

fn extract_deletion_payload_changes<'s, 'pl: 's>(
    index: &Index,
    rtxn: &RoTxn,
    available_docids: &mut AvailableIds,
    main_docids_version_offsets: &hashbrown::HashMap<&'s str, PayloadOperations<'pl>>,
    to_delete: &'pl [&'pl str],
) -> Result<hashbrown::HashMap<&'s str, PayloadOperations<'pl>>> {
    let mut new_docids_version_offsets = hashbrown::HashMap::<&str, PayloadOperations<'pl>>::new();

    for external_id in to_delete {
        match main_docids_version_offsets.get(external_id) {
            None => {
                match index.external_documents_ids().get(rtxn, external_id) {
                    Ok(Some(docid)) => {
                        match new_docids_version_offsets.entry(external_id) {
                            Entry::Occupied(mut entry) => entry.get_mut().push_deletion(),
                            Entry::Vacant(entry) => {
                                entry.insert(PayloadOperations::new_deletion(
                                    docid, false, // is new
                                ));
                            }
                        }
                    }
                    Ok(None) => {
                        let docid = match available_docids.next() {
                            Some(docid) => docid,
                            None => return Err(UserError::DocumentLimitReached.into()),
                        };
                        match new_docids_version_offsets.entry(external_id) {
                            Entry::Occupied(mut entry) => entry.get_mut().push_deletion(),
                            Entry::Vacant(entry) => {
                                entry.insert(PayloadOperations::new_deletion(
                                    docid, true, // is new
                                ));
                            }
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(payload_operations) => match new_docids_version_offsets.entry(external_id) {
                Entry::Occupied(mut entry) => entry.get_mut().push_deletion(),
                Entry::Vacant(entry) => {
                    entry.insert(PayloadOperations::new_deletion(
                        payload_operations.docid,
                        payload_operations.is_new,
                    ));
                }
            },
        }
    }

    Ok(new_docids_version_offsets)
}

fn merge_version_offsets<'s, 'pl>(
    main: &mut hashbrown::HashMap<&'s str, PayloadOperations<'pl>>,
    new: hashbrown::HashMap<&'s str, PayloadOperations<'pl>>,
) {
    // We cannot swap like nothing because documents
    // operations must be in the right order.
    if main.is_empty() {
        return *main = new;
    }

    for (key, new_payload) in new {
        match main.entry(key) {
            Entry::Occupied(mut entry) => entry.get_mut().append_operations(new_payload.operations),
            Entry::Vacant(entry) => {
                entry.insert(new_payload);
            }
        }
    }
}

impl<'pl> DocumentChanges<'pl> for DocumentOperationChanges<'pl> {
    type Item = (&'pl str, PayloadOperations<'pl>);

    fn iter(
        &self,
        chunk_size: usize,
    ) -> impl rayon::prelude::IndexedParallelIterator<Item = impl AsRef<[Self::Item]>> {
        self.docids_version_offsets.par_chunks(chunk_size)
    }

    fn item_to_document_change<'doc, T: MostlySend + 'doc>(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        item: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'pl: 'doc,
    {
        let (external_doc, payload_operations) = item;
        payload_operations.merge(external_doc, &context.doc_alloc)
    }

    fn len(&self) -> usize {
        self.docids_version_offsets.len()
    }
}

pub struct DocumentOperationChanges<'pl> {
    docids_version_offsets: &'pl [(&'pl str, PayloadOperations<'pl>)],
}

pub enum Payload<'pl> {
    Replace(&'pl [u8]),
    Update(&'pl [u8]),
    Deletion(&'pl [&'pl str]),
}

pub struct PayloadStats {
    pub bytes: u64,
    pub document_count: u64,
    pub error: Option<UserError>,
}

pub struct PayloadOperations<'pl> {
    /// The internal document id of the document.
    pub docid: DocumentId,
    /// Wether this document is not in the current database (visible by the rtxn).
    pub is_new: bool,
    /// The operations to perform, in order, on this document.
    pub operations: Vec<InnerDocOp<'pl>>,
}

impl<'pl> PayloadOperations<'pl> {
    fn new_replacement(docid: DocumentId, is_new: bool, offset: DocumentOffset<'pl>) -> Self {
        Self { docid, is_new, operations: vec![InnerDocOp::Replace(offset)] }
    }

    fn new_update(docid: DocumentId, is_new: bool, offset: DocumentOffset<'pl>) -> Self {
        Self { docid, is_new, operations: vec![InnerDocOp::Update(offset)] }
    }

    fn new_deletion(docid: DocumentId, is_new: bool) -> Self {
        Self { docid, is_new, operations: vec![InnerDocOp::Deletion] }
    }
}

impl<'pl> PayloadOperations<'pl> {
    fn push_replacement(&mut self, offset: DocumentOffset<'pl>) {
        self.operations.clear();
        self.operations.push(InnerDocOp::Replace(offset))
    }

    fn push_update(&mut self, offset: DocumentOffset<'pl>) {
        self.operations.push(InnerDocOp::Update(offset))
    }

    fn push_deletion(&mut self) {
        self.operations.clear();
        self.operations.push(InnerDocOp::Deletion);
    }

    fn append_operations(&mut self, mut operations: Vec<InnerDocOp<'pl>>) {
        debug_assert!(!operations.is_empty());
        if matches!(operations.first(), Some(InnerDocOp::Deletion | InnerDocOp::Replace(_))) {
            self.operations.clear();
        }
        self.operations.append(&mut operations);
    }

    /// Returns only the most recent version of a document based on the updates from the payloads.
    ///
    /// This function is only meant to be used when doing a replacement and not an update.
    fn merge<'doc>(
        &self,
        external_doc: &'doc str,
        doc_alloc: &'doc Bump,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'pl: 'doc,
    {
        match self.operations.last() {
            Some(InnerDocOp::Replace(DocumentOffset { content })) => {
                let document = serde_json::from_slice(content).unwrap();
                let document =
                    RawMap::from_raw_value_and_hasher(document, FxBuildHasher, doc_alloc)
                        .map_err(UserError::SerdeJson)?;

                if self.is_new {
                    Ok(Some(DocumentChange::Insertion(Insertion::create(
                        self.docid,
                        external_doc,
                        Versions::single(document),
                    ))))
                } else {
                    Ok(Some(DocumentChange::Update(Update::create(
                        self.docid,
                        external_doc,
                        Versions::single(document),
                        true,
                    ))))
                }
            }
            Some(InnerDocOp::Update(_)) => {
                // Search the first operation that is a tombstone which resets the document.
                let last_tombstone = self
                    .operations
                    .iter()
                    .rposition(|op| matches!(op, InnerDocOp::Deletion | InnerDocOp::Replace(_)));

                // Track when we must ignore previous document versions from the rtxn.
                let from_scratch = last_tombstone.is_some();

                // We ignore deletion and keep the replacement to create the appropriate versions.
                let operations = match last_tombstone {
                    Some(i) => match self.operations[i] {
                        InnerDocOp::Deletion => &self.operations[i + 1..],
                        InnerDocOp::Replace(_) => &self.operations[i..],
                        InnerDocOp::Update(_) => unreachable!("Found a non-tombstone operation"),
                    },
                    None => &self.operations[..],
                };

                // We collect the versions to generate the appropriate document.
                let versions = operations.iter().map(|operation| {
                    let DocumentOffset { content } = match operation {
                        InnerDocOp::Replace(offset) | InnerDocOp::Update(offset) => offset,
                        InnerDocOp::Deletion => unreachable!("Deletion in document operations"),
                    };

                    let document = serde_json::from_slice(content).unwrap();
                    let document =
                        RawMap::from_raw_value_and_hasher(document, FxBuildHasher, doc_alloc)
                            .map_err(UserError::SerdeJson)?;

                    Ok(document)
                });

                let Some(versions) = Versions::multiple(versions)? else { return Ok(None) };

                if self.is_new {
                    Ok(Some(DocumentChange::Insertion(Insertion::create(
                        self.docid,
                        external_doc,
                        versions,
                    ))))
                } else {
                    Ok(Some(DocumentChange::Update(Update::create(
                        self.docid,
                        external_doc,
                        versions,
                        from_scratch,
                    ))))
                }
            }
            Some(InnerDocOp::Deletion) => {
                return if self.is_new {
                    Ok(None)
                } else {
                    let deletion = Deletion::create(self.docid, external_doc);
                    Ok(Some(DocumentChange::Deletion(deletion)))
                };
            }
            None => unreachable!("We must not have an empty set of operations on a document"),
        }
    }
}

#[derive(Clone)]
pub enum InnerDocOp<'pl> {
    Replace(DocumentOffset<'pl>),
    Update(DocumentOffset<'pl>),
    Deletion,
}

/// Represents an offset where a document lives
/// in an mmapped grenad reader file.
#[derive(Clone)]
pub struct DocumentOffset<'pl> {
    /// The mmapped payload files.
    pub content: &'pl [u8],
}

/// Returns the first pointer of the first change in a document.
///
/// This is used to sort the documents in update file content order
/// and read the update file in order to largely speed up the indexation.
pub fn first_update_pointer(docops: &[InnerDocOp]) -> Option<usize> {
    docops.iter().find_map(|ido: &_| match ido {
        InnerDocOp::Replace(replace) => Some(replace.content.as_ptr() as usize),
        InnerDocOp::Update(update) => Some(update.content.as_ptr() as usize),
        InnerDocOp::Deletion => None,
    })
}
