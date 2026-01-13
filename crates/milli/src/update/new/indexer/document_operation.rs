use std::sync::atomic::Ordering;
use std::{mem, ops, ptr, vec};

use bstr::ByteSlice;
use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::Bump;
use bumparaw_collections::RawMap;
use heed::RoTxn;
use indexmap::map::Entry;
use indexmap::IndexMap;
use memmap2::Mmap;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use rayon::slice::{ParallelSlice, ParallelSliceMut as _};
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;
use serde_json::Deserializer;
use thread_local::ThreadLocal;

use super::super::document_change::DocumentChange;
use super::document_changes::DocumentChanges;
use super::guess_primary_key::retrieve_or_guess_primary_key;
use crate::documents::PrimaryKey;
use crate::progress::{AtomicPayloadStep, Progress};
use crate::update::new::document::{DocumentContext, Versions};
use crate::update::new::indexer::current_edition::sharding::Shards;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::{DocumentIdentifiers, Insertion, Update};
use crate::update::{AvailableIds, IndexDocumentsMethod, MissingDocumentPolicy};
use crate::{DocumentId, Error, FieldsIdsMap, Index, InternalError, Result, UserError};

/// The set of operations to be applied to multiple documents in an index.
#[derive(Default)]
pub struct IndexOperations<'pl> {
    operations: Vec<Payload<'pl>>,
}

impl<'pl> IndexOperations<'pl> {
    pub fn new() -> Self {
        Self { operations: Default::default() }
    }

    /// Append a replacement of documents.
    ///
    /// The payload is expected to be in the NDJSON format
    pub fn replace_documents(
        &mut self,
        payload: &'pl Mmap,
        on_missing_document: MissingDocumentPolicy,
    ) -> Result<()> {
        #[cfg(unix)]
        payload.advise(memmap2::Advice::Sequential)?;
        self.operations.push(Payload::Replace { payload: &payload[..], on_missing_document });
        Ok(())
    }

    /// Append an update of documents.
    ///
    /// The payload is expected to be in the NDJSON format
    pub fn update_documents(
        &mut self,
        payload: &'pl Mmap,
        on_missing_document: MissingDocumentPolicy,
    ) -> Result<()> {
        #[cfg(unix)]
        payload.advise(memmap2::Advice::Sequential)?;
        self.operations.push(Payload::Update { payload: &payload[..], on_missing_document });
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
        shards: Option<&Shards>,
    ) -> Result<(DocumentOperationChanges<'pl>, Vec<PayloadStats>, Option<PrimaryKey<'pl>>)>
    where
        MSP: Fn() -> bool + Sync,
    {
        progress.update_progress(IndexingStep::PreparingPayloads);
        let Self { operations } = self;

        // We first fetch the primary key from the index...
        let primary_key = retrieve_or_guess_primary_key(
            rtxn,
            index,
            new_fields_ids_map,
            primary_key_from_op,
            None,
        )?
        .map(|(pk, _)| pk);

        // ...and if not present we try to guess it from
        // the operations and ignore the useless ones.
        let (primary_key, pre_payload_stats, remaining_operations) = match primary_key {
            Ok(pk) => (pk, Vec::new(), operations),
            Err(_user_error) => {
                let (primary_key, pre_payload_stats, operations) =
                    fetch_primary_and_ignore_payloads(
                        indexer,
                        index,
                        rtxn,
                        primary_key_from_op,
                        new_fields_ids_map,
                        operations,
                    )?;

                match primary_key {
                    Some(pk) => (pk, pre_payload_stats, operations),
                    None => {
                        return Ok((
                            DocumentOperationChanges { docids_version_offsets: &[] },
                            pre_payload_stats,
                            None,
                        ));
                    }
                }
            }
        };

        let payload_count: u32 = remaining_operations.len().try_into().unwrap();
        let (step, progress_step) = AtomicPayloadStep::new(payload_count);
        progress.update_progress(progress_step);

        let IndexedPayloadOperations { document_operations, fields_ids_map, payload_stats } =
            remaining_operations
                .into_par_iter()
                .map(|payload| {
                    if must_stop_processing() {
                        return Err(InternalError::AbortedIndexation.into());
                    }
                    step.fetch_add(1, Ordering::Relaxed);
                    IndexedPayloadOperations::from_payload(payload, &primary_key, shards)
                })
                .try_reduce(IndexedPayloadOperations::default, |lhs, rhs| lhs | rhs)?;

        step.store(payload_count, Ordering::Relaxed);

        // We must drain the HashMap into a Vec because rayon::hash_map::IntoIter: !Clone
        progress.update_progress(IndexingStep::AssigningDocumentsIds);
        let external_documents_ids = index.external_documents_ids();

        // We read the database in parallel to retrieve the internal IDs of the existing
        // documents and mark the ones that need a new ID. To avoid creating a lot of
        // read transactions we prefer store the read transactions in a thread-local variable.
        let thread_local_rtxns = ThreadLocal::new();
        let extracted_docids = document_operations
            .par_keys()
            .enumerate()
            .map(|(_, external_id)| {
                let local_rtxn = thread_local_rtxns.get_or_try(|| index.read_txn())?;
                external_documents_ids.get(local_rtxn, external_id)
            })
            .collect_vec_list();

        let documents_ids = index.documents_ids(rtxn)?;
        let mut available_ids = AvailableIds::new(&documents_ids);
        let number_of_operations = document_operations.len();
        let mut docids_version_offsets = BumpVec::with_capacity_in(number_of_operations, indexer);

        let docids = extracted_docids.into_iter().flatten();
        for ((external_id, ops), docid_result) in document_operations.into_iter().zip(docids) {
            let (docid, is_missing) = match docid_result? {
                Some(docid) => (docid, false),
                None => (available_ids.next().ok_or(UserError::DocumentLimitReached)?, true),
            };

            if let Some(ops) = ops.into_payload_operations(is_missing, docid) {
                let external_id = &*indexer.alloc_str(&external_id);
                docids_version_offsets.push((external_id, ops));
            }
        }

        // We insert all the fields ids discovered in the payload that were extracted
        // from the parallel threads into the main fields ids map. There couldn't be more
        // than 2^16 fields so no need to optimize this part.
        for field_name in fields_ids_map.names() {
            new_fields_ids_map.insert(field_name).ok_or(UserError::AttributeLimitReached)?;
        }

        // Reorder the offsets to make sure we iterate on the file sequentially
        // And finally sort them. This clearly speeds up reading the update files.
        progress.update_progress(IndexingStep::ReorderingPayloadOffsets);
        docids_version_offsets
            .par_sort_unstable_by_key(|(_, po)| first_update_pointer(&po.operations).unwrap_or(0));

        Ok((
            DocumentOperationChanges {
                docids_version_offsets: docids_version_offsets.into_bump_slice(),
            },
            // Once we got the payload stats for the valid operations
            // we must prepend the stats from skipped ones.
            pre_payload_stats.into_iter().chain(payload_stats).collect(),
            Some(primary_key),
        ))
    }
}

/// Fetches the primary key from the operations and removes the ignored ones.
/// Collecting the stats and the useful payloads for future use.
fn fetch_primary_and_ignore_payloads<'pl>(
    bump: &'pl Bump,
    index: &Index,
    rtxn: &'pl RoTxn<'pl>,
    primary_key_from_op: Option<&'pl str>,
    new_fields_ids_map: &mut FieldsIdsMap,
    operations: Vec<Payload<'pl>>,
) -> Result<(Option<PrimaryKey<'pl>>, Vec<PayloadStats>, Vec<Payload<'pl>>)> {
    let mut payload_stats = Vec::new();
    let mut remaining_operations = Vec::new();
    let mut primary_key = None;

    for operation in operations {
        if primary_key.is_some() {
            remaining_operations.push(operation);
            continue;
        }

        let stats = match operation {
            Payload::Replace { payload: p, .. } | Payload::Update { payload: p, .. } => {
                // Fetches the first document from payload bytes.
                let first_document = Deserializer::from_slice(p)
                    .into_iter::<&RawValue>()
                    .next()
                    .map(|v| {
                        v.and_then(|v| RawMap::from_raw_value_and_hasher(v, FxBuildHasher, bump))
                    })
                    .transpose()
                    .map_err(InternalError::SerdeJson)?;

                let primary_key_result = retrieve_or_guess_primary_key(
                    rtxn,
                    index,
                    new_fields_ids_map,
                    primary_key_from_op,
                    first_document,
                )?;

                match primary_key_result {
                    Ok((pk, _)) => {
                        primary_key = Some(pk);
                        // From now on, we will collect the remaining
                        // operations in a vector to manage them later on.
                        remaining_operations.push(operation);
                        continue;
                    }
                    Err(error) => PayloadStats {
                        bytes: p.len() as u64,
                        document_count: 0,
                        // We do not consider errors when payloads are empty.
                        error: if p.trim().is_empty() { None } else { Some(error) },
                    },
                }
            }
            Payload::Deletion(_) => {
                // We reach this when we don't have a primary key so it's impossible to delete documents.
                PayloadStats { bytes: 0, document_count: 0, error: None }
            }
        };

        payload_stats.push(stats);
    }

    Ok((primary_key, payload_stats, remaining_operations))
}

/// The correctly ordered operations that were extracted from the payload.
///
/// We don't need the payload index as we merge them in order by using
/// the rayon `try_reduce` method.
#[derive(Default)]
struct IndexedPayloadOperations<'pl> {
    /// Represents the operations that will be applied to the documents of this payload.
    ///
    /// The key corresponds to the external document id.
    document_operations: IndexMap<String, DocumentOperations<'pl>>,

    /// The local fields ids map for this payload or a union of payloads.
    fields_ids_map: FieldsIdsMap,

    /// Some interesting stats and possible errors.
    ///
    /// The order is the same as the payload files.
    payload_stats: Vec<PayloadStats>,
}

impl<'pl> IndexedPayloadOperations<'pl> {
    fn from_payload(
        payload_operation: Payload<'pl>,
        primary_key: &PrimaryKey<'_>,
        shards: Option<&Shards>,
    ) -> Result<Self> {
        use IndexDocumentsMethod::*;

        let (document_operations, payload_stats, fields_ids_map) = match payload_operation {
            Payload::Replace { payload, on_missing_document } => extract_payload_changes(
                payload,
                on_missing_document,
                primary_key,
                ReplaceDocuments,
                shards,
            )?,
            Payload::Update { payload, on_missing_document } => extract_payload_changes(
                payload,
                on_missing_document,
                primary_key,
                UpdateDocuments,
                shards,
            )?,
            Payload::Deletion(docids) => {
                let (document_operations, stats) = extract_payload_deletions(docids, shards);
                (document_operations, stats, FieldsIdsMap::default())
            }
        };

        Ok(IndexedPayloadOperations {
            document_operations,
            fields_ids_map,
            payload_stats: vec![payload_stats],
        })
    }
}

impl ops::BitOr for IndexedPayloadOperations<'_> {
    type Output = Result<Self>;

    /// The merge operation consists of merging the document operations, in order: rhs into lhs.
    fn bitor(self, rhs: Self) -> Self::Output {
        let IndexedPayloadOperations {
            mut document_operations,
            mut fields_ids_map,
            mut payload_stats,
        } = self;
        let IndexedPayloadOperations {
            document_operations: rhs_document_operations,
            fields_ids_map: rhs_fields_ids_map,
            payload_stats: mut rhs_payload_stats,
        } = rhs;

        for (external_document_id, rhs_docops) in rhs_document_operations {
            match document_operations.entry(external_document_id) {
                Entry::Occupied(mut entry) => {
                    // Unfortunately we don't have the OccupiedEntry::replace_entry_with method
                    // on the IndexMap entry. This operation would be much more elegant otherwise.
                    let lhs_docops = mem::replace(entry.get_mut(), DocumentOperations::empty());
                    match DocumentOperations::from_iter(
                        lhs_docops.into_iter().chain(rhs_docops),
                        DocumentExistence::Unknown,
                    ) {
                        Some(operations) => entry.insert(operations),
                        None => entry.shift_remove(),
                    };
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(rhs_docops);
                }
            }
        }

        for field_name in rhs_fields_ids_map.names() {
            fields_ids_map.insert(field_name).ok_or(UserError::AttributeLimitReached)?;
        }

        payload_stats.append(&mut rhs_payload_stats);

        Ok(IndexedPayloadOperations { document_operations, fields_ids_map, payload_stats })
    }
}

/// A set of operations applied to a single document in a particular order.
struct DocumentOperations<'pl>(Vec<DocumentOperation<'pl>>);

impl<'pl> DocumentOperations<'pl> {
    /// Creates an empty set of operations.
    ///
    /// This is useful mostly when merging documents operations retrieved
    /// from payload and shouldn't be considered a valid state otherwise.
    fn empty() -> Self {
        DocumentOperations(Vec::new())
    }

    fn one_deletion() -> Self {
        DocumentOperations(vec![DocumentOperation::Deletion])
    }

    fn from_raw_value(
        method: IndexDocumentsMethod,
        document: &'pl RawValue,
        on_missing_document: MissingDocumentPolicy,
    ) -> Self {
        use DocumentOperation::*;
        use IndexDocumentsMethod::*;

        let operation = match method {
            ReplaceDocuments => Replacement { document, on_missing_document },
            UpdateDocuments => Update { document, on_missing_document },
        };

        DocumentOperations(vec![operation])
    }

    fn from_iter<I>(operations: I, document_existence: DocumentExistence) -> Option<Self>
    where
        I: IntoIterator<Item = DocumentOperation<'pl>>,
    {
        use DocumentOperation::*;
        use MissingDocumentPolicy::*;

        let mut document_operations = Vec::new();
        for operation in operations {
            let existence_after_last_op = match document_operations.last() {
                Some(Replacement { .. } | Update { .. }) => DocumentExistence::Exists,
                Some(Deletion) => DocumentExistence::Missing,
                None => document_existence,
            };

            match (existence_after_last_op, operation) {
                // when the document is missing for sure after the last operation,
                // and the next operation requires skipping creation,
                // we skip this operation
                (
                    DocumentExistence::Missing,
                    Replacement { on_missing_document: Skip, .. }
                    | Update { on_missing_document: Skip, .. },
                ) => continue,
                // deletions and replacements delete all previous operations
                (_, op @ (Deletion | Replacement { .. })) => {
                    document_operations.clear();
                    document_operations.push(op);
                }
                // updates executes after the previous operations
                (_, op @ Update { .. }) => document_operations.push(op),
            }
        }

        match (document_existence, document_operations.last()) {
            (DocumentExistence::Missing, Some(Deletion) | None) => None,
            (_, _) => Some(DocumentOperations(document_operations)),
        }
    }

    fn into_payload_operations(
        self,
        was_missing: bool,
        docid: DocumentId,
    ) -> Option<PayloadOperations<'pl>> {
        use DocumentExistence::*;
        use DocumentOperation::*;

        let document_existence = if was_missing { Missing } else { Exists };
        Self::from_iter(self.0, document_existence).map(|DocumentOperations(operations)| {
            PayloadOperations {
                docid,
                is_new: was_missing, // same thing
                operations: operations
                    .into_iter()
                    .map(|op| match op {
                        Replacement { document, .. } => {
                            InnerDocOp::Replace(DocumentOffset { content: document })
                        }
                        Update { document, .. } => {
                            InnerDocOp::Update(DocumentOffset { content: document })
                        }
                        Deletion => InnerDocOp::Deletion,
                    })
                    .collect(),
            }
        })
    }

    fn push_raw_value(
        &mut self,
        method: IndexDocumentsMethod,
        raw_value: &'pl RawValue,
        on_missing_document: MissingDocumentPolicy,
    ) {
        use DocumentOperation::*;
        use IndexDocumentsMethod::*;

        let operation = match method {
            ReplaceDocuments => Replacement { document: raw_value, on_missing_document },
            UpdateDocuments => Update { document: raw_value, on_missing_document },
        };

        self.0.push(operation);
    }
}

impl<'pl> IntoIterator for DocumentOperations<'pl> {
    type Item = DocumentOperation<'pl>;
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, Copy)]
enum DocumentExistence {
    Unknown,
    Exists,
    Missing,
}

/// Represents an operation to be performed on a document.
enum DocumentOperation<'pl> {
    Replacement { document: &'pl RawValue, on_missing_document: MissingDocumentPolicy },
    Update { document: &'pl RawValue, on_missing_document: MissingDocumentPolicy },
    Deletion,
}

fn extract_payload_changes<'pl>(
    payload: &'pl [u8],
    on_missing_document: MissingDocumentPolicy,
    primary_key: &PrimaryKey<'_>,
    method: IndexDocumentsMethod,
    shards: Option<&Shards>,
) -> Result<(IndexMap<String, DocumentOperations<'pl>>, PayloadStats, FieldsIdsMap)> {
    let mut new_docids_version_offsets = IndexMap::<_, DocumentOperations>::new();
    let mut fids_map = FieldsIdsMap::new();
    let bump = bumpalo::Bump::new();

    let mut iter = Deserializer::from_slice(payload).into_iter::<&RawValue>();
    while let Some(doc) = iter.next().transpose().map_err(InternalError::SerdeJson)? {
        let external_document_id =
            match primary_key.extract_fields_and_docid(doc, &mut fids_map, &bump) {
                Ok(external_document_id) => external_document_id.to_de(),
                Err(Error::UserError(user_error)) => {
                    let payload_stats = PayloadStats {
                        bytes: payload.len() as u64,
                        document_count: 0,
                        error: Some(user_error),
                    };
                    // In case of a user error, we immediately return
                    // it and ignore the documents from this payload.
                    return Ok((IndexMap::new(), payload_stats, FieldsIdsMap::new()));
                }
                Err(error) => return Err(error),
            };

        if shards.is_some_and(|shards| !shards.must_process(external_document_id)) {
            continue;
        }

        match new_docids_version_offsets.entry(external_document_id.to_owned()) {
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().push_raw_value(method, doc, on_missing_document);
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(DocumentOperations::from_raw_value(
                    method,
                    doc,
                    on_missing_document,
                ));
            }
        }
    }

    let payload_stats = PayloadStats {
        bytes: payload.len() as u64,
        document_count: new_docids_version_offsets.len() as u64,
        error: None,
    };

    Ok((new_docids_version_offsets, payload_stats, fids_map))
}

fn extract_payload_deletions<'pl>(
    external_document_ids: &[&str],
    shards: Option<&Shards>,
) -> (IndexMap<String, DocumentOperations<'pl>>, PayloadStats) {
    let docops: IndexMap<_, _> = external_document_ids
        .iter()
        .filter(|id| shards.is_none_or(|shards| shards.must_process(id)))
        .map(|id| (id.to_string(), DocumentOperations::one_deletion()))
        .collect();
    let payload_stats = PayloadStats { bytes: 0, document_count: docops.len() as u64, error: None };
    (docops, payload_stats)
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
        context: &'doc DocumentContext<T>,
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
    Replace { payload: &'pl [u8], on_missing_document: MissingDocumentPolicy },
    Update { payload: &'pl [u8], on_missing_document: MissingDocumentPolicy },
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
                let document = RawMap::from_raw_value_and_hasher(content, FxBuildHasher, doc_alloc)
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

                    let document =
                        RawMap::from_raw_value_and_hasher(content, FxBuildHasher, doc_alloc)
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
                if self.is_new {
                    Ok(None)
                } else {
                    let deletion = DocumentIdentifiers::create(self.docid, external_doc);
                    Ok(Some(DocumentChange::Deletion(deletion)))
                }
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
    pub content: &'pl RawValue,
}

/// Returns the first pointer of the first change in a document.
///
/// This is used to sort the documents in update file content order
/// and read the update file in order to largely speed up the indexation.
pub fn first_update_pointer(docops: &[InnerDocOp]) -> Option<usize> {
    // A &RawValue is an unsized transparent type that simply wraps an str. The ref (&)
    // corresponds to the pointer to the str and therefore a direct access into memory.
    //
    // <https://docs.rs/serde_json/1.0.148/src/serde_json/raw.rs.html#115-119>
    docops.iter().find_map(|ido: &_| match ido {
        InnerDocOp::Replace(replace) => Some(ptr::from_ref(replace.content) as *const () as usize),
        InnerDocOp::Update(update) => Some(ptr::from_ref(update.content) as *const () as usize),
        InnerDocOp::Deletion => None,
    })
}
