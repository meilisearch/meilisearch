use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use heed::RoTxn;
use memmap2::Mmap;
use rayon::iter::IntoParallelIterator;
use serde_json::value::RawValue;
use IndexDocumentsMethod as Idm;

use super::super::document_change::DocumentChange;
use super::document_changes::{DocumentChangeContext, DocumentChanges, MostlySend};
use crate::documents::{DocumentIdExtractionError, PrimaryKey};
use crate::update::new::document::DocumentFromVersions;
use crate::update::new::document_change::Versions;
use crate::update::new::indexer::de::DocumentVisitor;
use crate::update::new::{Deletion, Insertion, Update};
use crate::update::{AvailableIds, IndexDocumentsMethod};
use crate::{DocumentId, Error, FieldsIdsMap, Index, Result, UserError};

pub struct DocumentOperation<'pl> {
    operations: Vec<Payload<'pl>>,
    index_documents_method: IndexDocumentsMethod,
}

pub struct DocumentOperationChanges<'pl> {
    docids_version_offsets: &'pl [(&'pl str, ((u32, bool), &'pl [InnerDocOp<'pl>]))],
    index_documents_method: IndexDocumentsMethod,
}

pub enum Payload<'pl> {
    Addition(&'pl [u8]),
    Deletion(&'pl [&'pl str]),
}

pub struct PayloadStats {
    pub document_count: usize,
    pub bytes: u64,
}

#[derive(Clone)]
pub enum InnerDocOp<'pl> {
    Addition(DocumentOffset<'pl>),
    Deletion,
}

/// Represents an offset where a document lives
/// in an mmapped grenad reader file.
#[derive(Clone)]
pub struct DocumentOffset<'pl> {
    /// The mmapped payload files.
    pub content: &'pl [u8],
}

impl<'pl> DocumentOperation<'pl> {
    pub fn new(method: IndexDocumentsMethod) -> Self {
        Self { operations: Default::default(), index_documents_method: method }
    }

    /// TODO please give me a type
    /// The payload is expected to be in the grenad format
    pub fn add_documents(&mut self, payload: &'pl Mmap) -> Result<PayloadStats> {
        payload.advise(memmap2::Advice::Sequential)?;
        let document_count =
            memchr::memmem::find_iter(&payload[..], "}{").count().saturating_add(1);
        self.operations.push(Payload::Addition(&payload[..]));
        Ok(PayloadStats { bytes: payload.len() as u64, document_count })
    }

    pub fn delete_documents(&mut self, to_delete: &'pl [&'pl str]) {
        self.operations.push(Payload::Deletion(to_delete))
    }

    pub fn into_changes(
        self,
        indexer: &'pl Bump,
        index: &Index,
        rtxn: &RoTxn,
        primary_key: &PrimaryKey,
        new_fields_ids_map: &mut FieldsIdsMap,
    ) -> Result<DocumentOperationChanges<'pl>> {
        use serde::de::Deserializer;
        // will contain nodes from the intermediate hashmap
        let document_changes_alloc = Bump::with_capacity(1024 * 1024 * 1024); // 1 MiB

        let documents_ids = index.documents_ids(rtxn)?;
        let mut available_docids = AvailableIds::new(&documents_ids);
        let mut docids_version_offsets =
            hashbrown::HashMap::<&'pl str, _, _, _>::new_in(&document_changes_alloc);

        for operation in self.operations {
            match operation {
                Payload::Addition(payload) => {
                    let mut iter =
                        serde_json::Deserializer::from_slice(payload).into_iter::<&RawValue>();

                    /// TODO manage the error
                    let mut previous_offset = 0;
                    while let Some(document) =
                        iter.next().transpose().map_err(UserError::SerdeJson)?
                    {
                        let res = document
                            .deserialize_map(DocumentVisitor::new(
                                new_fields_ids_map,
                                primary_key,
                                indexer,
                            ))
                            .map_err(UserError::SerdeJson)?;

                        let external_document_id = match res {
                            Ok(document_id) => Ok(document_id),
                            Err(DocumentIdExtractionError::InvalidDocumentId(e)) => Err(e),
                            Err(DocumentIdExtractionError::MissingDocumentId) => {
                                Err(UserError::MissingDocumentId {
                                    primary_key: primary_key.name().to_string(),
                                    document: serde_json::from_str(document.get()).unwrap(),
                                })
                            }
                            Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                                Err(UserError::TooManyDocumentIds {
                                    primary_key: primary_key.name().to_string(),
                                    document: serde_json::from_str(document.get()).unwrap(),
                                })
                            }
                        }?;

                        let current_offset = iter.byte_offset();
                        let document_operation = InnerDocOp::Addition(DocumentOffset {
                            content: &payload[previous_offset..current_offset],
                        });

                        match docids_version_offsets.get_mut(external_document_id) {
                            None => {
                                let (docid, is_new) = match index
                                    .external_documents_ids()
                                    .get(rtxn, external_document_id)?
                                {
                                    Some(docid) => (docid, false),
                                    None => (
                                        available_docids.next().ok_or(Error::UserError(
                                            UserError::DocumentLimitReached,
                                        ))?,
                                        true,
                                    ),
                                };

                                docids_version_offsets.insert(
                                    external_document_id,
                                    (
                                        (docid, is_new),
                                        bumpalo::vec![in indexer; document_operation],
                                    ),
                                );
                            }
                            Some((_, offsets)) => {
                                let useless_previous_addition = match self.index_documents_method {
                                    IndexDocumentsMethod::ReplaceDocuments => {
                                        MergeDocumentForReplacement::USELESS_PREVIOUS_CHANGES
                                    }
                                    IndexDocumentsMethod::UpdateDocuments => {
                                        MergeDocumentForUpdates::USELESS_PREVIOUS_CHANGES
                                    }
                                };

                                if useless_previous_addition {
                                    offsets.clear();
                                }

                                offsets.push(document_operation);
                            }
                        }

                        previous_offset = iter.byte_offset();
                    }
                }
                Payload::Deletion(to_delete) => {
                    for external_document_id in to_delete {
                        match docids_version_offsets.get_mut(external_document_id) {
                            None => {
                                let (docid, is_new) = match index
                                    .external_documents_ids()
                                    .get(rtxn, external_document_id)?
                                {
                                    Some(docid) => (docid, false),
                                    None => (
                                        available_docids.next().ok_or(Error::UserError(
                                            UserError::DocumentLimitReached,
                                        ))?,
                                        true,
                                    ),
                                };

                                docids_version_offsets.insert(
                                    external_document_id,
                                    (
                                        (docid, is_new),
                                        bumpalo::vec![in indexer; InnerDocOp::Deletion],
                                    ),
                                );
                            }
                            Some((_, offsets)) => {
                                offsets.clear();
                                offsets.push(InnerDocOp::Deletion);
                            }
                        }
                    }
                }
            }
        }

        // TODO We must drain the HashMap into a Vec because rayon::hash_map::IntoIter: !Clone
        let mut docids_version_offsets: bumpalo::collections::vec::Vec<_> = docids_version_offsets
            .drain()
            .map(|(item, (docid, v))| (item, (docid, v.into_bump_slice())))
            .collect_in(indexer);
        // Reorder the offsets to make sure we iterate on the file sequentially
        let sort_function_key = match self.index_documents_method {
            Idm::ReplaceDocuments => MergeDocumentForReplacement::sort_key,
            Idm::UpdateDocuments => MergeDocumentForUpdates::sort_key,
        };

        // And finally sort them
        docids_version_offsets.sort_unstable_by_key(|(_, (_, docops))| sort_function_key(docops));
        let docids_version_offsets = docids_version_offsets.into_bump_slice();
        Ok(DocumentOperationChanges {
            docids_version_offsets,
            index_documents_method: self.index_documents_method,
        })
    }
}

impl<'pl> DocumentChanges<'pl> for DocumentOperationChanges<'pl> {
    type Item = &'pl (&'pl str, ((u32, bool), &'pl [InnerDocOp<'pl>]));

    fn iter(&self) -> impl rayon::prelude::IndexedParallelIterator<Item = Self::Item> {
        self.docids_version_offsets.into_par_iter()
    }

    fn item_to_document_change<'doc, T: MostlySend + 'doc>(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        item: Self::Item,
    ) -> Result<DocumentChange<'doc>>
    where
        'pl: 'doc,
    {
        let document_merge_function = match self.index_documents_method {
            Idm::ReplaceDocuments => MergeDocumentForReplacement::merge,
            Idm::UpdateDocuments => MergeDocumentForUpdates::merge,
        };

        let (external_doc, ((internal_docid, is_new), operations)) = *item;

        let change = document_merge_function(
            internal_docid,
            external_doc,
            is_new,
            &context.doc_alloc,
            operations,
        )?;
        Ok(change)
    }
}

trait MergeChanges {
    /// Whether the payloads in the list of operations are useless or not.
    const USELESS_PREVIOUS_CHANGES: bool;

    /// Returns a key that is used to order the payloads the right way.
    fn sort_key(docops: &[InnerDocOp]) -> usize;

    fn merge<'doc>(
        docid: DocumentId,
        external_docid: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<DocumentChange<'doc>>;
}

struct MergeDocumentForReplacement;

impl MergeChanges for MergeDocumentForReplacement {
    const USELESS_PREVIOUS_CHANGES: bool = true;

    /// Reorders to read only the last change.
    fn sort_key(docops: &[InnerDocOp]) -> usize {
        let f = |ido: &_| match ido {
            InnerDocOp::Addition(add) => Some(add.content.as_ptr() as usize),
            InnerDocOp::Deletion => None,
        };
        docops.iter().rev().find_map(f).unwrap_or(0)
    }

    /// Returns only the most recent version of a document based on the updates from the payloads.
    ///
    /// This function is only meant to be used when doing a replacement and not an update.
    fn merge<'doc>(
        docid: DocumentId,
        external_doc: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<DocumentChange<'doc>> {
        match operations.last() {
            Some(InnerDocOp::Addition(DocumentOffset { content })) => {
                let document = serde_json::from_slice(content).unwrap();
                let document = raw_collections::RawMap::from_raw_value(document, doc_alloc)
                    .map_err(UserError::SerdeJson)?;

                let document = document.into_bump_slice();
                let document = DocumentFromVersions::new(Versions::Single(document));

                if is_new {
                    Ok(DocumentChange::Insertion(Insertion::create(
                        docid,
                        external_doc.to_owned(),
                        document,
                    )))
                } else {
                    Ok(DocumentChange::Update(Update::create(
                        docid,
                        external_doc.to_owned(),
                        document,
                        true,
                    )))
                }
            }
            Some(InnerDocOp::Deletion) => {
                let deletion = if is_new {
                    Deletion::create(docid, external_doc.to_owned())
                } else {
                    todo!("Do that with Louis")
                };
                Ok(DocumentChange::Deletion(deletion))
            }
            None => unreachable!("We must not have empty set of operations on a document"),
        }
    }
}

struct MergeDocumentForUpdates;

impl MergeChanges for MergeDocumentForUpdates {
    const USELESS_PREVIOUS_CHANGES: bool = false;

    /// Reorders to read the first changes first so that it's faster to read the first one and then the rest.
    fn sort_key(docops: &[InnerDocOp]) -> usize {
        let f = |ido: &_| match ido {
            InnerDocOp::Addition(add) => Some(add.content.as_ptr() as usize),
            InnerDocOp::Deletion => None,
        };
        docops.iter().find_map(f).unwrap_or(0)
    }

    /// Reads the previous version of a document from the database, the new versions
    /// in the grenad update files and merges them to generate a new boxed obkv.
    ///
    /// This function is only meant to be used when doing an update and not a replacement.
    fn merge<'doc>(
        docid: DocumentId,
        external_docid: &'doc str,
        is_new: bool,
        doc_alloc: &'doc Bump,
        operations: &'doc [InnerDocOp],
    ) -> Result<DocumentChange<'doc>> {
        if operations.is_empty() {
            unreachable!("We must not have empty set of operations on a document");
        }

        let last_deletion = operations.iter().rposition(|op| matches!(op, InnerDocOp::Deletion));
        let operations = &operations[last_deletion.map_or(0, |i| i + 1)..];

        let has_deletion = last_deletion.is_some();

        if operations.is_empty() {
            let deletion = if !is_new {
                Deletion::create(docid, external_docid.to_owned())
            } else {
                todo!("Do that with Louis")
            };

            return Ok(DocumentChange::Deletion(deletion));
        }

        let mut versions = bumpalo::collections::Vec::with_capacity_in(operations.len(), doc_alloc);

        for operation in operations {
            let DocumentOffset { content } = match operation {
                InnerDocOp::Addition(offset) => offset,
                InnerDocOp::Deletion => {
                    unreachable!("Deletion in document operations")
                }
            };

            let document = serde_json::from_slice(content).unwrap();
            let document = raw_collections::RawMap::from_raw_value(document, doc_alloc)
                .map_err(UserError::SerdeJson)?;

            let document = document.into_bump_slice();
            versions.push(document);
        }

        let versions = versions.into_bump_slice();
        let versions = match versions {
            [single] => Versions::Single(single),
            versions => Versions::Multiple(versions),
        };

        let document = DocumentFromVersions::new(versions);

        if is_new {
            Ok(DocumentChange::Insertion(Insertion::create(
                docid,
                external_docid.to_owned(),
                document,
            )))
        } else {
            Ok(DocumentChange::Update(Update::create(
                docid,
                external_docid.to_owned(),
                document,
                has_deletion,
            )))
        }
    }
}
