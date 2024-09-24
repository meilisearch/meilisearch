use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use heed::types::{Bytes, DecodeIgnore};
use heed::RoTxn;
use memmap2::Mmap;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use IndexDocumentsMethod as Idm;

use super::super::document_change::DocumentChange;
use super::super::items_pool::ItemsPool;
use super::super::{CowStr, TopLevelMap};
use super::DocumentChanges;
use crate::documents::{DocumentIdExtractionError, PrimaryKey};
use crate::update::new::{Deletion, Insertion, KvReaderFieldId, KvWriterFieldId, Update};
use crate::update::{AvailableIds, IndexDocumentsMethod};
use crate::{DocumentId, Error, FieldsIdsMap, Index, Result, UserError};

pub struct DocumentOperation<'pl> {
    operations: Vec<Payload<'pl>>,
    index_documents_method: IndexDocumentsMethod,
}

pub enum Payload<'pl> {
    Addition(&'pl [u8]),
    Deletion(Vec<String>),
}

pub struct PayloadStats {
    pub document_count: usize,
    pub bytes: u64,
}

#[derive(Clone)]
enum InnerDocOp<'pl> {
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

    pub fn delete_documents(&mut self, to_delete: Vec<String>) {
        self.operations.push(Payload::Deletion(to_delete))
    }
}

impl<'p, 'pl: 'p> DocumentChanges<'p> for DocumentOperation<'pl> {
    type Parameter = (&'p Index, &'p RoTxn<'p>, &'p PrimaryKey<'p>);

    fn document_changes(
        self,
        fields_ids_map: &mut FieldsIdsMap,
        param: Self::Parameter,
    ) -> Result<impl IndexedParallelIterator<Item = Result<DocumentChange>> + Clone + 'p> {
        let (index, rtxn, primary_key) = param;

        let documents_ids = index.documents_ids(rtxn)?;
        let mut available_docids = AvailableIds::new(&documents_ids);
        let mut docids_version_offsets = HashMap::<CowStr<'pl>, _>::new();

        for operation in self.operations {
            match operation {
                Payload::Addition(payload) => {
                    let mut iter =
                        serde_json::Deserializer::from_slice(payload).into_iter::<TopLevelMap>();

                    /// TODO manage the error
                    let mut previous_offset = 0;
                    while let Some(document) = iter.next().transpose().unwrap() {
                        // TODO Fetch all document fields to fill the fields ids map
                        document.0.keys().for_each(|key| {
                            fields_ids_map.insert(key.as_ref());
                        });

                        // TODO we must manage the TooManyDocumentIds,InvalidDocumentId
                        //      we must manage the unwrap
                        let external_document_id =
                            match primary_key.document_id_from_top_level_map(&document)? {
                                Ok(document_id) => Ok(document_id),
                                Err(DocumentIdExtractionError::InvalidDocumentId(e)) => Err(e),
                                Err(DocumentIdExtractionError::MissingDocumentId) => {
                                    Err(UserError::MissingDocumentId {
                                        primary_key: primary_key.name().to_string(),
                                        document: document.try_into().unwrap(),
                                    })
                                }
                                Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                                    Err(UserError::TooManyDocumentIds {
                                        primary_key: primary_key.name().to_string(),
                                        document: document.try_into().unwrap(),
                                    })
                                }
                            }?;

                        let current_offset = iter.byte_offset();
                        let document_operation = InnerDocOp::Addition(DocumentOffset {
                            content: &payload[previous_offset..current_offset],
                        });

                        match docids_version_offsets.get_mut(external_document_id.as_ref()) {
                            None => {
                                let docid = match index
                                    .external_documents_ids()
                                    .get(rtxn, &external_document_id)?
                                {
                                    Some(docid) => docid,
                                    None => available_docids
                                        .next()
                                        .ok_or(Error::UserError(UserError::DocumentLimitReached))?,
                                };

                                docids_version_offsets.insert(
                                    external_document_id,
                                    (docid, vec![document_operation]),
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
                        match docids_version_offsets.get_mut(external_document_id.as_str()) {
                            None => {
                                let docid = match index
                                    .external_documents_ids()
                                    .get(rtxn, &external_document_id)?
                                {
                                    Some(docid) => docid,
                                    None => available_docids
                                        .next()
                                        .ok_or(Error::UserError(UserError::DocumentLimitReached))?,
                                };

                                docids_version_offsets.insert(
                                    CowStr(external_document_id.into()),
                                    (docid, vec![InnerDocOp::Deletion]),
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

        /// TODO is it the best way to provide FieldsIdsMap to the parallel iterator?
        let fields_ids_map = fields_ids_map.clone();
        // TODO We must drain the HashMap into a Vec because rayon::hash_map::IntoIter: !Clone
        let mut docids_version_offsets: Vec<_> = docids_version_offsets.drain().collect();
        // Reorder the offsets to make sure we iterate on the file sequentially
        let sort_function_key = match self.index_documents_method {
            Idm::ReplaceDocuments => MergeDocumentForReplacement::sort_key,
            Idm::UpdateDocuments => MergeDocumentForUpdates::sort_key,
        };

        // And finally sort them
        docids_version_offsets.sort_unstable_by_key(|(_, (_, docops))| sort_function_key(docops));

        Ok(docids_version_offsets.into_par_iter().map_with(
            Arc::new(ItemsPool::new(|| index.read_txn().map_err(crate::Error::from))),
            move |context_pool, (external_docid, (internal_docid, operations))| {
                context_pool.with(|rtxn| {
                    let document_merge_function = match self.index_documents_method {
                        Idm::ReplaceDocuments => MergeDocumentForReplacement::merge,
                        Idm::UpdateDocuments => MergeDocumentForUpdates::merge,
                    };

                    document_merge_function(
                        rtxn,
                        index,
                        &fields_ids_map,
                        internal_docid,
                        external_docid.to_string(), // TODO do not clone
                        &operations,
                    )
                })
            },
        ))
    }
}

trait MergeChanges {
    /// Wether the payloads in the list of operations are useless or not.
    const USELESS_PREVIOUS_CHANGES: bool;

    /// Returns a key that is used to order the payloads the right way.
    fn sort_key(docops: &[InnerDocOp]) -> usize;

    fn merge(
        rtxn: &RoTxn,
        index: &Index,
        fields_ids_map: &FieldsIdsMap,
        docid: DocumentId,
        external_docid: String,
        operations: &[InnerDocOp],
    ) -> Result<DocumentChange>;
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
    fn merge(
        rtxn: &RoTxn,
        index: &Index,
        fields_ids_map: &FieldsIdsMap,
        docid: DocumentId,
        external_docid: String,
        operations: &[InnerDocOp],
    ) -> Result<DocumentChange> {
        let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
        let current: Option<&KvReaderFieldId> = current.map(Into::into);

        match operations.last() {
            Some(InnerDocOp::Addition(DocumentOffset { content })) => {
                let map: TopLevelMap = serde_json::from_slice(content).unwrap();
                let mut document_entries = Vec::new();
                for (key, v) in map.0 {
                    let id = fields_ids_map.id(key.as_ref()).unwrap();
                    document_entries.push((id, v));
                }

                document_entries.sort_unstable_by_key(|(id, _)| *id);

                let mut writer = KvWriterFieldId::memory();
                document_entries
                    .into_iter()
                    .for_each(|(id, value)| writer.insert(id, value.get()).unwrap());
                let new = writer.into_boxed();

                match current {
                    Some(current) => {
                        let update = Update::create(docid, current.boxed(), new);
                        Ok(DocumentChange::Update(update))
                    }
                    None => Ok(DocumentChange::Insertion(Insertion::create(docid, new))),
                }
            }
            Some(InnerDocOp::Deletion) => {
                let deletion = match current {
                    Some(current) => Deletion::create(docid, current.boxed()),
                    None => todo!("Do that with Louis"),
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
    fn merge(
        rtxn: &RoTxn,
        index: &Index,
        fields_ids_map: &FieldsIdsMap,
        docid: DocumentId,
        external_docid: String,
        operations: &[InnerDocOp],
    ) -> Result<DocumentChange> {
        let mut document = BTreeMap::<_, Cow<_>>::new();
        let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
        let current: Option<&KvReaderFieldId> = current.map(Into::into);

        if operations.is_empty() {
            unreachable!("We must not have empty set of operations on a document");
        }

        let last_deletion = operations.iter().rposition(|op| matches!(op, InnerDocOp::Deletion));
        let operations = &operations[last_deletion.map_or(0, |i| i + 1)..];

        // If there was a deletion we must not start
        // from the original document but from scratch.
        if last_deletion.is_none() {
            if let Some(current) = current {
                current.into_iter().for_each(|(k, v)| {
                    document.insert(k, v.into());
                });
            }
        }

        if operations.is_empty() {
            let deletion = match current {
                Some(current) => Deletion::create(docid, current.boxed()),
                None => todo!("Do that with Louis"),
            };
            return Ok(DocumentChange::Deletion(deletion));
        }

        for operation in operations {
            let DocumentOffset { content } = match operation {
                InnerDocOp::Addition(offset) => offset,
                InnerDocOp::Deletion => {
                    unreachable!("Deletion in document operations")
                }
            };

            let map: TopLevelMap = serde_json::from_slice(content).unwrap();
            for (key, v) in map.0 {
                let id = fields_ids_map.id(key.as_ref()).unwrap();
                document.insert(id, v.get().as_bytes().to_vec().into());
            }
        }

        let mut writer = KvWriterFieldId::memory();
        document.into_iter().for_each(|(id, value)| writer.insert(id, value).unwrap());
        let new = writer.into_boxed();

        match current {
            Some(current) => {
                let update = Update::create(docid, current.boxed(), new);
                Ok(DocumentChange::Update(update))
            }
            None => {
                let insertion = Insertion::create(docid, new);
                Ok(DocumentChange::Insertion(insertion))
            }
        }
    }
}
