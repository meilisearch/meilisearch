use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use heed::types::Bytes;
use heed::RoTxn;
use memmap2::Mmap;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use IndexDocumentsMethod as Idm;

use super::super::document_change::DocumentChange;
use super::super::items_pool::ItemsPool;
use super::DocumentChanges;
use crate::documents::PrimaryKey;
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
        let document_count = memchr::Memchr::new(b'\n', &payload[..]).count();
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
    ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + Clone + 'p> {
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
                            match get_docid(&document, &[primary_key.name()]).unwrap() {
                                Some(document_id) => document_id,
                                None => {
                                    return Err(UserError::MissingDocumentId {
                                        primary_key: primary_key.name().to_string(),
                                        document: todo!(),
                                        // document: obkv_to_object(document, &batch_index)?,
                                    }
                                    .into());
                                }
                            };

                        // let external_document_id =
                        //     match primary_key.document_id(document, &batch_index)? {
                        //         Ok(document_id) => Ok(document_id),
                        //         Err(DocumentIdExtractionError::InvalidDocumentId(user_error)) => {
                        //             Err(user_error)
                        //         }
                        //         Err(DocumentIdExtractionError::MissingDocumentId) => {
                        //             Err(UserError::MissingDocumentId {
                        //                 primary_key: primary_key.name().to_string(),
                        //                 document: obkv_to_object(document, &batch_index)?,
                        //             })
                        //         }
                        //         Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                        //             Err(UserError::TooManyDocumentIds {
                        //                 primary_key: primary_key.name().to_string(),
                        //                 document: obkv_to_object(document, &batch_index)?,
                        //             })
                        //         }
                        //     }?;

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
                            // TODO clean the code to make sure we clean the useless operations
                            //      add a method to the MergeChanges trait
                            Some((_, offsets)) => offsets.push(document_operation),
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
                            Some((_, offsets)) => offsets.push(InnerDocOp::Deletion),
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
        match self.index_documents_method {
            Idm::ReplaceDocuments => MergeDocumentForReplacement::sort(&mut docids_version_offsets),
            Idm::UpdateDocuments => MergeDocumentForUpdates::sort(&mut docids_version_offsets),
        }

        Ok(docids_version_offsets
            .into_par_iter()
            .map_with(
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
            )
            .filter_map(Result::transpose))
    }
}

trait MergeChanges {
    /// Reorders the offsets to make sure we iterate on the file sequentially.
    fn sort(changes_offsets: &mut [(CowStr, (DocumentId, Vec<InnerDocOp>))]);

    fn merge(
        rtxn: &RoTxn,
        index: &Index,
        fields_ids_map: &FieldsIdsMap,
        docid: DocumentId,
        external_docid: String,
        operations: &[InnerDocOp],
    ) -> Result<Option<DocumentChange>>;
}

struct MergeDocumentForReplacement;

impl MergeChanges for MergeDocumentForReplacement {
    /// Reorders to read only the last change.
    fn sort(changes_offsets: &mut [(CowStr, (DocumentId, Vec<InnerDocOp>))]) {
        changes_offsets.sort_unstable_by_key(|(_, (_, offsets))| {
            offsets
                .iter()
                .rev()
                .find_map(|ido| match ido {
                    InnerDocOp::Addition(add) => Some(add.content.as_ptr() as usize),
                    InnerDocOp::Deletion => None,
                })
                .unwrap_or(0)
        });
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
    ) -> Result<Option<DocumentChange>> {
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
                        let update = Update::create(docid, external_docid, current.boxed(), new);
                        Ok(Some(DocumentChange::Update(update)))
                    }
                    None => {
                        let insertion = Insertion::create(docid, external_docid, new);
                        Ok(Some(DocumentChange::Insertion(insertion)))
                    }
                }
            }
            Some(InnerDocOp::Deletion) => match current {
                Some(current) => {
                    let deletion = Deletion::create(docid, external_docid, current.boxed());
                    Ok(Some(DocumentChange::Deletion(deletion)))
                }
                None => Ok(None),
            },
            None => Ok(None), // but it's strange
        }
    }
}

struct MergeDocumentForUpdates;

impl MergeChanges for MergeDocumentForUpdates {
    /// Reorders to read the first changes first so that it's faster to read the first one and then the rest.
    fn sort(changes_offsets: &mut [(CowStr, (DocumentId, Vec<InnerDocOp>))]) {
        changes_offsets.sort_unstable_by_key(|(_, (_, offsets))| {
            offsets
                .iter()
                .find_map(|ido| match ido {
                    InnerDocOp::Addition(add) => Some(add.content.as_ptr() as usize),
                    InnerDocOp::Deletion => None,
                })
                .unwrap_or(0)
        });
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
    ) -> Result<Option<DocumentChange>> {
        let mut document = BTreeMap::<_, Cow<_>>::new();
        let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
        let current: Option<&KvReaderFieldId> = current.map(Into::into);

        if operations.is_empty() {
            return Ok(None); // but it's strange
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
            match current {
                Some(current) => {
                    let deletion = Deletion::create(docid, external_docid, current.boxed());
                    return Ok(Some(DocumentChange::Deletion(deletion)));
                }
                None => return Ok(None),
            }
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
                let update = Update::create(docid, external_docid, current.boxed(), new);
                Ok(Some(DocumentChange::Update(update)))
            }
            None => {
                let insertion = Insertion::create(docid, external_docid, new);
                Ok(Some(DocumentChange::Insertion(insertion)))
            }
        }
    }
}

use std::borrow::Borrow;

use serde::Deserialize;
use serde_json::from_str;
use serde_json::value::RawValue;

#[derive(Deserialize)]
pub struct TopLevelMap<'p>(#[serde(borrow)] BTreeMap<CowStr<'p>, &'p RawValue>);

#[derive(Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CowStr<'p>(#[serde(borrow)] Cow<'p, str>);

impl CowStr<'_> {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl AsRef<str> for CowStr<'_> {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'doc> Borrow<str> for CowStr<'doc> {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

fn get_docid<'p>(
    map: &TopLevelMap<'p>,
    primary_key: &[&str],
) -> serde_json::Result<Option<CowStr<'p>>> {
    match primary_key {
        [] => unreachable!("arrrgh"), // would None be ok?
        [primary_key] => match map.0.get(*primary_key) {
            Some(value) => match from_str::<u64>(value.get()) {
                Ok(value) => Ok(Some(CowStr(Cow::Owned(value.to_string())))),
                Err(_) => Ok(Some(from_str(value.get())?)),
            },
            None => Ok(None),
        },
        [head, tail @ ..] => match map.0.get(*head) {
            Some(value) => get_docid(&from_str(value.get())?, tail),
            None => Ok(None),
        },
    }
}
