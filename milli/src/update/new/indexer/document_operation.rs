use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Cursor;
use std::sync::Arc;

use heed::types::Bytes;
use heed::RoTxn;
use memmap2::Mmap;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::super::document_change::DocumentChange;
use super::super::items_pool::ItemsPool;
use super::DocumentChanges;
use crate::documents::{
    obkv_to_object, DocumentIdExtractionError, DocumentsBatchReader, PrimaryKey,
};
use crate::update::new::{Deletion, Insertion, KvReaderFieldId, KvWriterFieldId, Update};
use crate::update::{AvailableIds, IndexDocumentsMethod};
use crate::{DocumentId, Error, FieldsIdsMap, Index, Result, UserError};

pub struct DocumentOperation {
    operations: Vec<Payload>,
    index_documents_method: IndexDocumentsMethod,
}

pub enum Payload {
    Addition(File),
    Deletion(Vec<String>),
}

pub struct PayloadStats {
    pub document_count: usize,
    pub bytes: u64,
}

#[derive(Clone)]
enum InnerDocOp {
    Addition(DocumentOffset),
    Deletion,
}

/// Represents an offset where a document lives
/// in an mmapped grenad reader file.
#[derive(Clone)]
pub struct DocumentOffset {
    /// The mmapped grenad reader file.
    pub content: Arc<Mmap>, // grenad::Reader
    /// The offset of the document in the file.
    pub offset: u32,
}

impl DocumentOperation {
    pub fn new(method: IndexDocumentsMethod) -> Self {
        Self { operations: Default::default(), index_documents_method: method }
    }

    /// TODO please give me a type
    /// The payload is expected to be in the grenad format
    pub fn add_documents(&mut self, payload: File) -> Result<PayloadStats> {
        let reader = DocumentsBatchReader::from_reader(&payload)?;
        let bytes = payload.metadata()?.len();
        let document_count = reader.documents_count() as usize;

        self.operations.push(Payload::Addition(payload));

        Ok(PayloadStats { bytes, document_count })
    }

    pub fn delete_documents(&mut self, to_delete: Vec<String>) {
        self.operations.push(Payload::Deletion(to_delete))
    }
}

impl<'p> DocumentChanges<'p> for DocumentOperation {
    type Parameter = (&'p Index, &'p RoTxn<'p>, &'p PrimaryKey<'p>);

    fn document_changes(
        self,
        fields_ids_map: &mut FieldsIdsMap,
        param: Self::Parameter,
    ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + Clone + 'p> {
        let (index, rtxn, primary_key) = param;

        let documents_ids = index.documents_ids(rtxn)?;
        let mut available_docids = AvailableIds::new(&documents_ids);
        let mut docids_version_offsets = HashMap::<String, _>::new();

        for operation in self.operations {
            match operation {
                Payload::Addition(payload) => {
                    let content = unsafe { Mmap::map(&payload).map(Arc::new)? };
                    let cursor = Cursor::new(content.as_ref());
                    let reader = DocumentsBatchReader::from_reader(cursor)?;

                    let (mut batch_cursor, batch_index) = reader.into_cursor_and_fields_index();
                    // TODO Fetch all document fields to fill the fields ids map
                    batch_index.iter().for_each(|(_, name)| {
                        fields_ids_map.insert(name);
                    });

                    let mut offset: u32 = 0;
                    while let Some(document) = batch_cursor.next_document()? {
                        let external_document_id =
                            match primary_key.document_id(document, &batch_index)? {
                                Ok(document_id) => Ok(document_id),
                                Err(DocumentIdExtractionError::InvalidDocumentId(user_error)) => {
                                    Err(user_error)
                                }
                                Err(DocumentIdExtractionError::MissingDocumentId) => {
                                    Err(UserError::MissingDocumentId {
                                        primary_key: primary_key.name().to_string(),
                                        document: obkv_to_object(document, &batch_index)?,
                                    })
                                }
                                Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                                    Err(UserError::TooManyDocumentIds {
                                        primary_key: primary_key.name().to_string(),
                                        document: obkv_to_object(document, &batch_index)?,
                                    })
                                }
                            }?;

                        let content = content.clone();
                        let document_offset = DocumentOffset { content, offset };
                        let document_operation = InnerDocOp::Addition(document_offset);

                        match docids_version_offsets.get_mut(&external_document_id) {
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
                            Some((_, offsets)) => offsets.push(document_operation),
                        }
                        offset += 1;
                    }
                }
                Payload::Deletion(to_delete) => {
                    for external_document_id in to_delete {
                        match docids_version_offsets.get_mut(&external_document_id) {
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
        let docids_version_offsets: Vec<_> = docids_version_offsets.drain().collect();

        Ok(docids_version_offsets
            .into_par_iter()
            .map_with(
                Arc::new(ItemsPool::new(|| index.read_txn().map_err(crate::Error::from))),
                move |context_pool, (external_docid, (internal_docid, operations))| {
                    context_pool.with(|rtxn| {
                        use IndexDocumentsMethod as Idm;

                        let document_merge_function = match self.index_documents_method {
                            Idm::ReplaceDocuments => merge_document_for_replacements,
                            Idm::UpdateDocuments => merge_document_for_updates,
                        };

                        document_merge_function(
                            rtxn,
                            index,
                            &fields_ids_map,
                            internal_docid,
                            external_docid,
                            &operations,
                        )
                    })
                },
            )
            .filter_map(Result::transpose))
    }
}

/// Returns only the most recent version of a document based on the updates from the payloads.
///
/// This function is only meant to be used when doing a replacement and not an update.
fn merge_document_for_replacements(
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
        Some(InnerDocOp::Addition(DocumentOffset { content, offset })) => {
            let reader = DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
            let (mut cursor, batch_index) = reader.into_cursor_and_fields_index();
            let update = cursor.get(*offset)?.expect("must exists");

            let mut document_entries = Vec::new();
            update.into_iter().for_each(|(k, v)| {
                let field_name = batch_index.name(k).unwrap();
                let id = fields_ids_map.id(field_name).unwrap();
                document_entries.push((id, v));
            });

            document_entries.sort_unstable_by_key(|(id, _)| *id);

            let mut writer = KvWriterFieldId::memory();
            document_entries.into_iter().for_each(|(id, value)| writer.insert(id, value).unwrap());
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

/// Reads the previous version of a document from the database, the new versions
/// in the grenad update files and merges them to generate a new boxed obkv.
///
/// This function is only meant to be used when doing an update and not a replacement.
fn merge_document_for_updates(
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
        let DocumentOffset { content, offset } = match operation {
            InnerDocOp::Addition(offset) => offset,
            InnerDocOp::Deletion => {
                unreachable!("Deletion in document operations")
            }
        };

        let reader = DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
        let (mut cursor, batch_index) = reader.into_cursor_and_fields_index();
        let update = cursor.get(*offset)?.expect("must exists");

        update.into_iter().for_each(|(k, v)| {
            let field_name = batch_index.name(k).unwrap();
            let id = fields_ids_map.id(field_name).unwrap();
            document.insert(id, v.to_vec().into());
        });
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
