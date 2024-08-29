mod document_change;
// mod extract;
mod items_pool;

mod global_fields_ids_map;

mod indexer {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};
    use std::fs::File;
    use std::io::Cursor;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;

    use heed::types::Bytes;
    use heed::RoTxn;
    use memmap2::Mmap;
    use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
    use roaring::RoaringBitmap;
    use serde_json::Value;

    use super::document_change::{self, DocumentChange, Insertion, Update};
    use super::items_pool::ItemsPool;
    use crate::documents::{
        obkv_to_object, DocumentIdExtractionError, DocumentsBatchReader, PrimaryKey,
    };
    use crate::update::{AvailableDocumentsIds, IndexDocumentsMethod};
    use crate::{
        DocumentId, Error, FieldId, FieldsIdsMap, Index, InternalError, Result, UserError,
    };

    pub type KvReaderFieldId = obkv2::KvReader<FieldId>;
    pub type KvWriterFieldId<W> = obkv2::KvWriter<W, FieldId>;

    pub struct DocumentOperationIndexer {
        operations: Vec<Payload>,
        method: IndexDocumentsMethod,
    }

    enum Payload {
        Addition(File),
        Deletion(Vec<String>),
    }

    pub struct PayloadStats {
        pub document_count: usize,
        pub bytes: u64,
    }

    enum DocumentOperation {
        Addition(DocumentOffset),
        Deletion,
    }

    /// Represents an offset where a document lives
    /// in an mmapped grenad reader file.
    struct DocumentOffset {
        /// The mmapped grenad reader file.
        pub content: Arc<Mmap>, // grenad::Reader
        /// The offset of the document in the file.
        pub offset: u32,
    }

    impl DocumentOperationIndexer {
        pub fn new(method: IndexDocumentsMethod) -> Self {
            Self { operations: Default::default(), method }
        }

        /// TODO please give me a type
        /// The payload is expected to be in the grenad format
        pub fn add_documents(&mut self, payload: File) -> Result<PayloadStats> {
            let reader = DocumentsBatchReader::from_reader(&payload)?;
            let bytes = payload.metadata()?.size();
            let document_count = reader.documents_count() as usize;

            self.operations.push(Payload::Addition(payload));

            Ok(PayloadStats { bytes, document_count })
        }

        pub fn delete_documents(&mut self, to_delete: Vec<String>) {
            self.operations.push(Payload::Deletion(to_delete))
        }

        pub fn document_changes<'a>(
            self,
            index: &'a Index,
            rtxn: &'a RoTxn,
            mut fields_ids_map: FieldsIdsMap,
            primary_key: &'a PrimaryKey<'a>,
        ) -> Result<impl ParallelIterator<Item = document_change::DocumentChange> + 'a> {
            let documents_ids = index.documents_ids(rtxn)?;
            let mut available_docids = AvailableDocumentsIds::from_documents_ids(&documents_ids);
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
                                match primary_key.document_id(&document, &batch_index)? {
                                    Ok(document_id) => Ok(document_id),
                                    Err(DocumentIdExtractionError::InvalidDocumentId(
                                        user_error,
                                    )) => Err(user_error),
                                    Err(DocumentIdExtractionError::MissingDocumentId) => {
                                        Err(UserError::MissingDocumentId {
                                            primary_key: primary_key.name().to_string(),
                                            document: obkv_to_object(&document, &batch_index)?,
                                        })
                                    }
                                    Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                                        Err(UserError::TooManyDocumentIds {
                                            primary_key: primary_key.name().to_string(),
                                            document: obkv_to_object(&document, &batch_index)?,
                                        })
                                    }
                                }?;

                            let content = content.clone();
                            let document_offset = DocumentOffset { content, offset };
                            let document_operation = DocumentOperation::Addition(document_offset);

                            match docids_version_offsets.get_mut(&external_document_id) {
                                None => {
                                    let docid = match index
                                        .external_documents_ids()
                                        .get(rtxn, &external_document_id)?
                                    {
                                        Some(docid) => docid,
                                        None => available_docids.next().ok_or(Error::UserError(
                                            UserError::DocumentLimitReached,
                                        ))?,
                                    };

                                    docids_version_offsets.insert(
                                        external_document_id.into(),
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
                                        None => available_docids.next().ok_or(Error::UserError(
                                            UserError::DocumentLimitReached,
                                        ))?,
                                    };

                                    docids_version_offsets.insert(
                                        external_document_id,
                                        (docid, vec![DocumentOperation::Deletion]),
                                    );
                                }
                                Some((_, offsets)) => offsets.push(DocumentOperation::Deletion),
                            }
                        }
                    }
                }
            }

            let items = Arc::new(ItemsPool::new(|| index.read_txn().map_err(crate::Error::from)));
            docids_version_offsets.into_par_iter().map_with(
                items,
                |context_pool, (external_docid, (internal_docid, operations))| {
                    context_pool.with(|rtxn| match self.method {
                        IndexDocumentsMethod::ReplaceDocuments => todo!(),
                        // TODO Remap the documents to match the db fields_ids_map
                        IndexDocumentsMethod::UpdateDocuments => merge_document_obkv_for_updates(
                            rtxn,
                            index,
                            &fields_ids_map,
                            internal_docid,
                            external_docid,
                            &operations,
                        ),
                    })
                },
            );

            Ok(vec![].into_par_iter())

            // let mut file_count: usize = 0;
            // for result in WalkDir::new(update_files_path)
            //     // TODO handle errors
            //     .sort_by_key(|entry| entry.metadata().unwrap().created().unwrap())
            // {
            //     let entry = result?;
            //     if !entry.file_type().is_file() {
            //         continue;
            //     }

            //     let file = File::open(entry.path())
            //         .with_context(|| format!("While opening {}", entry.path().display()))?;
            //     let content = unsafe {
            //         Mmap::map(&file)
            //             .map(Arc::new)
            //             .with_context(|| format!("While memory mapping {}", entry.path().display()))?
            //     };

            //     let reader =
            //         crate::documents::DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
            //     let (mut batch_cursor, batch_index) = reader.into_cursor_and_fields_index();
            //     batch_index.iter().for_each(|(_, name)| {
            //         fields_ids_map.insert(name);
            //     });
            //     let mut offset: u32 = 0;
            //     while let Some(document) = batch_cursor.next_document()? {
            //         let primary_key = batch_index.id(primary_key).unwrap();
            //         let document_id = document.get(primary_key).unwrap();
            //         let document_id = std::str::from_utf8(document_id).unwrap();

            //         let document_offset = DocumentOffset { content: content.clone(), offset };
            //         match docids_version_offsets.get_mut(document_id) {
            //             None => {
            //                 let docid = match maindb.external_documents_ids.get(rtxn, document_id)? {
            //                     Some(docid) => docid,
            //                     None => sequential_docids.next().context("no more available docids")?,
            //                 };
            //                 docids_version_offsets
            //                     .insert(document_id.into(), (docid, smallvec![document_offset]));
            //             }
            //             Some((_, offsets)) => offsets.push(document_offset),
            //         }
            //         offset += 1;
            //         p.inc(1);
            //     }

            //     file_count += 1;
            // }
        }
    }

    pub struct DeleteDocumentIndexer {
        to_delete: RoaringBitmap,
    }

    impl DeleteDocumentIndexer {
        pub fn new() -> Self {
            Self { to_delete: Default::default() }
        }

        pub fn delete_documents_by_docids(&mut self, docids: RoaringBitmap) {
            self.to_delete |= docids;
        }

        // let fields = index.fields_ids_map(rtxn)?;
        // let primary_key =
        //     index.primary_key(rtxn)?.ok_or(InternalError::DatabaseMissingEntry {
        //         db_name: db_name::MAIN,
        //         key: Some(main_key::PRIMARY_KEY_KEY),
        //     })?;
        // let primary_key = PrimaryKey::new(primary_key, &fields).ok_or_else(|| {
        //     InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldName {
        //         field_name: primary_key.to_owned(),
        //         process: "external_id_of",
        //     })
        // })?;
        pub fn document_changes<'a, F>(
            self,
            index: &'a Index,
            fields: &'a FieldsIdsMap,
            primary_key: &'a PrimaryKey<'a>,
        ) -> Result<impl ParallelIterator<Item = Result<document_change::DocumentChange>> + 'a>
        {
            let items = Arc::new(ItemsPool::new(|| index.read_txn().map_err(crate::Error::from)));
            Ok(self.to_delete.into_iter().par_bridge().map_with(items, |items, docid| {
                items.with(|rtxn| {
                    let document = index.document(rtxn, docid)?;
                    let external_docid = match primary_key.document_id(&document, fields)? {
                        Ok(document_id) => Ok(document_id) as Result<_>,
                        Err(_) => Err(InternalError::DocumentsError(
                            crate::documents::Error::InvalidDocumentFormat,
                        )
                        .into()),
                    }?;

                    /// TODO create a function for this
                    let document = document.as_bytes().to_vec().into_boxed_slice().into();
                    Ok(DocumentChange::Deletion(document_change::Deletion::create(
                        docid,
                        external_docid,
                        document,
                    )))
                })
            }))
        }
    }

    pub struct DumpIndexer;

    impl DumpIndexer {
        pub fn new() -> Self {
            todo!()
        }

        pub fn document_changes_from_json_iter<I>(
            self,
            iter: I,
            index: &Index,
        ) -> impl ParallelIterator<Item = document_change::DocumentChange>
        where
            I: IntoIterator<Item = Value>,
        {
            // let items = Arc::new(ItemsPool::new(|| {
            //     let rtxn = index.read_txn()?;
            //     let fields = index.fields_ids_map(&rtxn)?;
            //     let primary_key =
            //         index.primary_key(&rtxn)?.ok_or(InternalError::DatabaseMissingEntry {
            //             db_name: db_name::MAIN,
            //             key: Some(main_key::PRIMARY_KEY_KEY),
            //         })?;
            //     let primary_key = PrimaryKey::new(primary_key, &fields).ok_or_else(|| {
            //         InternalError::FieldIdMapMissingEntry(
            //             crate::FieldIdMapMissingEntry::FieldName {
            //                 field_name: primary_key.to_owned(),
            //                 process: "external_id_of",
            //             },
            //         )
            //     })?;
            //     Ok(DeleteDocumentExternalDocumentIdGetter { rtxn, fields, primary_key })
            //         as crate::Result<_>
            // }));

            todo!();
            vec![].into_par_iter()
        }
    }

    pub struct UpdateByFunctionIndexer;
    // DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?

    /// Reads the previous version of a document from the database, the new versions
    /// in the grenad update files and merges them to generate a new boxed obkv.
    ///
    /// This function is only meant to be used when doing an update and not a replacement.
    pub fn merge_document_obkv_for_updates(
        rtxn: &RoTxn,
        // Let's construct the new obkv in memory
        index: &Index,
        fields_ids_map: &FieldsIdsMap,
        docid: DocumentId,
        external_docid: String,
        operations: &[DocumentOperation],
    ) -> Result<Option<DocumentChange>> {
        let mut document = BTreeMap::<_, Cow<_>>::new();
        let original = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
        let original: Option<&KvReaderFieldId> = original.map(Into::into);

        if let Some(original) = original {
            original.into_iter().for_each(|(k, v)| {
                document.insert(k, v.into());
            });
        }

        let last_deletion = operations
            .iter()
            .rposition(|operation| matches!(operation, DocumentOperation::Deletion));

        let operations = &operations[last_deletion.map_or(0, |i| i + 1)..];

        if operations.is_empty() {
            match original {
                Some(original_obkv) => {
                    let current = original_obkv.as_bytes().to_vec().into_boxed_slice().into();
                    return Ok(Some(DocumentChange::Deletion(document_change::Deletion::create(
                        docid,
                        external_docid,
                        current,
                    ))));
                }
                None => return Ok(None),
            }
        }

        for operation in operations {
            let DocumentOffset { content, offset } = match operation {
                DocumentOperation::Addition(offset) => offset,
                DocumentOperation::Deletion => unreachable!("Deletion in document operations"),
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
        /// TODO create a function for this conversion
        let new = writer.into_inner().unwrap().into_boxed_slice().into();

        match original {
            Some(original) => {
                /// TODO create a function for this conversion
                let current = original.as_bytes().to_vec().into_boxed_slice().into();
                let update = Update::create(docid, external_docid, current, new);
                Ok(Some(DocumentChange::Update(update)))
            }
            None => {
                let insertion = Insertion::create(docid, external_docid, new);
                Ok(Some(DocumentChange::Insertion(insertion)))
            }
        }
    }
}
