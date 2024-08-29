mod document_change;
// mod extract;
mod channel;
mod items_pool;
mod merge;

/// TODO remove this
// mod global_fields_ids_map;

pub type StdResult<T, E> = std::result::Result<T, E>;

mod indexer {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};
    use std::fs::File;
    use std::io::Cursor;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use std::thread;

    use big_s::S;
    use heed::types::Bytes;
    use heed::{RoTxn, RwTxn};
    use memmap2::Mmap;
    use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
    use roaring::RoaringBitmap;
    use serde_json::Value;

    use super::channel::{
        extractors_merger_channels, merger_writer_channels, EntryOperation,
        ExtractorsMergerChannels, MergerReceiver, MergerSender, WriterOperation,
    };
    use super::document_change::{Deletion, DocumentChange, Insertion, Update};
    use super::items_pool::ItemsPool;
    use super::merge;
    use crate::documents::{
        obkv_to_object, DocumentIdExtractionError, DocumentsBatchReader, PrimaryKey,
    };
    use crate::update::del_add::DelAdd;
    use crate::update::new::channel::MergerOperation;
    use crate::update::{AvailableDocumentsIds, IndexDocumentsMethod};
    use crate::{
        CboRoaringBitmapCodec, DocumentId, Error, FieldId, FieldsIdsMap, Index, InternalError,
        Result, UserError,
    };

    pub type KvReaderFieldId = obkv::KvReader<FieldId>;
    pub type KvReaderDelAdd = obkv::KvReader<DelAdd>;
    pub type KvWriterFieldId<W> = obkv::KvWriter<W, FieldId>;
    pub type KvWriterDelAdd<W> = obkv::KvWriter<W, DelAdd>;

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
        ) -> Result<impl ParallelIterator<Item = Result<Option<DocumentChange>>> + 'a> {
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
            Ok(docids_version_offsets.into_par_iter().map_with(
                items,
                |context_pool, (external_docid, (internal_docid, operations))| {
                    context_pool.with(|rtxn| match self.method {
                        IndexDocumentsMethod::ReplaceDocuments => merge_document_for_replacements(
                            rtxn,
                            index,
                            &fields_ids_map,
                            internal_docid,
                            external_docid,
                            &operations,
                        ),
                        // TODO Remap the documents to match the db fields_ids_map
                        IndexDocumentsMethod::UpdateDocuments => merge_document_for_updates(
                            rtxn,
                            index,
                            &fields_ids_map,
                            internal_docid,
                            external_docid,
                            &operations,
                        ),
                    })
                },
            ))
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
        ) -> Result<impl ParallelIterator<Item = Result<DocumentChange>> + 'a> {
            let items = Arc::new(ItemsPool::new(|| index.read_txn().map_err(crate::Error::from)));
            Ok(self.to_delete.into_iter().par_bridge().map_with(items, |items, docid| {
                items.with(|rtxn| {
                    let current = index.document(rtxn, docid)?;
                    let external_docid = match primary_key.document_id(&current, fields)? {
                        Ok(document_id) => Ok(document_id) as Result<_>,
                        Err(_) => Err(InternalError::DocumentsError(
                            crate::documents::Error::InvalidDocumentFormat,
                        )
                        .into()),
                    }?;

                    /// TODO create a function for this
                    let current = current.as_bytes().to_vec().into_boxed_slice().into();
                    Ok(DocumentChange::Deletion(Deletion::create(docid, external_docid, current)))
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
        ) -> impl ParallelIterator<Item = DocumentChange>
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

    /// TODO return stats
    /// TODO take the rayon ThreadPool
    pub fn index<PI>(wtxn: &mut RwTxn, index: &Index, document_changes: PI) -> Result<()>
    where
        PI: IntoParallelIterator<Item = Result<DocumentChange>> + Send,
        PI::Iter: Clone,
    {
        let (merger_sender, writer_receiver) = merger_writer_channels(100);
        let ExtractorsMergerChannels { merger_receiver, deladd_cbo_roaring_bitmap_sender } =
            extractors_merger_channels(100);

        thread::scope(|s| {
            thread::Builder::new().name(S("indexer-extractors")).spawn_scoped(s, || {
                document_changes.into_par_iter().for_each(|_dc| ());
            });

            // TODO manage the errors correctly
            thread::Builder::new().name(S("indexer-merger")).spawn_scoped(s, || {
                let rtxn = index.read_txn().unwrap();
                merge_grenad_entries(merger_receiver, merger_sender, &rtxn, index).unwrap()
            });

            // TODO Split this code into another function
            for operation in writer_receiver {
                let database = operation.database(index);
                match operation {
                    WriterOperation::WordDocids(operation) => match operation {
                        EntryOperation::Delete(e) => database.delete(wtxn, e.entry()).map(drop)?,
                        EntryOperation::Write(e) => database.put(wtxn, e.key(), e.value())?,
                    },
                    WriterOperation::Document(e) => database.put(wtxn, &e.key(), e.content())?,
                }
            }

            Ok(())
        })
    }

    enum Operation {
        Write(RoaringBitmap),
        Delete,
        Ignore,
    }

    /// A function that merges the DelAdd CboRoaringBitmaps with the current bitmap.
    fn merge_cbo_bitmaps(
        current: Option<&[u8]>,
        del: Option<&[u8]>,
        add: Option<&[u8]>,
    ) -> Result<Operation> {
        let bitmap = match current {
            Some(current_bitmap_bytes) => {
                let bitmap_without_del = match del {
                    Some(del_bytes) => {
                        let del_bitmap = CboRoaringBitmapCodec::deserialize_from(del_bytes)?;
                        CboRoaringBitmapCodec::intersection_with_serialized(
                            current_bitmap_bytes,
                            &del_bitmap,
                        )?
                    }
                    None => CboRoaringBitmapCodec::deserialize_from(current_bitmap_bytes)?,
                };

                match add {
                    Some(add_bytes) => {
                        let add = CboRoaringBitmapCodec::deserialize_from(add_bytes)?;
                        bitmap_without_del | add
                    }
                    None => bitmap_without_del,
                }
            }
            None => match add {
                Some(add_bytes) => CboRoaringBitmapCodec::deserialize_from(add_bytes)?,
                None => return Ok(Operation::Ignore),
            },
        };

        if bitmap.is_empty() {
            Ok(Operation::Delete)
        } else {
            Ok(Operation::Write(bitmap))
        }
    }

    /// Return the slice directly from the serialize_into method
    fn cbo_serialize_into_vec<'b>(bitmap: &RoaringBitmap, buffer: &'b mut Vec<u8>) -> &'b [u8] {
        buffer.clear();
        CboRoaringBitmapCodec::serialize_into(bitmap, buffer);
        buffer.as_slice()
    }

    /// TODO We must return some infos/stats
    fn merge_grenad_entries(
        receiver: MergerReceiver,
        sender: MergerSender,
        rtxn: &RoTxn,
        index: &Index,
    ) -> Result<()> {
        let mut buffer = Vec::new();

        for merger_operation in receiver {
            match merger_operation {
                MergerOperation::WordDocidsCursors(cursors) => {
                    let sender = sender.word_docids();
                    let database = index.word_docids.remap_types::<Bytes, Bytes>();

                    let mut builder = grenad2::MergerBuilder::new(merge::DelAddRoaringBitmapMerger);
                    builder.extend(cursors);
                    /// TODO manage the error correctly
                    let mut merger_iter = builder.build().into_stream_merger_iter().unwrap();

                    // TODO manage the error correctly
                    while let Some((key, deladd)) = merger_iter.next().unwrap() {
                        let current = database.get(rtxn, key)?;
                        let deladd: &KvReaderDelAdd = deladd.into();
                        let del = deladd.get(DelAdd::Deletion);
                        let add = deladd.get(DelAdd::Addition);

                        match merge_cbo_bitmaps(current, del, add)? {
                            Operation::Write(bitmap) => {
                                let value = cbo_serialize_into_vec(&bitmap, &mut buffer);
                                sender.write(key, value).unwrap();
                            }
                            Operation::Delete => sender.delete(key).unwrap(),
                            Operation::Ignore => (),
                        }
                    }
                }
            }
        }

        Ok(())
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
        operations: &[DocumentOperation],
    ) -> Result<Option<DocumentChange>> {
        let mut document = BTreeMap::<_, Cow<_>>::new();
        let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
        let current: Option<&KvReaderFieldId> = current.map(Into::into);

        if let Some(current) = current {
            current.into_iter().for_each(|(k, v)| {
                document.insert(k, v.into());
            });
        }

        let last_deletion = operations
            .iter()
            .rposition(|operation| matches!(operation, DocumentOperation::Deletion));

        let operations = &operations[last_deletion.map_or(0, |i| i + 1)..];

        if operations.is_empty() {
            match current {
                Some(current) => {
                    /// TODO create a function for this
                    let current = current.as_bytes().to_vec().into_boxed_slice().into();
                    let deletion = Deletion::create(docid, external_docid, current);
                    return Ok(Some(DocumentChange::Deletion(deletion)));
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

        match current {
            Some(current) => {
                /// TODO create a function for this conversion
                let current = current.as_bytes().to_vec().into_boxed_slice().into();
                let update = Update::create(docid, external_docid, current, new);
                Ok(Some(DocumentChange::Update(update)))
            }
            None => {
                let insertion = Insertion::create(docid, external_docid, new);
                Ok(Some(DocumentChange::Insertion(insertion)))
            }
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
        operations: &[DocumentOperation],
    ) -> Result<Option<DocumentChange>> {
        let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
        let current: Option<&KvReaderFieldId> = current.map(Into::into);

        match operations.last() {
            Some(DocumentOperation::Addition(DocumentOffset { content, offset })) => {
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
                document_entries
                    .into_iter()
                    .for_each(|(id, value)| writer.insert(id, value).unwrap());
                /// TODO create a function for this conversion
                let new = writer.into_inner().unwrap().into_boxed_slice().into();

                match current {
                    Some(current) => {
                        /// TODO create a function for this conversion
                        let current = current.as_bytes().to_vec().into_boxed_slice().into();
                        let update = Update::create(docid, external_docid, current, new);
                        Ok(Some(DocumentChange::Update(update)))
                    }
                    None => {
                        let insertion = Insertion::create(docid, external_docid, new);
                        Ok(Some(DocumentChange::Insertion(insertion)))
                    }
                }
            }
            Some(DocumentOperation::Deletion) => {
                match current {
                    Some(current) => {
                        /// TODO create a function for this conversion
                        let current = current.as_bytes().to_vec().into_boxed_slice().into();
                        let deletion = Deletion::create(docid, external_docid, current);
                        Ok(Some(DocumentChange::Deletion(deletion)))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }
}
