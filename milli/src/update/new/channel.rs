use std::fs::File;
use std::marker::PhantomData;

use crossbeam_channel::{IntoIter, Receiver, SendError, Sender};
use grenad::Merger;
use heed::types::Bytes;
use memmap2::Mmap;
use roaring::RoaringBitmap;

use super::extract::FacetKind;
use super::StdResult;
use crate::index::main_key::{DOCUMENTS_IDS_KEY, WORDS_FST_KEY, WORDS_PREFIXES_FST_KEY};
use crate::update::new::KvReaderFieldId;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{DocumentId, Index};

/// The capacity of the channel is currently in number of messages.
pub fn merger_writer_channel(cap: usize) -> (MergerSender, WriterReceiver) {
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (
        MergerSender {
            sender,
            send_count: Default::default(),
            writer_contentious_count: Default::default(),
            merger_contentious_count: Default::default(),
        },
        WriterReceiver(receiver),
    )
}

/// The capacity of the channel is currently in number of messages.
pub fn extractors_merger_channels(cap: usize) -> (ExtractorSender, MergerReceiver) {
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (ExtractorSender(sender), MergerReceiver(receiver))
}

pub enum KeyValueEntry {
    SmallInMemory { key_length: usize, data: Box<[u8]> },
    LargeOnDisk { key: Box<[u8]>, value: Mmap },
}

impl KeyValueEntry {
    pub fn from_small_key_value(key: &[u8], value: &[u8]) -> Self {
        let mut data = Vec::with_capacity(key.len() + value.len());
        data.extend_from_slice(key);
        data.extend_from_slice(value);
        KeyValueEntry::SmallInMemory { key_length: key.len(), data: data.into_boxed_slice() }
    }

    pub fn from_small_key_bitmap(key: &[u8], bitmap: RoaringBitmap) -> Self {
        let mut data = Vec::with_capacity(key.len() + bitmap.serialized_size());
        data.extend_from_slice(key);
        bitmap.serialize_into(&mut data).unwrap();
        KeyValueEntry::SmallInMemory { key_length: key.len(), data: data.into_boxed_slice() }
    }

    pub fn from_large_key_value(key: &[u8], value: Mmap) -> Self {
        KeyValueEntry::LargeOnDisk { key: key.to_vec().into_boxed_slice(), value }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            KeyValueEntry::SmallInMemory { key_length, data } => &data.as_ref()[..*key_length],
            KeyValueEntry::LargeOnDisk { key, value: _ } => key.as_ref(),
        }
    }

    pub fn value(&self) -> &[u8] {
        match self {
            KeyValueEntry::SmallInMemory { key_length, data } => &data.as_ref()[*key_length..],
            KeyValueEntry::LargeOnDisk { key: _, value } => value.as_ref(),
        }
    }
}

pub struct KeyEntry {
    data: Box<[u8]>,
}

impl KeyEntry {
    pub fn from_key(key: &[u8]) -> Self {
        KeyEntry { data: key.to_vec().into_boxed_slice() }
    }

    pub fn entry(&self) -> &[u8] {
        self.data.as_ref()
    }
}

pub enum EntryOperation {
    Delete(KeyEntry),
    Write(KeyValueEntry),
}

pub struct DocumentEntry {
    docid: DocumentId,
    content: Box<[u8]>,
}

impl DocumentEntry {
    pub fn new_uncompressed(docid: DocumentId, content: Box<KvReaderFieldId>) -> Self {
        DocumentEntry { docid, content: content.into() }
    }

    pub fn new_compressed(docid: DocumentId, content: Box<[u8]>) -> Self {
        DocumentEntry { docid, content }
    }

    pub fn key(&self) -> [u8; 4] {
        self.docid.to_be_bytes()
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

pub struct DocumentDeletionEntry(DocumentId);

impl DocumentDeletionEntry {
    pub fn key(&self) -> [u8; 4] {
        self.0.to_be_bytes()
    }
}

pub struct WriterOperation {
    database: Database,
    entry: EntryOperation,
}

#[derive(Debug)]
pub enum Database {
    Documents,
    ExternalDocumentsIds,
    ExactWordDocids,
    FidWordCountDocids,
    Main,
    WordDocids,
    WordFidDocids,
    WordPairProximityDocids,
    WordPositionDocids,
    FacetIdIsNullDocids,
    FacetIdIsEmptyDocids,
    FacetIdExistsDocids,
    FacetIdF64NumberDocids,
    FacetIdStringDocids,
}

impl Database {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        match self {
            Database::Documents => index.documents.remap_types(),
            Database::ExternalDocumentsIds => index.external_documents_ids.remap_types(),
            Database::ExactWordDocids => index.exact_word_docids.remap_types(),
            Database::Main => index.main.remap_types(),
            Database::WordDocids => index.word_docids.remap_types(),
            Database::WordFidDocids => index.word_fid_docids.remap_types(),
            Database::WordPositionDocids => index.word_position_docids.remap_types(),
            Database::FidWordCountDocids => index.field_id_word_count_docids.remap_types(),
            Database::WordPairProximityDocids => index.word_pair_proximity_docids.remap_types(),
            Database::FacetIdIsNullDocids => index.facet_id_is_null_docids.remap_types(),
            Database::FacetIdIsEmptyDocids => index.facet_id_is_empty_docids.remap_types(),
            Database::FacetIdExistsDocids => index.facet_id_exists_docids.remap_types(),
            Database::FacetIdF64NumberDocids => index.facet_id_f64_docids.remap_types(),
            Database::FacetIdStringDocids => index.facet_id_string_docids.remap_types(),
        }
    }
}

impl From<FacetKind> for Database {
    fn from(value: FacetKind) -> Self {
        match value {
            FacetKind::Number => Database::FacetIdF64NumberDocids,
            FacetKind::String => Database::FacetIdStringDocids,
            FacetKind::Null => Database::FacetIdIsNullDocids,
            FacetKind::Empty => Database::FacetIdIsEmptyDocids,
            FacetKind::Exists => Database::FacetIdExistsDocids,
        }
    }
}

impl WriterOperation {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        self.database.database(index)
    }

    pub fn entry(self) -> EntryOperation {
        self.entry
    }
}

pub struct WriterReceiver(Receiver<WriterOperation>);

impl IntoIterator for WriterReceiver {
    type Item = WriterOperation;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct MergerSender {
    sender: Sender<WriterOperation>,
    /// The number of message we send in total in the channel.
    send_count: std::cell::Cell<usize>,
    /// The number of times we sent something in a channel that was full.
    writer_contentious_count: std::cell::Cell<usize>,
    /// The number of times we sent something in a channel that was empty.
    merger_contentious_count: std::cell::Cell<usize>,
}

impl Drop for MergerSender {
    fn drop(&mut self) {
        eprintln!(
            "Merger channel stats: {} sends, {} writer contentions ({}%), {} merger contentions ({}%)",
            self.send_count.get(),
            self.writer_contentious_count.get(),
            (self.writer_contentious_count.get() as f32 / self.send_count.get() as f32) * 100.0,
            self.merger_contentious_count.get(),
            (self.merger_contentious_count.get() as f32 / self.send_count.get() as f32) * 100.0
        )
    }
}

impl MergerSender {
    pub fn main(&self) -> MainSender<'_> {
        MainSender(self)
    }

    pub fn docids<D: DatabaseType>(&self) -> WordDocidsSender<'_, D> {
        WordDocidsSender { sender: self, _marker: PhantomData }
    }

    pub fn facet_docids(&self) -> FacetDocidsSender<'_> {
        FacetDocidsSender { sender: self }
    }

    pub fn documents(&self) -> DocumentsSender<'_> {
        DocumentsSender(self)
    }

    pub fn send_documents_ids(&self, documents_ids: RoaringBitmap) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_small_key_bitmap(
            DOCUMENTS_IDS_KEY.as_bytes(),
            documents_ids,
        ));
        match self.send(WriterOperation { database: Database::Main, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    fn send(&self, op: WriterOperation) -> StdResult<(), SendError<()>> {
        if self.sender.is_full() {
            self.writer_contentious_count.set(self.writer_contentious_count.get() + 1);
        }
        if self.sender.is_empty() {
            self.merger_contentious_count.set(self.merger_contentious_count.get() + 1);
        }
        self.send_count.set(self.send_count.get() + 1);
        match self.sender.send(op) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub struct MainSender<'a>(&'a MergerSender);

impl MainSender<'_> {
    pub fn write_words_fst(&self, value: Mmap) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_large_key_value(
            WORDS_FST_KEY.as_bytes(),
            value,
        ));
        match self.0.send(WriterOperation { database: Database::Main, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn write_words_prefixes_fst(&self, value: Mmap) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_large_key_value(
            WORDS_PREFIXES_FST_KEY.as_bytes(),
            value,
        ));
        match self.0.send(WriterOperation { database: Database::Main, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Delete(KeyEntry::from_key(key));
        match self.0.send(WriterOperation { database: Database::Main, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub enum ExactWordDocids {}
pub enum FidWordCountDocids {}
pub enum WordDocids {}
pub enum WordFidDocids {}
pub enum WordPairProximityDocids {}
pub enum WordPositionDocids {}
pub enum FacetDocids {}

pub trait DatabaseType {
    const DATABASE: Database;
}

pub trait MergerOperationType {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation;
}

impl DatabaseType for ExactWordDocids {
    const DATABASE: Database = Database::ExactWordDocids;
}

impl MergerOperationType for ExactWordDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::ExactWordDocidsMerger(merger)
    }
}

impl DatabaseType for FidWordCountDocids {
    const DATABASE: Database = Database::FidWordCountDocids;
}

impl MergerOperationType for FidWordCountDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::FidWordCountDocidsMerger(merger)
    }
}

impl DatabaseType for WordDocids {
    const DATABASE: Database = Database::WordDocids;
}

impl MergerOperationType for WordDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::WordDocidsMerger(merger)
    }
}

impl DatabaseType for WordFidDocids {
    const DATABASE: Database = Database::WordFidDocids;
}

impl MergerOperationType for WordFidDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::WordFidDocidsMerger(merger)
    }
}

impl DatabaseType for WordPairProximityDocids {
    const DATABASE: Database = Database::WordPairProximityDocids;
}

impl MergerOperationType for WordPairProximityDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::WordPairProximityDocidsMerger(merger)
    }
}

impl DatabaseType for WordPositionDocids {
    const DATABASE: Database = Database::WordPositionDocids;
}

impl MergerOperationType for WordPositionDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::WordPositionDocidsMerger(merger)
    }
}

impl MergerOperationType for FacetDocids {
    fn new_merger_operation(merger: Merger<File, MergeDeladdCboRoaringBitmaps>) -> MergerOperation {
        MergerOperation::FacetDocidsMerger(merger)
    }
}

pub trait DocidsSender {
    fn write(&self, key: &[u8], value: &[u8]) -> StdResult<(), SendError<()>>;
    fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>>;
}

pub struct WordDocidsSender<'a, D> {
    sender: &'a MergerSender,
    _marker: PhantomData<D>,
}

impl<D: DatabaseType> DocidsSender for WordDocidsSender<'_, D> {
    fn write(&self, key: &[u8], value: &[u8]) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_small_key_value(key, value));
        match self.sender.send(WriterOperation { database: D::DATABASE, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Delete(KeyEntry::from_key(key));
        match self.sender.send(WriterOperation { database: D::DATABASE, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub struct FacetDocidsSender<'a> {
    sender: &'a MergerSender,
}

impl DocidsSender for FacetDocidsSender<'_> {
    fn write(&self, key: &[u8], value: &[u8]) -> StdResult<(), SendError<()>> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let database = Database::from(facet_kind);
        // let entry = EntryOperation::Write(KeyValueEntry::from_small_key_value(key, value));
        let entry = match facet_kind {
            // skip level group size
            FacetKind::String | FacetKind::Number => {
                // add facet group size
                let value = [&[1], value].concat();
                EntryOperation::Write(KeyValueEntry::from_small_key_value(key, &value))
            }
            _ => EntryOperation::Write(KeyValueEntry::from_small_key_value(key, value)),
        };
        match self.sender.send(WriterOperation { database, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let database = Database::from(facet_kind);
        let entry = EntryOperation::Delete(KeyEntry::from_key(key));
        match self.sender.send(WriterOperation { database, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub struct DocumentsSender<'a>(&'a MergerSender);

impl DocumentsSender<'_> {
    /// TODO do that efficiently
    pub fn uncompressed(
        &self,
        docid: DocumentId,
        external_id: String,
        document: &KvReaderFieldId,
    ) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_small_key_value(
            &docid.to_be_bytes(),
            document.as_bytes(),
        ));
        match self.0.send(WriterOperation { database: Database::Documents, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }?;

        let entry = EntryOperation::Write(KeyValueEntry::from_small_key_value(
            external_id.as_bytes(),
            &docid.to_be_bytes(),
        ));
        match self.0.send(WriterOperation { database: Database::ExternalDocumentsIds, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, docid: DocumentId, external_id: String) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Delete(KeyEntry::from_key(&docid.to_be_bytes()));
        match self.0.send(WriterOperation { database: Database::Documents, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }?;

        let entry = EntryOperation::Delete(KeyEntry::from_key(external_id.as_bytes()));
        match self.0.send(WriterOperation { database: Database::ExternalDocumentsIds, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub enum MergerOperation {
    ExactWordDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    FidWordCountDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    WordDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    WordFidDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    WordPairProximityDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    WordPositionDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    FacetDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    DeleteDocument { docid: DocumentId, external_id: String },
    InsertDocument { docid: DocumentId, external_id: String, document: Box<KvReaderFieldId> },
    FinishedDocument,
}

pub struct MergerReceiver(Receiver<MergerOperation>);

impl IntoIterator for MergerReceiver {
    type Item = MergerOperation;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct ExtractorSender(Sender<MergerOperation>);

impl ExtractorSender {
    pub fn document_sender(&self) -> DocumentSender<'_> {
        DocumentSender(Some(&self.0))
    }

    pub fn send_searchable<D: MergerOperationType>(
        &self,
        merger: Merger<File, MergeDeladdCboRoaringBitmaps>,
    ) -> StdResult<(), SendError<()>> {
        match self.0.send(D::new_merger_operation(merger)) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub struct DocumentSender<'a>(Option<&'a Sender<MergerOperation>>);

impl DocumentSender<'_> {
    pub fn insert(
        &self,
        docid: DocumentId,
        external_id: String,
        document: Box<KvReaderFieldId>,
    ) -> StdResult<(), SendError<()>> {
        let sender = self.0.unwrap();
        match sender.send(MergerOperation::InsertDocument { docid, external_id, document }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, docid: DocumentId, external_id: String) -> StdResult<(), SendError<()>> {
        let sender = self.0.unwrap();
        match sender.send(MergerOperation::DeleteDocument { docid, external_id }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn finish(mut self) -> StdResult<(), SendError<()>> {
        let sender = self.0.take().unwrap();
        match sender.send(MergerOperation::FinishedDocument) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

impl Drop for DocumentSender<'_> {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(MergerOperation::FinishedDocument);
        }
    }
}
