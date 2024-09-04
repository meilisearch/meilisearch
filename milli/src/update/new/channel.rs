use std::fs::File;

use crossbeam_channel::{IntoIter, Receiver, SendError, Sender};
use grenad::Merger;
use heed::types::Bytes;

use super::StdResult;
use crate::index::main_key::{DOCUMENTS_IDS_KEY, WORDS_FST_KEY};
use crate::update::new::KvReaderFieldId;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{DocumentId, Index};

/// The capacity of the channel is currently in number of messages.
pub fn merger_writer_channel(cap: usize) -> (MergerSender, WriterReceiver) {
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (MergerSender(sender), WriterReceiver(receiver))
}

/// The capacity of the channel is currently in number of messages.
pub fn extractors_merger_channels(cap: usize) -> ExtractorsMergerChannels {
    let (sender, receiver) = crossbeam_channel::bounded(cap);

    ExtractorsMergerChannels {
        merger_receiver: MergerReceiver(receiver),
        deladd_cbo_roaring_bitmap_sender: DeladdCboRoaringBitmapSender(sender.clone()),
        extracted_documents_sender: ExtractedDocumentsSender(sender.clone()),
    }
}

pub struct ExtractorsMergerChannels {
    pub merger_receiver: MergerReceiver,
    pub deladd_cbo_roaring_bitmap_sender: DeladdCboRoaringBitmapSender,
    pub extracted_documents_sender: ExtractedDocumentsSender,
}

pub struct KeyValueEntry {
    key_length: usize,
    data: Box<[u8]>,
}

impl KeyValueEntry {
    pub fn from_key_value(key: &[u8], value: &[u8]) -> Self {
        let mut data = Vec::with_capacity(key.len() + value.len());
        data.extend_from_slice(key);
        data.extend_from_slice(value);

        KeyValueEntry { key_length: key.len(), data: data.into_boxed_slice() }
    }

    pub fn key(&self) -> &[u8] {
        &self.data.as_ref()[..self.key_length]
    }

    pub fn value(&self) -> &[u8] {
        &self.data.as_ref()[self.key_length..]
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

pub enum Database {
    WordDocids,
    Documents,
    Main,
}

impl WriterOperation {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        match self.database {
            Database::Main => index.main.remap_types(),
            Database::Documents => index.documents.remap_types(),
            Database::WordDocids => index.word_docids.remap_types(),
        }
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

pub struct MergerSender(Sender<WriterOperation>);

impl MergerSender {
    pub fn main(&self) -> MainSender<'_> {
        MainSender(&self.0)
    }

    pub fn word_docids(&self) -> WordDocidsSender<'_> {
        WordDocidsSender(&self.0)
    }

    pub fn documents(&self) -> DocumentsSender<'_> {
        DocumentsSender(&self.0)
    }

    pub fn send_documents_ids(&self, bitmap: &[u8]) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_key_value(
            DOCUMENTS_IDS_KEY.as_bytes(),
            bitmap,
        ));
        match self.0.send(WriterOperation { database: Database::Main, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub struct MainSender<'a>(&'a Sender<WriterOperation>);

impl MainSender<'_> {
    pub fn write_words_fst(&self, value: &[u8]) -> StdResult<(), SendError<()>> {
        let entry =
            EntryOperation::Write(KeyValueEntry::from_key_value(WORDS_FST_KEY.as_bytes(), value));
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

pub struct WordDocidsSender<'a>(&'a Sender<WriterOperation>);

impl WordDocidsSender<'_> {
    pub fn write(&self, key: &[u8], value: &[u8]) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_key_value(key, value));
        match self.0.send(WriterOperation { database: Database::WordDocids, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Delete(KeyEntry::from_key(key));
        match self.0.send(WriterOperation { database: Database::WordDocids, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub struct DocumentsSender<'a>(&'a Sender<WriterOperation>);

impl DocumentsSender<'_> {
    /// TODO do that efficiently
    pub fn uncompressed(
        &self,
        docid: DocumentId,
        document: &KvReaderFieldId,
    ) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Write(KeyValueEntry::from_key_value(
            &docid.to_be_bytes(),
            document.as_bytes(),
        ));
        match self.0.send(WriterOperation { database: Database::Documents, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, docid: DocumentId) -> StdResult<(), SendError<()>> {
        let entry = EntryOperation::Delete(KeyEntry::from_key(&docid.to_be_bytes()));
        match self.0.send(WriterOperation { database: Database::Documents, entry }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub enum MergerOperation {
    WordDocidsMerger(Merger<File, MergeDeladdCboRoaringBitmaps>),
    InsertDocument { docid: DocumentId, document: Box<KvReaderFieldId> },
    DeleteDocument { docid: DocumentId },
}

pub struct MergerReceiver(Receiver<MergerOperation>);

impl IntoIterator for MergerReceiver {
    type Item = MergerOperation;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone)]
pub struct DeladdCboRoaringBitmapSender(Sender<MergerOperation>);

impl DeladdCboRoaringBitmapSender {
    pub fn word_docids(
        &self,
        merger: Merger<File, MergeDeladdCboRoaringBitmaps>,
    ) -> StdResult<(), SendError<()>> {
        let operation = MergerOperation::WordDocidsMerger(merger);
        match self.0.send(operation) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

#[derive(Clone)]
pub struct ExtractedDocumentsSender(Sender<MergerOperation>);

impl ExtractedDocumentsSender {
    pub fn insert(
        &self,
        docid: DocumentId,
        document: Box<KvReaderFieldId>,
    ) -> StdResult<(), SendError<()>> {
        match self.0.send(MergerOperation::InsertDocument { docid, document }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, docid: DocumentId) -> StdResult<(), SendError<()>> {
        match self.0.send(MergerOperation::DeleteDocument { docid }) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}
