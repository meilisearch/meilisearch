use std::fs::File;

use crossbeam_channel::{IntoIter, Receiver, SendError, Sender};
use heed::types::Bytes;

use super::indexer::KvReaderFieldId;
use super::StdResult;
use crate::{DocumentId, Index};

/// The capacity of the channel is currently in number of messages.
pub fn merger_writer_channels(cap: usize) -> (MergerSender, WriterReceiver) {
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (MergerSender(sender), WriterReceiver(receiver))
}

/// The capacity of the channel is currently in number of messages.
pub fn extractors_merger_channels(cap: usize) -> ExtractorsMergerChannels {
    let (sender, receiver) = crossbeam_channel::bounded(cap);

    ExtractorsMergerChannels {
        merger_receiver: MergerReceiver(receiver),
        deladd_cbo_roaring_bitmap_sender: DeladdCboRoaringBitmapSender(sender.clone()),
    }
}

pub struct ExtractorsMergerChannels {
    pub merger_receiver: MergerReceiver,
    pub deladd_cbo_roaring_bitmap_sender: DeladdCboRoaringBitmapSender,
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

pub enum WriterOperation {
    WordDocids(EntryOperation),
    Document(DocumentEntry),
}

impl WriterOperation {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        match self {
            WriterOperation::WordDocids(_) => index.word_docids.remap_types(),
            WriterOperation::Document(_) => index.documents.remap_types(),
        }
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
    pub fn word_docids(&self) -> WordDocidsSender<'_> {
        WordDocidsSender(&self.0)
    }
}

pub struct WordDocidsSender<'a>(&'a Sender<WriterOperation>);

impl WordDocidsSender<'_> {
    pub fn write(&self, key: &[u8], value: &[u8]) -> StdResult<(), SendError<()>> {
        let operation = EntryOperation::Write(KeyValueEntry::from_key_value(key, value));
        match self.0.send(WriterOperation::WordDocids(operation)) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    pub fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>> {
        let operation = EntryOperation::Delete(KeyEntry::from_key(key));
        match self.0.send(WriterOperation::WordDocids(operation)) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

#[derive(Clone)]
pub struct DocumentSender(Sender<WriterOperation>);

impl DocumentSender {
    pub fn send(&self, document: DocumentEntry) -> StdResult<(), SendError<()>> {
        match self.0.send(WriterOperation::Document(document)) {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }
}

pub enum MergerOperation {
    WordDocidsCursors(Vec<grenad::ReaderCursor<File>>),
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
