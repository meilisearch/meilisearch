use crossbeam_channel::{Receiver, RecvError, SendError, Sender};
use heed::types::Bytes;

use super::indexer::KvReaderFieldId;
use super::StdResult;
use crate::{DocumentId, Index};

/// The capacity of the channel is currently in number of messages.
pub fn merge_writer_channel(cap: usize) -> WriterChannels {
    let (sender, receiver) = crossbeam_channel::bounded(cap);

    WriterChannels {
        writer_receiver: WriterReceiver(receiver),
        merger_sender: MergerSender(sender.clone()),
        document_sender: DocumentSender(sender),
    }
}

pub struct WriterChannels {
    pub writer_receiver: WriterReceiver,
    pub merger_sender: MergerSender,
    pub document_sender: DocumentSender,
}

pub struct KeyValueEntry {
    pub key_length: u16,
    pub data: Box<[u8]>,
}

impl KeyValueEntry {
    pub fn entry(&self) -> (&[u8], &[u8]) {
        self.data.split_at(self.key_length as usize)
    }
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

    pub fn entry(&self) -> ([u8; 4], &[u8]) {
        let docid = self.docid.to_be_bytes();
        (docid, &self.content)
    }
}

pub enum WriterOperation {
    WordDocIds(KeyValueEntry),
    Document(DocumentEntry),
}

impl WriterOperation {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        match self {
            WriterOperation::WordDocIds(_) => index.word_docids.remap_types(),
            WriterOperation::Document(_) => index.documents.remap_types(),
        }
    }
}

pub struct WriterReceiver(Receiver<WriterOperation>);

impl WriterReceiver {
    pub fn recv(&self) -> StdResult<WriterOperation, RecvError> {
        self.0.recv()
    }
}

pub struct MergerSender(Sender<WriterOperation>);

#[derive(Clone)]
pub struct DocumentSender(Sender<WriterOperation>);

impl DocumentSender {
    pub fn send(&self, document: DocumentEntry) -> StdResult<(), SendError<DocumentEntry>> {
        match self.0.send(WriterOperation::Document(document)) {
            Ok(()) => Ok(()),
            Err(SendError(wop)) => match wop {
                WriterOperation::Document(entry) => Err(SendError(entry)),
                _ => unreachable!(),
            },
        }
    }
}
