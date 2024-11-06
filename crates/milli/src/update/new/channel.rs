use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_channel::{IntoIter, Receiver, SendError, Sender};
use heed::types::Bytes;
use roaring::RoaringBitmap;

use super::extract::FacetKind;
use super::StdResult;
use crate::index::main_key::DOCUMENTS_IDS_KEY;
use crate::update::new::KvReaderFieldId;
use crate::{DocumentId, Index};

/// The capacity of the channel is currently in number of messages.
pub fn extractor_writer_channel(cap: usize) -> (ExtractorSender, WriterReceiver) {
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (
        ExtractorSender {
            sender,
            send_count: Default::default(),
            writer_contentious_count: Default::default(),
            extractor_contentious_count: Default::default(),
        },
        WriterReceiver(receiver),
    )
}

pub struct KeyValueEntry {
    pub key_length: usize,
    pub data: Box<[u8]>,
}

impl KeyValueEntry {
    pub fn from_small_key_value(key: &[u8], value: &[u8]) -> Self {
        let mut data = Vec::with_capacity(key.len() + value.len());
        data.extend_from_slice(key);
        data.extend_from_slice(value);
        KeyValueEntry { key_length: key.len(), data: data.into_boxed_slice() }
    }

    pub fn from_small_key_bitmap(key: &[u8], bitmap: RoaringBitmap) -> Self {
        let mut data = Vec::with_capacity(key.len() + bitmap.serialized_size());
        data.extend_from_slice(key);
        bitmap.serialize_into(&mut data).unwrap();
        KeyValueEntry { key_length: key.len(), data: data.into_boxed_slice() }
    }

    pub fn key(&self) -> &[u8] {
        &self.data[..self.key_length]
    }

    pub fn value(&self) -> &[u8] {
        &self.data[self.key_length..]
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

pub struct ExtractorSender {
    sender: Sender<WriterOperation>,
    /// The number of message we sent in total in the channel.
    send_count: AtomicUsize,
    /// The number of times we sent something in a channel that was full.
    writer_contentious_count: AtomicUsize,
    /// The number of times we sent something in a channel that was empty.
    extractor_contentious_count: AtomicUsize,
}

impl Drop for ExtractorSender {
    fn drop(&mut self) {
        let send_count = *self.send_count.get_mut();
        let writer_contentious_count = *self.writer_contentious_count.get_mut();
        let extractor_contentious_count = *self.extractor_contentious_count.get_mut();
        eprintln!(
            "Extractor channel stats: {send_count} sends, \
            {writer_contentious_count} writer contentions ({}%), \
            {extractor_contentious_count} extractor contentions ({}%)",
            (writer_contentious_count as f32 / send_count as f32) * 100.0,
            (extractor_contentious_count as f32 / send_count as f32) * 100.0
        )
    }
}

impl ExtractorSender {
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
            self.writer_contentious_count.fetch_add(1, Ordering::SeqCst);
        }
        if self.sender.is_empty() {
            self.extractor_contentious_count.fetch_add(1, Ordering::SeqCst);
        }
        self.send_count.fetch_add(1, Ordering::SeqCst);
        match self.sender.send(op) {
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

pub trait DatabaseType {
    const DATABASE: Database;
}

impl DatabaseType for ExactWordDocids {
    const DATABASE: Database = Database::ExactWordDocids;
}

impl DatabaseType for FidWordCountDocids {
    const DATABASE: Database = Database::FidWordCountDocids;
}

impl DatabaseType for WordDocids {
    const DATABASE: Database = Database::WordDocids;
}

impl DatabaseType for WordFidDocids {
    const DATABASE: Database = Database::WordFidDocids;
}

impl DatabaseType for WordPairProximityDocids {
    const DATABASE: Database = Database::WordPairProximityDocids;
}

impl DatabaseType for WordPositionDocids {
    const DATABASE: Database = Database::WordPositionDocids;
}

pub trait DocidsSender {
    fn write(&self, key: &[u8], value: &[u8]) -> StdResult<(), SendError<()>>;
    fn delete(&self, key: &[u8]) -> StdResult<(), SendError<()>>;
}

pub struct WordDocidsSender<'a, D> {
    sender: &'a ExtractorSender,
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
    sender: &'a ExtractorSender,
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

pub struct DocumentsSender<'a>(&'a ExtractorSender);

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
