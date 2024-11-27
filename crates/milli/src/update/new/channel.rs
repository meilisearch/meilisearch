use std::cell::RefCell;
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroU16;

use bbqueue::framed::{FrameGrantR, FrameProducer};
use bytemuck::{checked, CheckedBitPattern, NoUninit};
use crossbeam_channel::SendError;
use heed::types::Bytes;
use heed::BytesDecode;
use memmap2::Mmap;
use roaring::RoaringBitmap;

use super::extract::FacetKind;
use super::ref_cell_ext::RefCellExt;
use super::thread_local::{FullySend, ThreadLocal};
use super::StdResult;
use crate::heed_codec::facet::{FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec};
use crate::index::db_name;
use crate::index::main_key::{GEO_FACETED_DOCUMENTS_IDS_KEY, GEO_RTREE_KEY};
use crate::update::new::KvReaderFieldId;
use crate::vector::Embedding;
use crate::{CboRoaringBitmapCodec, DocumentId, Index};

/// Creates a tuple of senders/receiver to be used by
/// the extractors and the writer loop.
///
/// The `channel_capacity` parameter defines the number of
/// too-large-to-fit-in-BBQueue entries that can be sent through
/// a crossbeam channel. This parameter must stay low to make
/// sure we do not use too much memory.
///
/// Note that the channel is also used to wake-up the receiver
/// wehn new stuff is available in any BBQueue buffer but we send
/// a message in this queue only if it is empty to avoid filling
/// the channel *and* the BBQueue.
///
/// # Safety
///
/// Panics if the number of provided BBQueues is not exactly equal
/// to the number of available threads in the rayon threadpool.
pub fn extractor_writer_bbqueue(
    bbbuffers: &[bbqueue::BBBuffer],
    channel_capacity: usize,
) -> (ExtractorBbqueueSender, WriterBbqueueReceiver) {
    assert_eq!(
        bbbuffers.len(),
        rayon::current_num_threads(),
        "You must provide as many BBBuffer as the available number of threads to extract"
    );

    let capacity = bbbuffers.first().unwrap().capacity();
    // Read the field description to understand this
    let capacity = capacity.checked_sub(9).unwrap();

    let producers = ThreadLocal::with_capacity(bbbuffers.len());
    let consumers = rayon::broadcast(|bi| {
        let bbqueue = &bbbuffers[bi.index()];
        let (producer, consumer) = bbqueue.try_split_framed().unwrap();
        producers.get_or(|| FullySend(RefCell::new(producer)));
        consumer
    });

    let (sender, receiver) = crossbeam_channel::bounded(channel_capacity);
    let sender = ExtractorBbqueueSender { sender, producers, capacity };
    let receiver = WriterBbqueueReceiver { receiver, consumers };
    (sender, receiver)
}

pub struct ExtractorBbqueueSender<'a> {
    /// This channel is used to wake-up the receiver and
    /// send large entries that cannot fit in the BBQueue.
    sender: crossbeam_channel::Sender<ReceiverAction>,
    /// A memory buffer, one by thread, is used to serialize
    /// the entries directly in this shared, lock-free space.
    producers: ThreadLocal<FullySend<RefCell<FrameProducer<'a>>>>,
    /// The capacity of this frame producer, will never be able to store more than that.
    ///
    /// Note that the FrameProducer requires up to 9 bytes to encode the length,
    /// the capacity has been shrinked accordingly.
    ///
    /// <https://docs.rs/bbqueue/latest/bbqueue/framed/index.html#frame-header>
    capacity: usize,
}

pub struct WriterBbqueueReceiver<'a> {
    /// Used to wake up when new entries are available either in
    /// any BBQueue buffer or directly sent throught this channel
    /// (still written to disk).
    receiver: crossbeam_channel::Receiver<ReceiverAction>,
    /// The BBQueue frames to read when waking-up.
    consumers: Vec<bbqueue::framed::FrameConsumer<'a>>,
}

/// The action to perform on the receiver/writer side.
#[derive(Debug)]
pub enum ReceiverAction {
    /// Wake up, you have frames to read for the BBQueue buffers.
    WakeUp,
    /// An entry that cannot fit in the BBQueue buffers has been
    /// written to disk, memory-mapped and must be written in the
    /// database.
    LargeEntry {
        /// The database where the entry must be written.
        database: Database,
        /// The key of the entry that must be written in the database.
        key: Box<[u8]>,
        /// The large value that must be written.
        ///
        /// Note: We can probably use a `File` here and
        /// use `Database::put_reserved` instead of memory-mapping.
        value: Mmap,
    },
}

impl<'a> WriterBbqueueReceiver<'a> {
    pub fn recv(&mut self) -> Option<ReceiverAction> {
        self.receiver.recv().ok()
    }

    pub fn read(&mut self) -> Option<FrameWithHeader<'a>> {
        for consumer in &mut self.consumers {
            if let Some(frame) = consumer.read() {
                return Some(FrameWithHeader::from(frame));
            }
        }
        None
    }
}

pub struct FrameWithHeader<'a> {
    header: EntryHeader,
    frame: FrameGrantR<'a>,
}

impl FrameWithHeader<'_> {
    pub fn header(&self) -> EntryHeader {
        self.header
    }

    pub fn frame(&self) -> &FrameGrantR<'_> {
        &self.frame
    }
}

impl<'a> From<FrameGrantR<'a>> for FrameWithHeader<'a> {
    fn from(mut frame: FrameGrantR<'a>) -> Self {
        frame.auto_release(true);
        FrameWithHeader { header: EntryHeader::from_slice(&frame[..]), frame }
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
/// Wether a put of the key/value pair or a delete of the given key.
pub struct DbOperation {
    /// The database on which to perform the operation.
    pub database: Database,
    /// The key length in the buffer.
    ///
    /// If None it means that the buffer is dedicated
    /// to the key and it is therefore a deletion operation.
    pub key_length: Option<NonZeroU16>,
}

impl DbOperation {
    pub fn key_value<'a>(&self, frame: &'a FrameGrantR<'_>) -> (&'a [u8], Option<&'a [u8]>) {
        /// TODO replace the return type by an enum Write | Delete
        let skip = EntryHeader::variant_size() + mem::size_of::<Self>();
        match self.key_length {
            Some(key_length) => {
                let (key, value) = frame[skip..].split_at(key_length.get() as usize);
                (key, Some(value))
            }
            None => (&frame[skip..], None),
        }
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(transparent)]
pub struct ArroyDeleteVector {
    pub docid: DocumentId,
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
/// The embedding is the remaining space and represents a non-aligned [f32].
pub struct ArroySetVector {
    pub docid: DocumentId,
    pub embedder_id: u8,
    _padding: [u8; 3],
}

impl ArroySetVector {
    pub fn read_embedding_into_vec<'v>(
        &self,
        frame: &FrameGrantR<'_>,
        vec: &'v mut Vec<f32>,
    ) -> &'v [f32] {
        vec.clear();
        let skip = EntryHeader::variant_size() + mem::size_of::<Self>();
        let bytes = &frame[skip..];
        bytes.chunks_exact(mem::size_of::<f32>()).for_each(|bytes| {
            let f = bytes.try_into().map(f32::from_ne_bytes).unwrap();
            vec.push(f);
        });
        &vec[..]
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryHeader {
    DbOperation(DbOperation),
    ArroyDeleteVector(ArroyDeleteVector),
    ArroySetVector(ArroySetVector),
}

impl EntryHeader {
    const fn variant_size() -> usize {
        mem::size_of::<u8>()
    }

    const fn variant_id(&self) -> u8 {
        match self {
            EntryHeader::DbOperation(_) => 0,
            EntryHeader::ArroyDeleteVector(_) => 1,
            EntryHeader::ArroySetVector(_) => 2,
        }
    }

    const fn total_key_value_size(key_length: NonZeroU16, value_length: usize) -> usize {
        Self::variant_size()
            + mem::size_of::<DbOperation>()
            + key_length.get() as usize
            + value_length
    }

    const fn total_key_size(key_length: NonZeroU16) -> usize {
        Self::total_key_value_size(key_length, 0)
    }

    const fn total_delete_vector_size() -> usize {
        Self::variant_size() + mem::size_of::<ArroyDeleteVector>()
    }

    /// The `embedding_length` corresponds to the number of `f32` in the embedding.
    fn total_set_vector_size(embedding_length: usize) -> usize {
        Self::variant_size()
            + mem::size_of::<ArroySetVector>()
            + embedding_length * mem::size_of::<f32>()
    }

    fn header_size(&self) -> usize {
        let payload_size = match self {
            EntryHeader::DbOperation(op) => mem::size_of_val(op),
            EntryHeader::ArroyDeleteVector(adv) => mem::size_of_val(adv),
            EntryHeader::ArroySetVector(asv) => mem::size_of_val(asv),
        };
        Self::variant_size() + payload_size
    }

    fn from_slice(slice: &[u8]) -> EntryHeader {
        let (variant_id, remaining) = slice.split_first().unwrap();
        match variant_id {
            0 => {
                let header_bytes = &remaining[..mem::size_of::<DbOperation>()];
                let header = checked::pod_read_unaligned(header_bytes);
                EntryHeader::DbOperation(header)
            }
            1 => {
                let header_bytes = &remaining[..mem::size_of::<ArroyDeleteVector>()];
                let header = checked::pod_read_unaligned(header_bytes);
                EntryHeader::ArroyDeleteVector(header)
            }
            2 => {
                let header_bytes = &remaining[..mem::size_of::<ArroySetVector>()];
                let header = checked::pod_read_unaligned(header_bytes);
                EntryHeader::ArroySetVector(header)
            }
            id => panic!("invalid variant id: {id}"),
        }
    }

    fn serialize_into(&self, header_bytes: &mut [u8]) {
        let (first, remaining) = header_bytes.split_first_mut().unwrap();
        let payload_bytes = match self {
            EntryHeader::DbOperation(op) => bytemuck::bytes_of(op),
            EntryHeader::ArroyDeleteVector(adv) => bytemuck::bytes_of(adv),
            EntryHeader::ArroySetVector(asv) => bytemuck::bytes_of(asv),
        };
        *first = self.variant_id();
        remaining.copy_from_slice(payload_bytes);
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(u16)]
pub enum Database {
    Main,
    Documents,
    ExternalDocumentsIds,
    ExactWordDocids,
    FidWordCountDocids,
    WordDocids,
    WordFidDocids,
    WordPairProximityDocids,
    WordPositionDocids,
    FacetIdIsNullDocids,
    FacetIdIsEmptyDocids,
    FacetIdExistsDocids,
    FacetIdF64Docids,
    FacetIdStringDocids,
    FieldIdDocidFacetStrings,
    FieldIdDocidFacetF64s,
}

impl Database {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        match self {
            Database::Main => index.main.remap_types(),
            Database::Documents => index.documents.remap_types(),
            Database::ExternalDocumentsIds => index.external_documents_ids.remap_types(),
            Database::ExactWordDocids => index.exact_word_docids.remap_types(),
            Database::WordDocids => index.word_docids.remap_types(),
            Database::WordFidDocids => index.word_fid_docids.remap_types(),
            Database::WordPositionDocids => index.word_position_docids.remap_types(),
            Database::FidWordCountDocids => index.field_id_word_count_docids.remap_types(),
            Database::WordPairProximityDocids => index.word_pair_proximity_docids.remap_types(),
            Database::FacetIdIsNullDocids => index.facet_id_is_null_docids.remap_types(),
            Database::FacetIdIsEmptyDocids => index.facet_id_is_empty_docids.remap_types(),
            Database::FacetIdExistsDocids => index.facet_id_exists_docids.remap_types(),
            Database::FacetIdF64Docids => index.facet_id_f64_docids.remap_types(),
            Database::FacetIdStringDocids => index.facet_id_string_docids.remap_types(),
            Database::FieldIdDocidFacetStrings => index.field_id_docid_facet_strings.remap_types(),
            Database::FieldIdDocidFacetF64s => index.field_id_docid_facet_f64s.remap_types(),
        }
    }

    pub fn database_name(&self) -> &'static str {
        match self {
            Database::Main => db_name::MAIN,
            Database::Documents => db_name::DOCUMENTS,
            Database::ExternalDocumentsIds => db_name::EXTERNAL_DOCUMENTS_IDS,
            Database::ExactWordDocids => db_name::EXACT_WORD_DOCIDS,
            Database::WordDocids => db_name::WORD_DOCIDS,
            Database::WordFidDocids => db_name::WORD_FIELD_ID_DOCIDS,
            Database::WordPositionDocids => db_name::WORD_POSITION_DOCIDS,
            Database::FidWordCountDocids => db_name::FIELD_ID_WORD_COUNT_DOCIDS,
            Database::WordPairProximityDocids => db_name::WORD_PAIR_PROXIMITY_DOCIDS,
            Database::FacetIdIsNullDocids => db_name::FACET_ID_IS_NULL_DOCIDS,
            Database::FacetIdIsEmptyDocids => db_name::FACET_ID_IS_EMPTY_DOCIDS,
            Database::FacetIdExistsDocids => db_name::FACET_ID_EXISTS_DOCIDS,
            Database::FacetIdF64Docids => db_name::FACET_ID_F64_DOCIDS,
            Database::FacetIdStringDocids => db_name::FACET_ID_STRING_DOCIDS,
            Database::FieldIdDocidFacetStrings => db_name::FIELD_ID_DOCID_FACET_STRINGS,
            Database::FieldIdDocidFacetF64s => db_name::FIELD_ID_DOCID_FACET_F64S,
        }
    }
}

impl From<FacetKind> for Database {
    fn from(value: FacetKind) -> Self {
        match value {
            FacetKind::Number => Database::FacetIdF64Docids,
            FacetKind::String => Database::FacetIdStringDocids,
            FacetKind::Null => Database::FacetIdIsNullDocids,
            FacetKind::Empty => Database::FacetIdIsEmptyDocids,
            FacetKind::Exists => Database::FacetIdExistsDocids,
        }
    }
}

impl<'b> ExtractorBbqueueSender<'b> {
    pub fn docids<'a, D: DatabaseType>(&'a self) -> WordDocidsSender<'a, 'b, D> {
        WordDocidsSender { sender: self, _marker: PhantomData }
    }

    pub fn facet_docids<'a>(&'a self) -> FacetDocidsSender<'a, 'b> {
        FacetDocidsSender { sender: self }
    }

    pub fn field_id_docid_facet_sender<'a>(&'a self) -> FieldIdDocidFacetSender<'a, 'b> {
        FieldIdDocidFacetSender(&self)
    }

    pub fn documents<'a>(&'a self) -> DocumentsSender<'a, 'b> {
        DocumentsSender(&self)
    }

    pub fn embeddings<'a>(&'a self) -> EmbeddingSender<'a, 'b> {
        EmbeddingSender(&self)
    }

    pub fn geo<'a>(&'a self) -> GeoSender<'a, 'b> {
        GeoSender(&self)
    }

    fn delete_vector(&self, docid: DocumentId) -> crate::Result<()> {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let payload_header = EntryHeader::ArroyDeleteVector(ArroyDeleteVector { docid });
        let total_length = EntryHeader::total_delete_vector_size();
        if total_length > capacity {
            unreachable!("entry larger that the BBQueue capacity");
        }

        // Spin loop to have a frame the size we requested.
        let mut grant = loop {
            match producer.grant(total_length) {
                Ok(grant) => break grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            }
        };

        payload_header.serialize_into(&mut grant);

        // We could commit only the used memory.
        grant.commit(total_length);

        // We only send a wake up message when the channel is empty
        // so that we don't fill the channel with too many WakeUps.
        if self.sender.is_empty() {
            self.sender.send(ReceiverAction::WakeUp).unwrap();
        }

        Ok(())
    }

    fn set_vector(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embedding: &[f32],
    ) -> crate::Result<()> {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let payload_header =
            EntryHeader::ArroySetVector(ArroySetVector { docid, embedder_id, _padding: [0; 3] });
        let total_length = EntryHeader::total_set_vector_size(embedding.len());
        if total_length > capacity {
            unreachable!("entry larger that the BBQueue capacity");
        }

        // Spin loop to have a frame the size we requested.
        let mut grant = loop {
            match producer.grant(total_length) {
                Ok(grant) => break grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            }
        };

        // payload_header.serialize_into(&mut grant);
        let header_size = payload_header.header_size();
        let (header_bytes, remaining) = grant.split_at_mut(header_size);
        payload_header.serialize_into(header_bytes);
        remaining.copy_from_slice(bytemuck::cast_slice(embedding));

        // We could commit only the used memory.
        grant.commit(total_length);

        // We only send a wake up message when the channel is empty
        // so that we don't fill the channel with too many WakeUps.
        if self.sender.is_empty() {
            self.sender.send(ReceiverAction::WakeUp).unwrap();
        }

        Ok(())
    }

    fn write_key_value(&self, database: Database, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let key_length = NonZeroU16::new(key.len().try_into().unwrap()).unwrap();
        self.write_key_value_with(database, key_length, value.len(), |buffer| {
            let (key_buffer, value_buffer) = buffer.split_at_mut(key.len());
            key_buffer.copy_from_slice(key);
            value_buffer.copy_from_slice(value);
            Ok(())
        })
    }

    fn write_key_value_with<F>(
        &self,
        database: Database,
        key_length: NonZeroU16,
        value_length: usize,
        key_value_writer: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(&mut [u8]) -> crate::Result<()>,
    {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let operation = DbOperation { database, key_length: Some(key_length) };
        let payload_header = EntryHeader::DbOperation(operation);
        let total_length = EntryHeader::total_key_value_size(key_length, value_length);
        if total_length > capacity {
            unreachable!("entry larger that the BBQueue capacity");
        }

        // Spin loop to have a frame the size we requested.
        let mut grant = loop {
            match producer.grant(total_length) {
                Ok(grant) => break grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            }
        };

        let header_size = payload_header.header_size();
        let (header_bytes, remaining) = grant.split_at_mut(header_size);
        payload_header.serialize_into(header_bytes);
        key_value_writer(remaining)?;

        // We could commit only the used memory.
        grant.commit(total_length);

        // We only send a wake up message when the channel is empty
        // so that we don't fill the channel with too many WakeUps.
        if self.sender.is_empty() {
            self.sender.send(ReceiverAction::WakeUp).unwrap();
        }

        Ok(())
    }

    fn delete_entry(&self, database: Database, key: &[u8]) -> crate::Result<()> {
        let key_length = NonZeroU16::new(key.len().try_into().unwrap()).unwrap();
        self.delete_entry_with(database, key_length, |buffer| {
            buffer.copy_from_slice(key);
            Ok(())
        })
    }

    fn delete_entry_with<F>(
        &self,
        database: Database,
        key_length: NonZeroU16,
        key_writer: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(&mut [u8]) -> crate::Result<()>,
    {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        // For deletion we do not specify the key length,
        // it's in the remaining bytes.
        let operation = DbOperation { database, key_length: None };
        let payload_header = EntryHeader::DbOperation(operation);
        let total_length = EntryHeader::total_key_size(key_length);
        if total_length > capacity {
            unreachable!("entry larger that the BBQueue capacity");
        }

        // Spin loop to have a frame the size we requested.
        let mut grant = loop {
            match producer.grant(total_length) {
                Ok(grant) => break grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            }
        };

        let header_size = payload_header.header_size();
        let (header_bytes, remaining) = grant.split_at_mut(header_size);
        payload_header.serialize_into(header_bytes);
        key_writer(remaining)?;

        // We could commit only the used memory.
        grant.commit(total_length);

        // We only send a wake up message when the channel is empty
        // so that we don't fill the channel with too many WakeUps.
        if self.sender.is_empty() {
            self.sender.send(ReceiverAction::WakeUp).unwrap();
        }

        Ok(())
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

#[derive(Clone, Copy)]
pub struct WordDocidsSender<'a, 'b, D> {
    sender: &'a ExtractorBbqueueSender<'b>,
    _marker: PhantomData<D>,
}

impl<D: DatabaseType> WordDocidsSender<'_, '_, D> {
    pub fn write(&self, key: &[u8], bitmap: &RoaringBitmap) -> crate::Result<()> {
        let key_length = NonZeroU16::new(key.len().try_into().unwrap()).unwrap();
        let value_length = CboRoaringBitmapCodec::serialized_size(bitmap);
        self.sender.write_key_value_with(D::DATABASE, key_length, value_length, |buffer| {
            let (key_buffer, value_buffer) = buffer.split_at_mut(key.len());
            key_buffer.copy_from_slice(key);
            CboRoaringBitmapCodec::serialize_into_writer(bitmap, value_buffer)?;
            Ok(())
        })
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        self.sender.delete_entry(D::DATABASE, key)
    }
}

#[derive(Clone, Copy)]
pub struct FacetDocidsSender<'a, 'b> {
    sender: &'a ExtractorBbqueueSender<'b>,
}

impl FacetDocidsSender<'_, '_> {
    pub fn write(&self, key: &[u8], bitmap: &RoaringBitmap) -> crate::Result<()> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let database = Database::from(facet_kind);

        let key_length = NonZeroU16::new(key.len().try_into().unwrap()).unwrap();
        let value_length = CboRoaringBitmapCodec::serialized_size(bitmap);
        let value_length = match facet_kind {
            // We must take the facet group size into account
            // when we serialize strings and numbers.
            FacetKind::Number | FacetKind::String => value_length + 1,
            FacetKind::Null | FacetKind::Empty | FacetKind::Exists => value_length,
        };

        self.sender.write_key_value_with(database, key_length, value_length, |buffer| {
            let (key_out, value_out) = buffer.split_at_mut(key.len());
            key_out.copy_from_slice(key);

            let value_out = match facet_kind {
                // We must take the facet group size into account
                // when we serialize strings and numbers.
                FacetKind::String | FacetKind::Number => {
                    let (first, remaining) = value_out.split_first_mut().unwrap();
                    *first = 1;
                    remaining
                }
                FacetKind::Null | FacetKind::Empty | FacetKind::Exists => value_out,
            };

            CboRoaringBitmapCodec::serialize_into_writer(bitmap, value_out)?;

            Ok(())
        })
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let database = Database::from(facet_kind);
        self.sender.delete_entry(database, key)
    }
}

#[derive(Clone, Copy)]
pub struct FieldIdDocidFacetSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl FieldIdDocidFacetSender<'_, '_> {
    pub fn write_facet_string(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetStringCodec::bytes_decode(key).is_ok());
        self.0.write_key_value(Database::FieldIdDocidFacetStrings, key, value)
    }

    pub fn write_facet_f64(&self, key: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetF64Codec::bytes_decode(key).is_ok());
        self.0.write_key_value(Database::FieldIdDocidFacetF64s, key, &[])
    }

    pub fn delete_facet_string(&self, key: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetStringCodec::bytes_decode(key).is_ok());
        self.0.delete_entry(Database::FieldIdDocidFacetStrings, key)
    }

    pub fn delete_facet_f64(&self, key: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetF64Codec::bytes_decode(key).is_ok());
        self.0.delete_entry(Database::FieldIdDocidFacetF64s, key)
    }
}

#[derive(Clone, Copy)]
pub struct DocumentsSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl DocumentsSender<'_, '_> {
    /// TODO do that efficiently
    pub fn uncompressed(
        &self,
        docid: DocumentId,
        external_id: String,
        document: &KvReaderFieldId,
    ) -> crate::Result<()> {
        self.0.write_key_value(Database::Documents, &docid.to_be_bytes(), document.as_bytes())?;
        self.0.write_key_value(
            Database::ExternalDocumentsIds,
            external_id.as_bytes(),
            &docid.to_be_bytes(),
        )
    }

    pub fn delete(&self, docid: DocumentId, external_id: String) -> crate::Result<()> {
        self.0.delete_entry(Database::Documents, &docid.to_be_bytes())?;
        self.0.delete_vector(docid)?;
        self.0.delete_entry(Database::ExternalDocumentsIds, external_id.as_bytes())
    }
}

#[derive(Clone, Copy)]
pub struct EmbeddingSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl EmbeddingSender<'_, '_> {
    pub fn set_vectors(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embeddings: Vec<Embedding>,
    ) -> crate::Result<()> {
        for embedding in embeddings {
            self.set_vector(docid, embedder_id, embedding)?;
        }
        Ok(())
    }

    pub fn set_vector(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embedding: Embedding,
    ) -> crate::Result<()> {
        self.0.set_vector(docid, embedder_id, &embedding[..])
    }
}

#[derive(Clone, Copy)]
pub struct GeoSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl GeoSender<'_, '_> {
    pub fn set_rtree(&self, value: Mmap) -> StdResult<(), SendError<()>> {
        todo!("set rtree from file")
        // self.0
        //     .send(WriterOperation::DbOperation(DbOperation {
        //         database: Database::Main,
        //         entry: EntryOperation::Write(KeyValueEntry::from_large_key_value(
        //             GEO_RTREE_KEY.as_bytes(),
        //             value,
        //         )),
        //     }))
        //     .map_err(|_| SendError(()))
    }

    pub fn set_geo_faceted(&self, bitmap: &RoaringBitmap) -> StdResult<(), SendError<()>> {
        todo!("serialize directly into bbqueue (as a real roaringbitmap not a cbo)")

        // let mut buffer = Vec::new();
        // bitmap.serialize_into(&mut buffer).unwrap();

        // self.0
        //     .send(WriterOperation::DbOperation(DbOperation {
        //         database: Database::Main,
        //         entry: EntryOperation::Write(KeyValueEntry::from_small_key_value(
        //             GEO_FACETED_DOCUMENTS_IDS_KEY.as_bytes(),
        //             &buffer,
        //         )),
        //     }))
        //     .map_err(|_| SendError(()))
    }
}
