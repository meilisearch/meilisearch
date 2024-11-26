use std::cell::RefCell;
use std::marker::PhantomData;
use std::num::NonZeroU16;
use std::{mem, slice};

use bbqueue::framed::{FrameGrantR, FrameProducer};
use bytemuck::{NoUninit, CheckedBitPattern};
use crossbeam::sync::{Parker, Unparker};
use crossbeam_channel::{IntoIter, Receiver, SendError};
use heed::types::Bytes;
use heed::BytesDecode;
use memmap2::Mmap;
use roaring::RoaringBitmap;

use super::extract::FacetKind;
use super::ref_cell_ext::RefCellExt;
use super::thread_local::{FullySend, ThreadLocal};
use super::StdResult;
use crate::heed_codec::facet::{FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec};
use crate::index::main_key::{GEO_FACETED_DOCUMENTS_IDS_KEY, GEO_RTREE_KEY};
use crate::index::{db_name, IndexEmbeddingConfig};
use crate::update::new::KvReaderFieldId;
use crate::vector::Embedding;
use crate::{CboRoaringBitmapCodec, DocumentId, Index};

/// Creates a tuple of producer/receivers to be used by
/// the extractors and the writer loop.
///
/// # Safety
///
/// Panics if the number of provided bbqueue is not exactly equal
/// to the number of available threads in the rayon threadpool.
pub fn extractor_writer_bbqueue(
    bbbuffers: &[bbqueue::BBBuffer],
) -> (ExtractorBbqueueSender, WriterBbqueueReceiver) {
    assert_eq!(
        bbbuffers.len(),
        rayon::current_num_threads(),
        "You must provide as many BBBuffer as the available number of threads to extract"
    );

    let capacity = bbbuffers.first().unwrap().capacity();
    let parker = Parker::new();
    let extractors = ThreadLocal::with_capacity(bbbuffers.len());
    let producers = rayon::broadcast(|bi| {
        let bbqueue = &bbbuffers[bi.index()];
        let (producer, consumer) = bbqueue.try_split_framed().unwrap();
        extractors.get_or(|| FullySend(RefCell::new(producer)));
        consumer
    });

    (
        ExtractorBbqueueSender {
            inner: extractors,
            capacity: capacity.checked_sub(9).unwrap(),
            unparker: parker.unparker().clone(),
        },
        WriterBbqueueReceiver { inner: producers, parker },
    )
}

pub struct WriterBbqueueReceiver<'a> {
    inner: Vec<bbqueue::framed::FrameConsumer<'a>>,
    /// Used to park when no more work is required
    parker: Parker,
}

impl<'a> WriterBbqueueReceiver<'a> {
    pub fn read(&mut self) -> Option<FrameWithHeader<'a>> {
        loop {
            for consumer in &mut self.inner {
                // mark the frame as auto release
                if let Some() = consumer.read()
            }
            break None;
        }
    }
}

struct FrameWithHeader<'a> {
    header: EntryHeader,
    frame: FrameGrantR<'a>,
}

#[derive(Debug, Clone, Copy, CheckedBitPattern)]
#[repr(u8)]
enum EntryHeader {
    /// Wether a put of the key/value pair or a delete of the given key.
    DbOperation {
        /// The database on which to perform the operation.
        database: Database,
        /// The key length in the buffer.
        ///
        /// If None it means that the buffer is dedicated
        /// to the key and it is therefore a deletion operation.
        key_length: Option<NonZeroU16>,
    },
    ArroyDeleteVector {
        docid: DocumentId,
    },
    /// The embedding is the remaining space and represents a non-aligned [f32].
    ArroySetVector {
        docid: DocumentId,
        embedder_id: u8,
    },
}

impl EntryHeader {
    fn delete_key_size(key_length: u16) -> usize {
        mem::size_of::<Self>() + key_length as usize
    }

    fn put_key_value_size(key_length: u16, value_length: usize) -> usize {
        mem::size_of::<Self>() + key_length as usize + value_length
    }

    fn bytes_of(&self) -> &[u8] {
        /// TODO do the variant matching ourselves
        todo!()
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(u32)]
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

pub struct ExtractorBbqueueSender<'a> {
    inner: ThreadLocal<FullySend<RefCell<FrameProducer<'a>>>>,
    /// The capacity of this frame producer, will never be able to store more than that.
    ///
    /// Note that the FrameProducer requires up to 9 bytes to encode the length,
    /// the capacity has been shrinked accordingly.
    ///
    /// <https://docs.rs/bbqueue/latest/bbqueue/framed/index.html#frame-header>
    capacity: usize,
    /// Used to wake up the receiver thread,
    /// Used everytime we write something in the producer.
    unparker: Unparker,
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

    fn send_delete_vector(&self, docid: DocumentId) -> crate::Result<()> {
        match self
            .sender
            .send(WriterOperation::ArroyOperation(ArroyOperation::DeleteVectors { docid }))
        {
            Ok(()) => Ok(()),
            Err(SendError(_)) => Err(SendError(())),
        }
    }

    fn write_key_value(&self, database: Database, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let capacity = self.capacity;
        let refcell = self.inner.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let key_length = key.len().try_into().unwrap();
        let value_length = value.len();
        let total_length = EntryHeader::put_key_value_size(key_length, value_length);
        if total_length > capacity {
            unreachable!("entry larger that the bbqueue capacity");
        }

        let payload_header =
            EntryHeader::DbOperation { database, key_length: NonZeroU16::new(key_length) };

        loop {
            let mut grant = match producer.grant(total_length) {
                Ok(grant) => grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            };

            let (header, remaining) = grant.split_at_mut(mem::size_of::<EntryHeader>());
            header.copy_from_slice(payload_header.bytes_of());
            let (key_out, value_out) = remaining.split_at_mut(key.len());
            key_out.copy_from_slice(key);
            value_out.copy_from_slice(value);

            // We could commit only the used memory.
            grant.commit(total_length);

            break Ok(());
        }
    }

    fn delete_entry(&self, database: Database, key: &[u8]) -> crate::Result<()> {
        let capacity = self.capacity;
        let refcell = self.inner.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let key_length = key.len().try_into().unwrap();
        let total_length = EntryHeader::delete_key_size(key_length);
        if total_length > capacity {
            unreachable!("entry larger that the bbqueue capacity");
        }

        let payload_header = EntryHeader::DbOperation { database, key_length: None };

        loop {
            let mut grant = match producer.grant(total_length) {
                Ok(grant) => grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            };

            let (header, remaining) = grant.split_at_mut(mem::size_of::<EntryHeader>());
            header.copy_from_slice(payload_header.bytes_of());
            remaining.copy_from_slice(key);

            // We could commit only the used memory.
            grant.commit(total_length);

            break Ok(());
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

pub struct WordDocidsSender<'a, 'b, D> {
    sender: &'a ExtractorBbqueueSender<'b>,
    _marker: PhantomData<D>,
}

impl<D: DatabaseType> WordDocidsSender<'_, '_, D> {
    pub fn write(&self, key: &[u8], bitmap: &RoaringBitmap) -> crate::Result<()> {
        let capacity = self.sender.capacity;
        let refcell = self.sender.inner.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let key_length = key.len().try_into().unwrap();
        let value_length = CboRoaringBitmapCodec::serialized_size(bitmap);

        let total_length = EntryHeader::put_key_value_size(key_length, value_length);
        if total_length > capacity {
            unreachable!("entry larger that the bbqueue capacity");
        }

        let payload_header = EntryHeader::DbOperation {
            database: D::DATABASE,
            key_length: NonZeroU16::new(key_length),
        };

        loop {
            let mut grant = match producer.grant(total_length) {
                Ok(grant) => grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            };

            let (header, remaining) = grant.split_at_mut(mem::size_of::<EntryHeader>());
            header.copy_from_slice(payload_header.bytes_of());
            let (key_out, value_out) = remaining.split_at_mut(key.len());
            key_out.copy_from_slice(key);
            CboRoaringBitmapCodec::serialize_into_writer(bitmap, value_out)?;

            // We could commit only the used memory.
            grant.commit(total_length);

            break Ok(());
        }
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        let capacity = self.sender.capacity;
        let refcell = self.sender.inner.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let key_length = key.len().try_into().unwrap();
        let total_length = EntryHeader::delete_key_size(key_length);
        if total_length > capacity {
            unreachable!("entry larger that the bbqueue capacity");
        }

        let payload_header = EntryHeader::DbOperation { database: D::DATABASE, key_length: None };

        loop {
            let mut grant = match producer.grant(total_length) {
                Ok(grant) => grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            };

            let (header, remaining) = grant.split_at_mut(mem::size_of::<EntryHeader>());
            header.copy_from_slice(payload_header.bytes_of());
            remaining.copy_from_slice(key);

            // We could commit only the used memory.
            grant.commit(total_length);

            break Ok(());
        }
    }
}

pub struct FacetDocidsSender<'a, 'b> {
    sender: &'a ExtractorBbqueueSender<'b>,
}

impl FacetDocidsSender<'_, '_> {
    pub fn write(&self, key: &[u8], bitmap: &RoaringBitmap) -> crate::Result<()> {
        let capacity = self.sender.capacity;
        let refcell = self.sender.inner.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let key_length = key.len().try_into().unwrap();

        let value_length = CboRoaringBitmapCodec::serialized_size(bitmap);
        let value_length = match facet_kind {
            // We must take the facet group size into account
            // when we serialize strings and numbers.
            FacetKind::Number | FacetKind::String => value_length + 1,
            FacetKind::Null | FacetKind::Empty | FacetKind::Exists => value_length,
        };

        let total_length = EntryHeader::put_key_value_size(key_length, value_length);
        if total_length > capacity {
            unreachable!("entry larger that the bbqueue capacity");
        }

        let payload_header = EntryHeader::DbOperation {
            database: Database::from(facet_kind),
            key_length: NonZeroU16::new(key_length),
        };

        loop {
            let mut grant = match producer.grant(total_length) {
                Ok(grant) => grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            };

            let (header, remaining) = grant.split_at_mut(mem::size_of::<EntryHeader>());
            header.copy_from_slice(payload_header.bytes_of());
            let (key_out, value_out) = remaining.split_at_mut(key.len());
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

            // We could commit only the used memory.
            grant.commit(total_length);

            break Ok(());
        }
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        let capacity = self.sender.capacity;
        let refcell = self.sender.inner.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let key_length = key.len().try_into().unwrap();

        let total_length = EntryHeader::delete_key_size(key_length);
        if total_length > capacity {
            unreachable!("entry larger that the bbqueue capacity");
        }

        let payload_header =
            EntryHeader::DbOperation { database: Database::from(facet_kind), key_length: None };

        loop {
            let mut grant = match producer.grant(total_length) {
                Ok(grant) => grant,
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            };

            let (header, remaining) = grant.split_at_mut(mem::size_of::<EntryHeader>());
            header.copy_from_slice(payload_header.bytes_of());
            remaining.copy_from_slice(key);

            // We could commit only the used memory.
            grant.commit(total_length);

            break Ok(());
        }
    }
}

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
        self.0.send_delete_vector(docid)?;
        self.0.delete_entry(Database::ExternalDocumentsIds, external_id.as_bytes())
    }
}

pub struct EmbeddingSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl EmbeddingSender<'_, '_> {
    pub fn set_vectors(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embeddings: Vec<Embedding>,
    ) -> crate::Result<()> {
        self.0
            .send(WriterOperation::ArroyOperation(ArroyOperation::SetVectors {
                docid,
                embedder_id,
                embeddings,
            }))
            .map_err(|_| SendError(()))
    }

    pub fn set_vector(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embedding: Embedding,
    ) -> StdResult<(), SendError<()>> {
        self.0
            .send(WriterOperation::ArroyOperation(ArroyOperation::SetVector {
                docid,
                embedder_id,
                embedding,
            }))
            .map_err(|_| SendError(()))
    }

    /// Marks all embedders as "to be built"
    pub fn finish(self, configs: Vec<IndexEmbeddingConfig>) -> StdResult<(), SendError<()>> {
        self.0
            .send(WriterOperation::ArroyOperation(ArroyOperation::Finish { configs }))
            .map_err(|_| SendError(()))
    }
}

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
