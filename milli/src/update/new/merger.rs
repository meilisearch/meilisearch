use std::fs::File;
use std::io::{self};

use bincode::ErrorKind;
use grenad::Merger;
use hashbrown::HashSet;
use heed::types::Bytes;
use heed::{Database, RoTxn};
use roaring::RoaringBitmap;

use super::channel::*;
use super::extract::FacetKind;
use super::word_fst_builder::{PrefixData, PrefixDelta};
use super::{Deletion, DocumentChange, KvReaderDelAdd, KvReaderFieldId};
use crate::update::del_add::DelAdd;
use crate::update::new::channel::MergerOperation;
use crate::update::new::word_fst_builder::WordFstBuilder;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{CboRoaringBitmapCodec, Error, FieldId, GeoPoint, GlobalFieldsIdsMap, Index, Result};

/// TODO We must return some infos/stats
#[tracing::instrument(level = "trace", skip_all, target = "indexing::documents", name = "merge")]
pub fn merge_grenad_entries(
    receiver: MergerReceiver,
    sender: MergerSender,
    rtxn: &RoTxn,
    index: &Index,
    global_fields_ids_map: GlobalFieldsIdsMap<'_>,
) -> Result<MergerResult> {
    let mut buffer: Vec<u8> = Vec::new();
    let mut documents_ids = index.documents_ids(rtxn)?;
    let mut geo_extractor = GeoExtractor::new(rtxn, index)?;
    let mut merger_result = MergerResult::default();

    for merger_operation in receiver {
        match merger_operation {
            MergerOperation::ExactWordDocidsMerger(merger) => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "exact_word_docids");
                let _entered = span.enter();
                merge_and_send_docids(
                    merger,
                    /// TODO do a MergerOperation::database(&Index) -> Database<Bytes, Bytes>.
                    index.exact_word_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<ExactWordDocids>(),
                    |_, _key| Ok(()),
                )?;
            }
            MergerOperation::FidWordCountDocidsMerger(merger) => {
                let span = tracing::trace_span!(target: "indexing::documents::merge", "fid_word_count_docids");
                let _entered = span.enter();
                merge_and_send_docids(
                    merger,
                    index.field_id_word_count_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<FidWordCountDocids>(),
                    |_, _key| Ok(()),
                )?;
            }
            MergerOperation::WordDocidsMerger(merger) => {
                let words_fst = index.words_fst(rtxn)?;
                let mut word_fst_builder = WordFstBuilder::new(&words_fst)?;
                let prefix_settings = index.prefix_settings(rtxn)?;
                word_fst_builder.with_prefix_settings(prefix_settings);

                {
                    let span =
                        tracing::trace_span!(target: "indexing::documents::merge", "word_docids");
                    let _entered = span.enter();

                    merge_and_send_docids(
                        merger,
                        index.word_docids.remap_types(),
                        rtxn,
                        &mut buffer,
                        sender.docids::<WordDocids>(),
                        |deladd, key| word_fst_builder.register_word(deladd, key),
                    )?;
                }

                {
                    let span =
                        tracing::trace_span!(target: "indexing::documents::merge", "words_fst");
                    let _entered = span.enter();

                    let (word_fst_mmap, prefix_data) = word_fst_builder.build(index, rtxn)?;
                    sender.main().write_words_fst(word_fst_mmap).unwrap();
                    if let Some(PrefixData { prefixes_fst_mmap, prefix_delta }) = prefix_data {
                        sender.main().write_words_prefixes_fst(prefixes_fst_mmap).unwrap();
                        merger_result.prefix_delta = Some(prefix_delta);
                    }
                }
            }
            MergerOperation::WordFidDocidsMerger(merger) => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "word_fid_docids");
                let _entered = span.enter();
                merge_and_send_docids(
                    merger,
                    index.word_fid_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<WordFidDocids>(),
                    |_, _key| Ok(()),
                )?;
            }
            MergerOperation::WordPairProximityDocidsMerger(merger) => {
                let span = tracing::trace_span!(target: "indexing::documents::merge", "word_pair_proximity_docids");
                let _entered = span.enter();
                merge_and_send_docids(
                    merger,
                    index.word_pair_proximity_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<WordPairProximityDocids>(),
                    |_, _key| Ok(()),
                )?;
            }
            MergerOperation::WordPositionDocidsMerger(merger) => {
                let span = tracing::trace_span!(target: "indexing::documents::merge", "word_position_docids");
                let _entered = span.enter();
                merge_and_send_docids(
                    merger,
                    index.word_position_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<WordPositionDocids>(),
                    |_, _key| Ok(()),
                )?;
            }
            MergerOperation::InsertDocument { docid, external_id, document } => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "insert_document");
                let _entered = span.enter();
                documents_ids.insert(docid);
                sender.documents().uncompressed(docid, external_id.clone(), &document).unwrap();

                if let Some(geo_extractor) = geo_extractor.as_mut() {
                    let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
                    let current: Option<&KvReaderFieldId> = current.map(Into::into);
                    let change = match current {
                        Some(current) => DocumentChange::Update(todo!()),
                        None => DocumentChange::Insertion(todo!()),
                    };
                    geo_extractor.manage_change(&mut global_fields_ids_map, &change)?;
                }
            }
            MergerOperation::DeleteDocument { docid, external_id } => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "delete_document");
                let _entered = span.enter();
                if !documents_ids.remove(docid) {
                    unreachable!("Tried deleting a document that we do not know about");
                }
                sender.documents().delete(docid, external_id.clone()).unwrap();

                if let Some(geo_extractor) = geo_extractor.as_mut() {
                    let change = DocumentChange::Deletion(Deletion::create(docid, todo!()));
                    geo_extractor.manage_change(&mut global_fields_ids_map, &change)?;
                }
            }
            MergerOperation::FinishedDocument => {
                // send the rtree
            }
            MergerOperation::FacetDocidsMerger(merger) => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "facet_docids");
                let _entered = span.enter();
                let mut facet_field_ids_delta = FacetFieldIdsDelta::new();
                merge_and_send_facet_docids(
                    merger,
                    FacetDatabases::new(index),
                    rtxn,
                    &mut buffer,
                    sender.facet_docids(),
                    &mut facet_field_ids_delta,
                )?;

                merger_result.facet_field_ids_delta = Some(facet_field_ids_delta);
            }
        }
    }

    {
        let span = tracing::trace_span!(target: "indexing::documents::merge", "documents_ids");
        let _entered = span.enter();

        // Send the documents ids unionized with the current one
        /// TODO return the slice of bytes directly
        serialize_bitmap_into_vec(&documents_ids, &mut buffer);
        sender.send_documents_ids(&buffer).unwrap();
    }

    // ...

    Ok(merger_result)
}

#[derive(Default, Debug)]
pub struct MergerResult {
    /// The delta of the prefixes
    pub prefix_delta: Option<PrefixDelta>,
    /// The field ids that have been modified
    pub facet_field_ids_delta: Option<FacetFieldIdsDelta>,
}

pub struct GeoExtractor {
    rtree: Option<rstar::RTree<GeoPoint>>,
}

impl GeoExtractor {
    pub fn new(rtxn: &RoTxn, index: &Index) -> Result<Option<Self>> {
        let is_sortable = index.sortable_fields(rtxn)?.contains("_geo");
        let is_filterable = index.filterable_fields(rtxn)?.contains("_geo");
        if is_sortable || is_filterable {
            Ok(Some(GeoExtractor { rtree: index.geo_rtree(rtxn)? }))
        } else {
            Ok(None)
        }
    }

    pub fn manage_change(
        &mut self,
        fidmap: &mut GlobalFieldsIdsMap,
        change: &DocumentChange,
    ) -> Result<()> {
        match change {
            DocumentChange::Deletion(_) => todo!(),
            DocumentChange::Update(_) => todo!(),
            DocumentChange::Insertion(_) => todo!(),
        }
    }

    pub fn serialize_rtree<W: io::Write>(self, writer: &mut W) -> Result<bool> {
        match self.rtree {
            Some(rtree) => {
                // TODO What should I do?
                bincode::serialize_into(writer, &rtree).map(|_| true).map_err(|e| match *e {
                    ErrorKind::Io(e) => Error::IoError(e),
                    ErrorKind::InvalidUtf8Encoding(_) => todo!(),
                    ErrorKind::InvalidBoolEncoding(_) => todo!(),
                    ErrorKind::InvalidCharEncoding => todo!(),
                    ErrorKind::InvalidTagEncoding(_) => todo!(),
                    ErrorKind::DeserializeAnyNotSupported => todo!(),
                    ErrorKind::SizeLimit => todo!(),
                    ErrorKind::SequenceMustHaveLength => todo!(),
                    ErrorKind::Custom(_) => todo!(),
                })
            }
            None => Ok(false),
        }
    }
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
fn merge_and_send_docids(
    merger: Merger<File, MergeDeladdCboRoaringBitmaps>,
    database: Database<Bytes, Bytes>,
    rtxn: &RoTxn<'_>,
    buffer: &mut Vec<u8>,
    docids_sender: impl DocidsSender,
    mut register_key: impl FnMut(DelAdd, &[u8]) -> Result<()>,
) -> Result<()> {
    let mut merger_iter = merger.into_stream_merger_iter().unwrap();
    while let Some((key, deladd)) = merger_iter.next().unwrap() {
        let current = database.get(rtxn, key)?;
        let deladd: &KvReaderDelAdd = deladd.into();
        let del = deladd.get(DelAdd::Deletion);
        let add = deladd.get(DelAdd::Addition);

        match merge_cbo_bitmaps(current, del, add)? {
            Operation::Write(bitmap) => {
                let value = cbo_bitmap_serialize_into_vec(&bitmap, buffer);
                docids_sender.write(key, value).unwrap();
                register_key(DelAdd::Addition, key)?;
            }
            Operation::Delete => {
                docids_sender.delete(key).unwrap();
                register_key(DelAdd::Deletion, key)?;
            }
            Operation::Ignore => (),
        }
    }

    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
fn merge_and_send_facet_docids(
    merger: Merger<File, MergeDeladdCboRoaringBitmaps>,
    database: FacetDatabases,
    rtxn: &RoTxn<'_>,
    buffer: &mut Vec<u8>,
    docids_sender: impl DocidsSender,
    facet_field_ids_delta: &mut FacetFieldIdsDelta,
) -> Result<()> {
    let mut merger_iter = merger.into_stream_merger_iter().unwrap();
    while let Some((key, deladd)) = merger_iter.next().unwrap() {
        let current = database.get_cbo_roaring_bytes_value(rtxn, key)?;
        let deladd: &KvReaderDelAdd = deladd.into();
        let del = deladd.get(DelAdd::Deletion);
        let add = deladd.get(DelAdd::Addition);

        match merge_cbo_bitmaps(current, del, add)? {
            Operation::Write(bitmap) => {
                facet_field_ids_delta.register_from_key(key);
                let value = cbo_bitmap_serialize_into_vec(&bitmap, buffer);
                docids_sender.write(key, value).unwrap();
            }
            Operation::Delete => {
                facet_field_ids_delta.register_from_key(key);
                docids_sender.delete(key).unwrap();
            }
            Operation::Ignore => (),
        }
    }

    Ok(())
}

struct FacetDatabases<'a> {
    index: &'a Index,
}

impl<'a> FacetDatabases<'a> {
    fn new(index: &'a Index) -> Self {
        Self { index }
    }

    fn get_cbo_roaring_bytes_value<'t>(
        &self,
        rtxn: &'t RoTxn<'_>,
        key: &[u8],
    ) -> heed::Result<Option<&'t [u8]>> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);

        let value =
            super::channel::Database::from(facet_kind).database(self.index).get(rtxn, key)?;
        match facet_kind {
            // skip level group size
            FacetKind::String | FacetKind::Number => Ok(value.map(|v| &v[1..])),
            _ => Ok(value),
        }
    }
}

#[derive(Debug)]
pub struct FacetFieldIdsDelta {
    /// The field ids that have been modified
    modified_facet_string_ids: HashSet<FieldId>,
    modified_facet_number_ids: HashSet<FieldId>,
}

impl FacetFieldIdsDelta {
    fn new() -> Self {
        Self {
            modified_facet_string_ids: HashSet::new(),
            modified_facet_number_ids: HashSet::new(),
        }
    }

    fn register_facet_string_id(&mut self, field_id: FieldId) {
        self.modified_facet_string_ids.insert(field_id);
    }

    fn register_facet_number_id(&mut self, field_id: FieldId) {
        self.modified_facet_number_ids.insert(field_id);
    }

    fn register_from_key(&mut self, key: &[u8]) {
        let (facet_kind, field_id) = self.extract_key_data(key);
        match facet_kind {
            FacetKind::Number => self.register_facet_number_id(field_id),
            FacetKind::String => self.register_facet_string_id(field_id),
            _ => (),
        }
    }

    fn extract_key_data(&self, key: &[u8]) -> (FacetKind, FieldId) {
        let facet_kind = FacetKind::from(key[0]);
        let field_id = FieldId::from_be_bytes([key[1], key[2]]);
        (facet_kind, field_id)
    }

    pub fn modified_facet_string_ids(&self) -> Option<Vec<FieldId>> {
        if self.modified_facet_string_ids.is_empty() {
            None
        } else {
            Some(self.modified_facet_string_ids.iter().copied().collect())
        }
    }

    pub fn modified_facet_number_ids(&self) -> Option<Vec<FieldId>> {
        if self.modified_facet_number_ids.is_empty() {
            None
        } else {
            Some(self.modified_facet_number_ids.iter().copied().collect())
        }
    }
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
    let current = current.map(CboRoaringBitmapCodec::deserialize_from).transpose()?;
    let del = del.map(CboRoaringBitmapCodec::deserialize_from).transpose()?;
    let add = add.map(CboRoaringBitmapCodec::deserialize_from).transpose()?;

    match (current, del, add) {
        (None, None, None) => Ok(Operation::Ignore), // but it's strange
        (None, None, Some(add)) => Ok(Operation::Write(add)),
        (None, Some(_del), None) => Ok(Operation::Ignore), // but it's strange
        (None, Some(_del), Some(add)) => Ok(Operation::Write(add)),
        (Some(_current), None, None) => Ok(Operation::Ignore), // but it's strange
        (Some(current), None, Some(add)) => Ok(Operation::Write(current | add)),
        (Some(current), Some(del), add) => {
            let output = match add {
                Some(add) => (&current - del) | add,
                None => &current - del,
            };
            if output.is_empty() {
                Ok(Operation::Delete)
            } else if current == output {
                Ok(Operation::Ignore)
            } else {
                Ok(Operation::Write(output))
            }
        }
    }
}

/// TODO Return the slice directly from the serialize_into method
fn cbo_bitmap_serialize_into_vec<'b>(bitmap: &RoaringBitmap, buffer: &'b mut Vec<u8>) -> &'b [u8] {
    buffer.clear();
    CboRoaringBitmapCodec::serialize_into(bitmap, buffer);
    buffer.as_slice()
}

/// TODO Return the slice directly from the serialize_into method
fn serialize_bitmap_into_vec(bitmap: &RoaringBitmap, buffer: &mut Vec<u8>) {
    buffer.clear();
    bitmap.serialize_into(buffer).unwrap();
    // buffer.as_slice()
}
