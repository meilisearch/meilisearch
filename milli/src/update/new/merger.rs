use std::fs::File;
use std::io::{self, BufWriter};

use bincode::ErrorKind;
use fst::{Set, SetBuilder};
use grenad::Merger;
use heed::types::Bytes;
use heed::{BoxedError, Database, RoTxn};
use memmap2::Mmap;
use roaring::RoaringBitmap;
use tempfile::tempfile;

use super::channel::*;
use super::{Deletion, DocumentChange, Insertion, KvReaderDelAdd, KvReaderFieldId, Update};
use super::extract::FacetKind;
use crate::update::del_add::DelAdd;
use crate::update::new::channel::MergerOperation;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{
    CboRoaringBitmapCodec, Error, GeoPoint, GlobalFieldsIdsMap, Index, InternalError, Result,
};

/// TODO We must return some infos/stats
#[tracing::instrument(level = "trace", skip_all, target = "indexing::documents", name = "merge")]
pub fn merge_grenad_entries(
    receiver: MergerReceiver,
    sender: MergerSender,
    rtxn: &RoTxn,
    index: &Index,
    mut global_fields_ids_map: GlobalFieldsIdsMap<'_>,
) -> Result<()> {
    let mut buffer = Vec::new();
    let mut documents_ids = index.documents_ids(rtxn)?;
    let mut geo_extractor = GeoExtractor::new(rtxn, index)?;

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
                    |_key| Ok(()),
                    |_key| Ok(()),
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
                    |_key| Ok(()),
                    |_key| Ok(()),
                )?;
            }
            MergerOperation::WordDocidsMerger(merger) => {
                let mut add_words_fst = SetBuilder::new(BufWriter::new(tempfile()?))?;
                let mut del_words_fst = SetBuilder::new(BufWriter::new(tempfile()?))?;
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
                        |key| add_words_fst.insert(key),
                        |key| del_words_fst.insert(key),
                    )?;
                }

                {
                    let span =
                        tracing::trace_span!(target: "indexing::documents::merge", "words_fst");
                    let _entered = span.enter();
                    // Move that into a dedicated function
                    let words_fst = index.words_fst(rtxn)?;
                    let mmap = compute_new_words_fst(add_words_fst, del_words_fst, words_fst)?;
                    sender.main().write_words_fst(mmap).unwrap();
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
                    |_key| Ok(()),
                    |_key| Ok(()),
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
                    |_key| Ok(()),
                    |_key| Ok(()),
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
                    |_key| Ok(()),
                    |_key| Ok(()),
                )?;
            }
            MergerOperation::InsertDocument { docid, document } => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "insert_document");
                let _entered = span.enter();
                documents_ids.insert(docid);
                sender.documents().uncompressed(docid, &document).unwrap();

                if let Some(geo_extractor) = geo_extractor.as_mut() {
                    let current = index.documents.remap_data_type::<Bytes>().get(rtxn, &docid)?;
                    let current: Option<&KvReaderFieldId> = current.map(Into::into);
                    let change = match current {
                        Some(current) => {
                            DocumentChange::Update(Update::create(docid, current.boxed(), document))
                        }
                        None => DocumentChange::Insertion(Insertion::create(docid, document)),
                    };
                    geo_extractor.manage_change(&mut global_fields_ids_map, &change)?;
                }
            }
            MergerOperation::DeleteDocument { docid } => {
                let span =
                    tracing::trace_span!(target: "indexing::documents::merge", "delete_document");
                let _entered = span.enter();
                if !documents_ids.remove(docid) {
                    unreachable!("Tried deleting a document that we do not know about");
                }
                sender.documents().delete(docid).unwrap();

                if let Some(geo_extractor) = geo_extractor.as_mut() {
                    let current = index.document(rtxn, docid)?;
                    let change = DocumentChange::Deletion(Deletion::create(docid, current.boxed()));
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
                merge_and_send_facet_docids(
                    merger,
                    FacetDatabases::new(index),
                    rtxn,
                    &mut buffer,
                    sender.facet_docids(),
                )?;
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

    Ok(())
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

fn compute_new_words_fst(
    add_words_fst: SetBuilder<BufWriter<File>>,
    del_words_fst: SetBuilder<BufWriter<File>>,
    words_fst: Set<std::borrow::Cow<'_, [u8]>>,
) -> Result<Mmap> {
    let add_words_fst_file = add_words_fst.into_inner()?;
    let add_words_fst_mmap = unsafe { Mmap::map(&add_words_fst_file.into_inner().unwrap())? };
    let add_words_fst = Set::new(&add_words_fst_mmap)?;

    let del_words_fst_file = del_words_fst.into_inner()?;
    let del_words_fst_mmap = unsafe { Mmap::map(&del_words_fst_file.into_inner().unwrap())? };
    let del_words_fst = Set::new(&del_words_fst_mmap)?;

    let diff = words_fst.op().add(&del_words_fst).difference();
    let stream = add_words_fst.op().add(diff).union();

    let mut words_fst = SetBuilder::new(tempfile()?)?;
    words_fst.extend_stream(stream)?;
    let words_fst_file = words_fst.into_inner()?;
    let words_fst_mmap = unsafe { Mmap::map(&words_fst_file)? };

    Ok(words_fst_mmap)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::merge")]
fn merge_and_send_docids(
    merger: Merger<File, MergeDeladdCboRoaringBitmaps>,
    database: Database<Bytes, Bytes>,
    rtxn: &RoTxn<'_>,
    buffer: &mut Vec<u8>,
    docids_sender: impl DocidsSender,
    mut add_key: impl FnMut(&[u8]) -> fst::Result<()>,
    mut del_key: impl FnMut(&[u8]) -> fst::Result<()>,
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
                add_key(key)?;
            }
            Operation::Delete => {
                docids_sender.delete(key).unwrap();
                del_key(key)?;
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
            }
            Operation::Delete => {
                docids_sender.delete(key).unwrap();
            }
            Operation::Ignore => (),
        }
    }

    Ok(())
}

struct FacetDatabases {
    /// Maps the facet field id and the docids for which this field exists
    facet_id_exists_docids: Database<Bytes, Bytes>,
    /// Maps the facet field id and the docids for which this field is set as null
    facet_id_is_null_docids: Database<Bytes, Bytes>,
    /// Maps the facet field id and the docids for which this field is considered empty
    facet_id_is_empty_docids: Database<Bytes, Bytes>,
    /// Maps the facet field id and ranges of numbers with the docids that corresponds to them.
    facet_id_f64_docids: Database<Bytes, Bytes>,
    /// Maps the facet field id and ranges of strings with the docids that corresponds to them.
    facet_id_string_docids: Database<Bytes, Bytes>,
}

impl FacetDatabases {
    fn new(index: &Index) -> Self {
        Self {
            facet_id_exists_docids: index.facet_id_exists_docids.remap_types(),
            facet_id_is_null_docids: index.facet_id_is_null_docids.remap_types(),
            facet_id_is_empty_docids: index.facet_id_is_empty_docids.remap_types(),
            facet_id_f64_docids: index.facet_id_f64_docids.remap_types(),
            facet_id_string_docids: index.facet_id_string_docids.remap_types(),
        }
    }

    fn get<'a>(&self, rtxn: &'a RoTxn<'_>, key: &[u8]) -> heed::Result<Option<&'a [u8]>> {
        let (facet_kind, key) = self.extract_facet_kind(key);
        match facet_kind {
            FacetKind::Exists => self.facet_id_exists_docids.get(rtxn, key),
            FacetKind::Null => self.facet_id_is_null_docids.get(rtxn, key),
            FacetKind::Empty => self.facet_id_is_empty_docids.get(rtxn, key),
            FacetKind::Number => self.facet_id_f64_docids.get(rtxn, key),
            FacetKind::String => self.facet_id_string_docids.get(rtxn, key),
        }
    }

    fn extract_facet_kind<'a>(&self, key: &'a [u8]) -> (FacetKind, &'a [u8]) {
        (FacetKind::from(key[0]), &key[1..])
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
                Some(add) => (current - del) | add,
                None => current - del,
            };
            if output.is_empty() {
                Ok(Operation::Delete)
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
