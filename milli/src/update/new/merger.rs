use std::fs::File;

use fst::{Set, SetBuilder};
use grenad::Merger;
use heed::types::Bytes;
use heed::{Database, RoTxn};
use memmap2::Mmap;
use roaring::RoaringBitmap;
use tempfile::tempfile;

use super::channel::{
    DatabaseType, DocidsSender, ExactWordDocids, MergerReceiver, MergerSender, WordDocids,
    WordFidDocids, WordPositionDocids,
};
use super::KvReaderDelAdd;
use crate::update::del_add::DelAdd;
use crate::update::new::channel::MergerOperation;
use crate::update::MergeDeladdCboRoaringBitmaps;
use crate::{CboRoaringBitmapCodec, Index, Result};

/// TODO We must return some infos/stats
pub fn merge_grenad_entries(
    receiver: MergerReceiver,
    sender: MergerSender,
    rtxn: &RoTxn,
    index: &Index,
) -> Result<()> {
    let mut buffer = Vec::new();
    let mut documents_ids = index.documents_ids(rtxn)?;

    for merger_operation in receiver {
        match merger_operation {
            MergerOperation::WordDocidsMerger(merger) => {
                let mut add_words_fst = SetBuilder::new(tempfile()?)?;
                let mut del_words_fst = SetBuilder::new(tempfile()?)?;

                merge_and_send_docids(
                    merger,
                    index.word_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<WordDocids>(),
                    |key| add_words_fst.insert(key),
                    |key| del_words_fst.insert(key),
                )?;

                // Move that into a dedicated function
                let words_fst = index.words_fst(rtxn)?;
                let mmap = compute_new_words_fst(add_words_fst, del_words_fst, words_fst)?;
                sender.main().write_words_fst(mmap).unwrap();
            }
            MergerOperation::ExactWordDocidsMerger(merger) => {
                merge_and_send_docids(
                    merger,
                    index.exact_word_docids.remap_types(),
                    rtxn,
                    &mut buffer,
                    sender.docids::<ExactWordDocids>(),
                    |_key| Ok(()),
                    |_key| Ok(()),
                )?;
            }
            MergerOperation::WordFidDocidsMerger(merger) => {
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
            MergerOperation::WordPositionDocidsMerger(merger) => {
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
                documents_ids.insert(docid);
                sender.documents().uncompressed(docid, &document).unwrap();
            }
            MergerOperation::DeleteDocument { docid } => {
                if !documents_ids.remove(docid) {
                    unreachable!("Tried deleting a document that we do not know about");
                }
                sender.documents().delete(docid).unwrap();
            }
        }
    }

    // Send the documents ids unionized with the current one
    /// TODO return the slice of bytes directly
    serialize_bitmap_into_vec(&documents_ids, &mut buffer);
    sender.send_documents_ids(&buffer).unwrap();

    // ...

    Ok(())
}

fn compute_new_words_fst(
    add_words_fst: SetBuilder<File>,
    del_words_fst: SetBuilder<File>,
    words_fst: Set<std::borrow::Cow<'_, [u8]>>,
) -> Result<Mmap> {
    let add_words_fst_file = add_words_fst.into_inner()?;
    let add_words_fst_mmap = unsafe { Mmap::map(&add_words_fst_file)? };
    let add_words_fst = Set::new(&add_words_fst_mmap)?;

    let del_words_fst_file = del_words_fst.into_inner()?;
    let del_words_fst_mmap = unsafe { Mmap::map(&del_words_fst_file)? };
    let del_words_fst = Set::new(&del_words_fst_mmap)?;

    let diff = words_fst.op().add(&del_words_fst).difference();
    let stream = add_words_fst.op().add(diff).union();

    let mut words_fst = SetBuilder::new(tempfile()?)?;
    words_fst.extend_stream(stream)?;
    let words_fst_file = words_fst.into_inner()?;
    let words_fst_mmap = unsafe { Mmap::map(&words_fst_file)? };

    Ok(words_fst_mmap)
}

fn merge_and_send_docids<D: DatabaseType>(
    merger: Merger<File, MergeDeladdCboRoaringBitmaps>,
    database: Database<Bytes, Bytes>,
    rtxn: &RoTxn<'_>,
    buffer: &mut Vec<u8>,
    word_docids_sender: DocidsSender<'_, D>,
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
                word_docids_sender.write(key, value).unwrap();
                add_key(key)?;
            }
            Operation::Delete => {
                word_docids_sender.delete(key).unwrap();
                del_key(key)?;
            }
            Operation::Ignore => (),
        }
    }

    Ok(())
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
