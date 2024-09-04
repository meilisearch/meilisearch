use fst::set::OpBuilder;
use fst::{Set, SetBuilder};
use heed::types::Bytes;
use heed::RoTxn;
use memmap2::Mmap;
use roaring::RoaringBitmap;
use tempfile::tempfile;

use super::channel::{MergerReceiver, MergerSender};
use super::KvReaderDelAdd;
use crate::index::main_key::WORDS_FST_KEY;
use crate::update::del_add::DelAdd;
use crate::update::new::channel::MergerOperation;
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
                let word_docids_sender = sender.word_docids();
                let database = index.word_docids.remap_types::<Bytes, Bytes>();
                let mut add_words_fst = SetBuilder::new(tempfile()?)?;
                let mut del_words_fst = SetBuilder::new(tempfile()?)?;

                /// TODO manage the error correctly
                let mut merger_iter = merger.into_stream_merger_iter().unwrap();

                // TODO manage the error correctly
                while let Some((key, deladd)) = merger_iter.next().unwrap() {
                    let current = database.get(rtxn, key)?;
                    let deladd: &KvReaderDelAdd = deladd.into();
                    let del = deladd.get(DelAdd::Deletion);
                    let add = deladd.get(DelAdd::Addition);

                    match merge_cbo_bitmaps(current, del, add)? {
                        Operation::Write(bitmap) => {
                            let value = cbo_bitmap_serialize_into_vec(&bitmap, &mut buffer);
                            word_docids_sender.write(key, value).unwrap();
                            add_words_fst.insert(key)?;
                        }
                        Operation::Delete => {
                            word_docids_sender.delete(key).unwrap();
                            del_words_fst.insert(key)?;
                        }
                        Operation::Ignore => (),
                    }
                }

                // Move that into a dedicated function
                let words_fst = index.words_fst(rtxn)?;

                let add_words_fst_file = add_words_fst.into_inner()?;
                let add_words_fst_mmap = unsafe { Mmap::map(&add_words_fst_file)? };
                let add_words_fst = Set::new(&add_words_fst_mmap)?;

                let del_words_fst_file = del_words_fst.into_inner()?;
                let del_words_fst_mmap = unsafe { Mmap::map(&del_words_fst_file)? };
                let del_words_fst = Set::new(&del_words_fst_mmap)?;

                // TO BE IMPROVED @many
                let diff = words_fst.op().add(&del_words_fst).difference();
                let stream = add_words_fst.op().add(diff).union();

                let mut words_fst = SetBuilder::new(tempfile()?)?;
                words_fst.extend_stream(stream)?;
                let words_fst_file = words_fst.into_inner()?;
                let words_fst_mmap = unsafe { Mmap::map(&words_fst_file)? };

                // PLEASE SEND THIS AS AN MMAP
                let main_sender = sender.main();
                main_sender.write_words_fst(&words_fst_mmap).unwrap();
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
fn serialize_bitmap_into_vec<'b>(bitmap: &RoaringBitmap, buffer: &'b mut Vec<u8>) {
    buffer.clear();
    bitmap.serialize_into(buffer).unwrap();
    // buffer.as_slice()
}
