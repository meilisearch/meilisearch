use heed::types::Bytes;
use heed::RoTxn;
use roaring::RoaringBitmap;

use super::channel::{MergerReceiver, MergerSender};
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

    for merger_operation in receiver {
        match merger_operation {
            MergerOperation::WordDocidsCursors(cursors) => {
                let sender = sender.word_docids();
                let database = index.word_docids.remap_types::<Bytes, Bytes>();

                let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
                builder.extend(cursors);
                /// TODO manage the error correctly
                let mut merger_iter = builder.build().into_stream_merger_iter().unwrap();

                // TODO manage the error correctly
                while let Some((key, deladd)) = merger_iter.next().unwrap() {
                    let current = database.get(rtxn, key)?;
                    let deladd: &KvReaderDelAdd = deladd.into();
                    let del = deladd.get(DelAdd::Deletion);
                    let add = deladd.get(DelAdd::Addition);

                    match merge_cbo_bitmaps(current, del, add)? {
                        Operation::Write(bitmap) => {
                            let value = cbo_serialize_into_vec(&bitmap, &mut buffer);
                            sender.write(key, value).unwrap();
                        }
                        Operation::Delete => sender.delete(key).unwrap(),
                        Operation::Ignore => (),
                    }
                }
            }
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

/// Return the slice directly from the serialize_into method
fn cbo_serialize_into_vec<'b>(bitmap: &RoaringBitmap, buffer: &'b mut Vec<u8>) -> &'b [u8] {
    buffer.clear();
    CboRoaringBitmapCodec::serialize_into(bitmap, buffer);
    buffer.as_slice()
}
