use std::borrow::Cow;
use std::convert::TryInto;
use std::{mem, ptr};

use heed::{BytesDecode, BytesEncode};
use sdset::{Set, SetBuf};
use zerocopy::{AsBytes, FromBytes};

use crate::{DocumentId, DocIndex};

#[derive(Default, Debug)]
pub struct Postings<'a> {
    pub docids: Cow<'a, Set<DocumentId>>,
    pub matches: Cow<'a, Set<DocIndex>>,
}

pub struct PostingsCodec;

impl<'a> BytesEncode<'a> for PostingsCodec {
    type EItem = Postings<'a>;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let u64_size = mem::size_of::<u64>();
        let docids_size = item.docids.len() * mem::size_of::<DocumentId>();
        let matches_size = item.matches.len() * mem::size_of::<DocIndex>();

        let mut buffer = Vec::with_capacity(u64_size + docids_size + matches_size);

        let docids_len = item.docids.len() as u64;
        buffer.extend_from_slice(&docids_len.to_be_bytes());
        buffer.extend_from_slice(item.docids.as_bytes());
        buffer.extend_from_slice(item.matches.as_bytes());

        Some(Cow::Owned(buffer))
    }
}

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

fn from_bytes_to_set<'a, T: 'a>(bytes: &'a [u8]) -> Option<Cow<'a, Set<T>>>
where T: Clone + FromBytes
{
    match zerocopy::LayoutVerified::<_, [T]>::new_slice(bytes) {
        Some(layout) => Some(Cow::Borrowed(Set::new_unchecked(layout.into_slice()))),
        None => {
            let len = bytes.len();
            let elem_size = mem::size_of::<T>();

            // ensure that it is the alignment that is wrong
            // and the length is valid
            if len % elem_size == 0 && !aligned_to(bytes, mem::align_of::<T>()) {
                let elems = len / elem_size;
                let mut vec = Vec::<T>::with_capacity(elems);

                unsafe {
                    let dst = vec.as_mut_ptr() as *mut u8;
                    ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                    vec.set_len(elems);
                }

                return Some(Cow::Owned(SetBuf::new_unchecked(vec)));
            }

            None
        }
    }
}

impl<'a> BytesDecode<'a> for PostingsCodec {
    type DItem = Postings<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let u64_size = mem::size_of::<u64>();
        let docid_size = mem::size_of::<DocumentId>();

        let (len_bytes, bytes) = bytes.split_at(u64_size);
        let docids_len = len_bytes.try_into().ok().map(u64::from_be_bytes)? as usize;
        let docids_size = docids_len * docid_size;

        let docids_bytes = &bytes[..docids_size];
        let matches_bytes = &bytes[docids_size..];

        let docids = from_bytes_to_set(docids_bytes)?;
        let matches = from_bytes_to_set(matches_bytes)?;

        Some(Postings { docids, matches })
    }
}
