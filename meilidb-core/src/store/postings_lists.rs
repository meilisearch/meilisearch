use std::borrow::Cow;
use std::{mem, ptr};

use zerocopy::{AsBytes, LayoutVerified};
use rkv::StoreError;

use crate::DocIndex;
use crate::store::aligned_to;

#[derive(Copy, Clone)]
pub struct PostingsLists {
    pub(crate) postings_lists: rkv::SingleStore,
}

impl PostingsLists {
    pub fn put_postings_list(
        &self,
        writer: &mut rkv::Writer,
        word: &[u8],
        words_indexes: &[DocIndex],
    ) -> Result<(), rkv::StoreError>
    {
        let blob = rkv::Value::Blob(words_indexes.as_bytes());
        self.postings_lists.put(writer, word, &blob)
    }

    pub fn del_postings_list(
        &self,
        writer: &mut rkv::Writer,
        word: &[u8],
    ) -> Result<bool, rkv::StoreError>
    {
        match self.postings_lists.delete(writer, word) {
            Ok(()) => Ok(true),
            Err(StoreError::LmdbError(lmdb::Error::NotFound)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn postings_list<'a>(
        &self,
        reader: &'a impl rkv::Readable,
        word: &[u8],
    ) -> Result<Option<Cow<'a, sdset::Set<DocIndex>>>, rkv::StoreError>
    {
        let bytes = match self.postings_lists.get(reader, word)? {
            Some(rkv::Value::Blob(bytes)) => bytes,
            Some(value) => panic!("invalid type {:?}", value),
            None => return Ok(None),
        };

        match LayoutVerified::new_slice(bytes) {
            Some(layout) => {
                let set = sdset::Set::new(layout.into_slice()).unwrap();
                Ok(Some(Cow::Borrowed(set)))
            },
            None => {
                let len = bytes.len();
                let elem_size = mem::size_of::<DocIndex>();

                // ensure that it is the alignment that is wrong
                // and the length is valid
                if len % elem_size == 0 && !aligned_to(bytes, mem::align_of::<DocIndex>()) {
                    let elems = len / elem_size;
                    let mut vec = Vec::<DocIndex>::with_capacity(elems);

                    unsafe {
                        let dst = vec.as_mut_ptr() as *mut u8;
                        ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                        vec.set_len(elems);
                    }

                    let setbuf = sdset::SetBuf::new(vec).unwrap();
                    return Ok(Some(Cow::Owned(setbuf)))
                }

                Ok(None)
            },
        }
    }
}
