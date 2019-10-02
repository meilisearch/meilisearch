use std::borrow::Cow;
use std::sync::Arc;
use std::{mem, ptr};
use zerocopy::{AsBytes, LayoutVerified};

use crate::DocIndex;
use crate::store::aligned_to;
use crate::store::WORDS_KEY;

pub struct Words {
    pub(crate) main: rkv::SingleStore,
    pub(crate) words_indexes: rkv::SingleStore,
}

impl Words {
    pub fn put_words_fst(
        &self,
        writer: &mut rkv::Writer,
        fst: &fst::Set,
    ) -> Result<(), rkv::StoreError>
    {
        let blob = rkv::Value::Blob(fst.as_fst().as_bytes());
        self.main.put(writer, WORDS_KEY, &blob)
    }

    pub fn words_fst<T: rkv::Readable>(
        &self,
        reader: &T,
    ) -> Result<fst::Set, rkv::StoreError>
    {
        match self.main.get(reader, WORDS_KEY)? {
            Some(rkv::Value::Blob(bytes)) => {
                let len = bytes.len();
                let bytes = Arc::from(bytes);
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(fst::Set::from(fst))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => panic!("could not find word index"),
        }
    }

    pub fn put_words_indexes(
        &self,
        writer: &mut rkv::Writer,
        word: &[u8],
        words_indexes: &[DocIndex],
    ) -> Result<(), rkv::StoreError>
    {
        let blob = rkv::Value::Blob(words_indexes.as_bytes());
        self.main.put(writer, word, &blob)
    }

    pub fn word_indexes<'a, T: rkv::Readable>(
        &self,
        reader: &'a T,
        word: &[u8],
    ) -> Result<Option<Cow<'a, [DocIndex]>>, rkv::StoreError>
    {
        let bytes = match self.main.get(reader, word)? {
            Some(rkv::Value::Blob(bytes)) => bytes,
            Some(value) => panic!("invalid type {:?}", value),
            None => return Ok(None),
        };

        match LayoutVerified::new_slice(bytes) {
            Some(layout) => Ok(Some(Cow::Borrowed(layout.into_slice()))),
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

                    return Ok(Some(Cow::Owned(vec)))
                }

                Ok(None)
            },
        }
    }
}
