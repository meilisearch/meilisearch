use std::sync::Arc;
use crate::store::WORDS_KEY;
use crate::RankedMap;

#[derive(Copy, Clone)]
pub struct Main {
    pub(crate) main: rkv::SingleStore,
}

impl Main {
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
    ) -> Result<Option<fst::Set>, rkv::StoreError>
    {
        match self.main.get(reader, WORDS_KEY)? {
            Some(rkv::Value::Blob(bytes)) => {
                let len = bytes.len();
                let bytes = Arc::from(bytes);
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }

    pub fn put_ranked_map(
        &self,
        writer: &mut rkv::Writer,
        ranked_map: &RankedMap,
    ) -> Result<(), rkv::StoreError>
    {
        unimplemented!()
    }

    pub fn ranked_map<T: rkv::Readable>(
        &self,
        reader: &T,
    ) -> Result<RankedMap, rkv::StoreError>
    {
        unimplemented!()
    }

    pub fn put_number_of_documents<F: Fn(u64) -> u64>(
        &self,
        writer: &mut rkv::Writer,
        func: F,
    ) -> Result<(), rkv::StoreError>
    {
        unimplemented!()
    }
}
