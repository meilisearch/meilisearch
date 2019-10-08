use std::sync::Arc;
use crate::error::MResult;

#[derive(Copy, Clone)]
pub struct Synonyms {
    pub(crate) synonyms: rkv::SingleStore,
}

impl Synonyms {
    pub fn put_synonyms(
        &self,
        writer: &mut rkv::Writer,
        word: &[u8],
        synonyms: &fst::Set,
    ) -> Result<(), rkv::StoreError>
    {
        let blob = rkv::Value::Blob(synonyms.as_fst().as_bytes());
        self.synonyms.put(writer, word, &blob)
    }

    pub fn synonyms(
        &self,
        reader: &impl rkv::Readable,
        word: &[u8],
    ) -> MResult<Option<fst::Set>>
    {
        match self.synonyms.get(reader, word)? {
            Some(rkv::Value::Blob(bytes)) => {
                let len = bytes.len();
                let bytes = Arc::from(bytes);
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }
}
