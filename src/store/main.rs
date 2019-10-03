use std::sync::Arc;
use std::convert::TryInto;

use rkv::Value;
use crate::RankedMap;

const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY:          &str = "ranked-map";
const SCHEMA_KEY:              &str = "schema";
const SYNONYMS_KEY:            &str = "synonyms";
const WORDS_KEY:               &str = "words";

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
            Some(Value::Blob(bytes)) => {
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
        let mut bytes = Vec::new();
        ranked_map.write_to_bin(&mut bytes).unwrap();
        let blob = Value::Blob(&bytes[..]);
        self.main.put(writer, RANKED_MAP_KEY, &blob)
    }

    pub fn ranked_map<T: rkv::Readable>(
        &self,
        reader: &T,
    ) -> Result<Option<RankedMap>, rkv::StoreError>
    {
        match self.main.get(reader, RANKED_MAP_KEY)? {
            Some(Value::Blob(bytes)) => {
                let ranked_map = RankedMap::read_from_bin(bytes).unwrap();
                Ok(Some(ranked_map))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }

    pub fn put_number_of_documents<F: Fn(u64) -> u64>(
        &self,
        writer: &mut rkv::Writer,
        f: F,
    ) -> Result<u64, rkv::StoreError>
    {
        let new = self.number_of_documents(writer).map(f)?;
        self.main.put(writer, NUMBER_OF_DOCUMENTS_KEY, &Value::Blob(&new.to_be_bytes()))?;
        Ok(new)
    }

    pub fn number_of_documents<T: rkv::Readable>(
        &self,
        reader: &T,
    ) -> Result<u64, rkv::StoreError>
    {
        match self.main.get(reader, NUMBER_OF_DOCUMENTS_KEY)? {
            Some(Value::Blob(bytes)) => {
                let array = bytes.try_into().unwrap();
                Ok(u64::from_be_bytes(array))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(0),
        }
    }
}
