use crate::RankedMap;
use heed::types::{ByteSlice, OwnedType, SerdeBincode, Str};
use heed::Result as ZResult;
use meilidb_schema::Schema;
use std::sync::Arc;

const CUSTOMS_KEY: &str = "customs-key";
const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY: &str = "ranked-map";
const SCHEMA_KEY: &str = "schema";
const SYNONYMS_KEY: &str = "synonyms";
const WORDS_KEY: &str = "words";

#[derive(Copy, Clone)]
pub struct Main {
    pub(crate) main: heed::PolyDatabase,
}

impl Main {
    pub fn put_words_fst(self, writer: &mut heed::RwTxn, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main.put::<Str, ByteSlice>(writer, WORDS_KEY, bytes)
    }

    pub fn words_fst(self, reader: &heed::RoTxn) -> ZResult<Option<fst::Set>> {
        match self.main.get::<Str, ByteSlice>(reader, WORDS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::from(bytes);
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }

    pub fn put_schema(self, writer: &mut heed::RwTxn, schema: &Schema) -> ZResult<()> {
        self.main
            .put::<Str, SerdeBincode<Schema>>(writer, SCHEMA_KEY, schema)
    }

    pub fn schema(self, reader: &heed::RoTxn) -> ZResult<Option<Schema>> {
        self.main
            .get::<Str, SerdeBincode<Schema>>(reader, SCHEMA_KEY)
    }

    pub fn put_ranked_map(self, writer: &mut heed::RwTxn, ranked_map: &RankedMap) -> ZResult<()> {
        self.main
            .put::<Str, SerdeBincode<RankedMap>>(writer, RANKED_MAP_KEY, &ranked_map)
    }

    pub fn ranked_map(self, reader: &heed::RoTxn) -> ZResult<Option<RankedMap>> {
        self.main
            .get::<Str, SerdeBincode<RankedMap>>(reader, RANKED_MAP_KEY)
    }

    pub fn put_synonyms_fst(self, writer: &mut heed::RwTxn, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main.put::<Str, ByteSlice>(writer, SYNONYMS_KEY, bytes)
    }

    pub fn synonyms_fst(self, reader: &heed::RoTxn) -> ZResult<Option<fst::Set>> {
        match self.main.get::<Str, ByteSlice>(reader, SYNONYMS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::from(bytes);
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }

    pub fn put_number_of_documents<F>(self, writer: &mut heed::RwTxn, f: F) -> ZResult<u64>
    where
        F: Fn(u64) -> u64,
    {
        let new = self.number_of_documents(writer).map(f)?;
        self.main
            .put::<Str, OwnedType<u64>>(writer, NUMBER_OF_DOCUMENTS_KEY, &new)?;
        Ok(new)
    }

    pub fn number_of_documents(self, reader: &heed::RoTxn) -> ZResult<u64> {
        match self
            .main
            .get::<Str, OwnedType<u64>>(reader, NUMBER_OF_DOCUMENTS_KEY)?
        {
            Some(value) => Ok(value),
            None => Ok(0),
        }
    }

    pub fn put_customs(self, writer: &mut heed::RwTxn, customs: &[u8]) -> ZResult<()> {
        self.main
            .put::<Str, ByteSlice>(writer, CUSTOMS_KEY, customs)
    }

    pub fn customs<'txn>(self, reader: &'txn heed::RoTxn) -> ZResult<Option<&'txn [u8]>> {
        self.main.get::<Str, ByteSlice>(reader, CUSTOMS_KEY)
    }
}
