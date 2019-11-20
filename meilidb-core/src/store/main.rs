use std::collections::HashMap;
use chrono::{DateTime, Utc};
use crate::RankedMap;
use heed::Result as ZResult;
use heed::types::{ByteSlice, OwnedType, SerdeBincode, Str};
use meilidb_schema::Schema;
use std::sync::Arc;

const CREATED_AT: &str = "created-at";
const CUSTOMS_KEY: &str = "customs-key";
const FIELDS_FREQUENCY: &str = "fields-frequency";
const NAME: &str = "name";
const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY: &str = "ranked-map";
const SCHEMA_KEY: &str = "schema";
const STOP_WORDS_KEY: &str = "stop-words";
const SYNONYMS_KEY: &str = "synonyms";
const UPDATED_AT: &str = "updated-at";
const WORDS_KEY: &str = "words";

pub type FreqsMap = HashMap<String, usize>;
type SerdeFreqsMap = SerdeBincode<FreqsMap>;
type SerdeDatetime = SerdeBincode<DateTime<Utc>>;

#[derive(Copy, Clone)]
pub struct Main {
    pub(crate) main: heed::PolyDatabase,
}

impl Main {
    pub fn clear(self, writer: &mut heed::RwTxn) -> ZResult<()> {
        self.main.clear(writer)
    }

    pub fn name(self, reader: &heed::RoTxn) -> ZResult<Option<String>> {
        Ok(self.main.get::<Str, Str>(reader, NAME)?.map(|name| name.to_owned()))
    }

    pub fn put_name(self, writer: &mut heed::RwTxn, name: &str) -> ZResult<()> {
        self.main.put::<Str, Str>(writer, NAME, name)
    }

    pub fn created_at(self, reader: &heed::RoTxn) -> ZResult<Option<DateTime<Utc>>> {
        self.main.get::<Str, SerdeDatetime>(reader, CREATED_AT)
    }

    pub fn put_created_at(self, writer: &mut heed::RwTxn) -> ZResult<()> {
        self.main.put::<Str, SerdeDatetime>(writer, CREATED_AT, &Utc::now())
    }

    pub fn updated_at(self, reader: &heed::RoTxn) -> ZResult<Option<DateTime<Utc>>> {
        self.main.get::<Str, SerdeDatetime>(reader, UPDATED_AT)
    }

    pub fn put_updated_at(self, writer: &mut heed::RwTxn) -> ZResult<()> {
        self.main.put::<Str, SerdeDatetime>(writer, UPDATED_AT, &Utc::now())
    }

    pub fn put_words_fst(self, writer: &mut heed::RwTxn, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main.put::<Str, ByteSlice>(writer, WORDS_KEY, bytes)
    }

    pub fn words_fst(self, reader: &heed::RoTxn) -> ZResult<Option<fst::Set>> {
        match self.main.get::<Str, ByteSlice>(reader, WORDS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
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
                let bytes = Arc::new(bytes.to_owned());
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }

    pub fn put_stop_words_fst(self, writer: &mut heed::RwTxn, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main
            .put::<Str, ByteSlice>(writer, STOP_WORDS_KEY, bytes)
    }

    pub fn stop_words_fst(self, reader: &heed::RoTxn) -> ZResult<Option<fst::Set>> {
        match self.main.get::<Str, ByteSlice>(reader, STOP_WORDS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
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

    pub fn put_fields_frequency(self, writer: &mut heed::RwTxn, fields_frequency: &FreqsMap) -> ZResult<()> {
        self.main
            .put::<Str, SerdeFreqsMap>(writer, FIELDS_FREQUENCY, fields_frequency)
    }

    pub fn fields_frequency(&self, reader: &heed::RoTxn) -> ZResult<Option<FreqsMap>> {
        match self
            .main
            .get::<Str, SerdeFreqsMap>(&reader, FIELDS_FREQUENCY)?
        {
            Some(freqs) => Ok(Some(freqs)),
            None => Ok(None),
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
