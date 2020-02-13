use std::sync::Arc;
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use heed::types::{ByteSlice, OwnedType, SerdeBincode, Str};
use heed::Result as ZResult;
use meilisearch_schema::Schema;

use crate::database::MainT;
use crate::RankedMap;
use crate::settings::RankingRule;

const CREATED_AT_KEY: &str = "created-at";
const RANKING_RULES_KEY: &str = "ranking-rules";
const RANKING_DISTINCT_KEY: &str = "ranking-distinct";
const STOP_WORDS_KEY: &str = "stop-words";
const SYNONYMS_KEY: &str = "synonyms";
const CUSTOMS_KEY: &str = "customs";
const FIELDS_FREQUENCY_KEY: &str = "fields-frequency";
const NAME_KEY: &str = "name";
const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY: &str = "ranked-map";
const SCHEMA_KEY: &str = "schema";
const UPDATED_AT_KEY: &str = "updated-at";
const WORDS_KEY: &str = "words";

pub type FreqsMap = HashMap<String, usize>;
type SerdeFreqsMap = SerdeBincode<FreqsMap>;
type SerdeDatetime = SerdeBincode<DateTime<Utc>>;

#[derive(Copy, Clone)]
pub struct Main {
    pub(crate) main: heed::PolyDatabase,
}

impl Main {
    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.main.clear(writer)
    }

    pub fn put_name(self, writer: &mut heed::RwTxn<MainT>, name: &str) -> ZResult<()> {
        self.main.put::<_, Str, Str>(writer, NAME_KEY, name)
    }

    pub fn name(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<String>> {
        Ok(self
            .main
            .get::<_, Str, Str>(reader, NAME_KEY)?
            .map(|name| name.to_owned()))
    }

    pub fn put_created_at(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.main
            .put::<_, Str, SerdeDatetime>(writer, CREATED_AT_KEY, &Utc::now())
    }

    pub fn created_at(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<DateTime<Utc>>> {
        self.main.get::<_, Str, SerdeDatetime>(reader, CREATED_AT_KEY)
    }

    pub fn put_updated_at(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.main
            .put::<_, Str, SerdeDatetime>(writer, UPDATED_AT_KEY, &Utc::now())
    }

    pub fn updated_at(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<DateTime<Utc>>> {
        self.main.get::<_, Str, SerdeDatetime>(reader, UPDATED_AT_KEY)
    }

    pub fn put_words_fst(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main.put::<_, Str, ByteSlice>(writer, WORDS_KEY, bytes)
    }

    pub unsafe fn static_words_fst(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<fst::Set>> {
        match self.main.get::<_, Str, ByteSlice>(reader, WORDS_KEY)? {
            Some(bytes) => {
                let bytes: &'static [u8] = std::mem::transmute(bytes);
                let set = fst::Set::from_static_slice(bytes).unwrap();
                Ok(Some(set))
            }
            None => Ok(None),
        }
    }

    pub fn words_fst(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<fst::Set>> {
        match self.main.get::<_, Str, ByteSlice>(reader, WORDS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }

    pub fn put_schema(self, writer: &mut heed::RwTxn<MainT>, schema: &Schema) -> ZResult<()> {
        self.main.put::<_, Str, SerdeBincode<Schema>>(writer, SCHEMA_KEY, schema)
    }

    pub fn schema(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<Schema>> {
        self.main.get::<_, Str, SerdeBincode<Schema>>(reader, SCHEMA_KEY)
    }

    pub fn delete_schema(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<bool> {
        self.main.delete::<_, Str>(writer, SCHEMA_KEY)
    }

    pub fn put_ranked_map(self, writer: &mut heed::RwTxn<MainT>, ranked_map: &RankedMap) -> ZResult<()> {
        self.main.put::<_, Str, SerdeBincode<RankedMap>>(writer, RANKED_MAP_KEY, &ranked_map)
    }

    pub fn ranked_map(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<RankedMap>> {
        self.main.get::<_, Str, SerdeBincode<RankedMap>>(reader, RANKED_MAP_KEY)
    }

    pub fn put_synonyms_fst(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main.put::<_, Str, ByteSlice>(writer, SYNONYMS_KEY, bytes)
    }

    pub fn synonyms_fst(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<fst::Set>> {
        match self.main.get::<_, Str, ByteSlice>(reader, SYNONYMS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }

    pub fn put_stop_words_fst(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set) -> ZResult<()> {
        let bytes = fst.as_fst().as_bytes();
        self.main.put::<_, Str, ByteSlice>(writer, STOP_WORDS_KEY, bytes)
    }

    pub fn stop_words_fst(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<fst::Set>> {
        match self.main.get::<_, Str, ByteSlice>(reader, STOP_WORDS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(Some(fst::Set::from(fst)))
            }
            None => Ok(None),
        }
    }

    pub fn put_number_of_documents<F>(self, writer: &mut heed::RwTxn<MainT>, f: F) -> ZResult<u64>
    where
        F: Fn(u64) -> u64,
    {
        let new = self.number_of_documents(&*writer).map(f)?;
        self.main
            .put::<_, Str, OwnedType<u64>>(writer, NUMBER_OF_DOCUMENTS_KEY, &new)?;
        Ok(new)
    }

    pub fn number_of_documents(self, reader: &heed::RoTxn<MainT>) -> ZResult<u64> {
        match self
            .main
            .get::<_, Str, OwnedType<u64>>(reader, NUMBER_OF_DOCUMENTS_KEY)?
        {
            Some(value) => Ok(value),
            None => Ok(0),
        }
    }

    pub fn put_fields_frequency(
        self,
        writer: &mut heed::RwTxn<MainT>,
        fields_frequency: &FreqsMap,
    ) -> ZResult<()> {
        self.main
            .put::<_, Str, SerdeFreqsMap>(writer, FIELDS_FREQUENCY_KEY, fields_frequency)
    }

    pub fn fields_frequency(&self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<FreqsMap>> {
        match self
            .main
            .get::<_, Str, SerdeFreqsMap>(reader, FIELDS_FREQUENCY_KEY)?
        {
            Some(freqs) => Ok(Some(freqs)),
            None => Ok(None),
        }
    }

    pub fn ranking_rules(&self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<Vec<RankingRule>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<RankingRule>>>(reader, RANKING_RULES_KEY)
    }

    pub fn put_ranking_rules(self, writer: &mut heed::RwTxn<MainT>, value: &[RankingRule]) -> ZResult<()> {
        self.main.put::<_, Str, SerdeBincode<Vec<RankingRule>>>(writer, RANKING_RULES_KEY, &value.to_vec())
    }

    pub fn delete_ranking_rules(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<bool> {
        self.main.delete::<_, Str>(writer, RANKING_RULES_KEY)
    }

    pub fn ranking_distinct(&self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<String>> {
        if let Some(value) = self.main.get::<_, Str, Str>(reader, RANKING_DISTINCT_KEY)? {
            return Ok(Some(value.to_owned()))
        }
        return Ok(None)
    }

    pub fn put_ranking_distinct(self, writer: &mut heed::RwTxn<MainT>, value: &str) -> ZResult<()> {
        self.main.put::<_, Str, Str>(writer, RANKING_DISTINCT_KEY, value)
    }

    pub fn delete_ranking_distinct(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<bool> {
        self.main.delete::<_, Str>(writer, RANKING_DISTINCT_KEY)
    }

    pub fn put_customs(self, writer: &mut heed::RwTxn<MainT>, customs: &[u8]) -> ZResult<()> {
        self.main
            .put::<_, Str, ByteSlice>(writer, CUSTOMS_KEY, customs)
    }

    pub fn customs<'txn>(self, reader: &'txn heed::RoTxn<MainT>) -> ZResult<Option<&'txn [u8]>> {
        self.main.get::<_, Str, ByteSlice>(reader, CUSTOMS_KEY)
    }
}
