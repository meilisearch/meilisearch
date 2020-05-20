use std::borrow::Cow;
use std::sync::Arc;
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use heed::Result as ZResult;
use heed::types::{ByteSlice, OwnedType, SerdeBincode, Str};
use meilisearch_schema::{FieldId, Schema};
use meilisearch_types::DocumentId;
use sdset::Set;

use crate::database::MainT;
use crate::RankedMap;
use crate::settings::RankingRule;
use super::{CowSet, DocumentsIds};

const ATTRIBUTES_FOR_FACETING_KEY: &str = "attributes-for-faceting";
const CREATED_AT_KEY: &str = "created-at";
const CUSTOMS_KEY: &str = "customs";
const DISTINCT_ATTRIBUTE_KEY: &str = "distinct-attribute";
const EXTERNAL_DOCIDS_KEY: &str = "external-docids";
const FIELDS_FREQUENCY_KEY: &str = "fields-frequency";
const INTERNAL_DOCIDS_KEY: &str = "internal-docids";
const NAME_KEY: &str = "name";
const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY: &str = "ranked-map";
const RANKING_RULES_KEY: &str = "ranking-rules";
const SCHEMA_KEY: &str = "schema";
const STOP_WORDS_KEY: &str = "stop-words";
const SYNONYMS_KEY: &str = "synonyms";
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

    pub fn put_internal_docids(self, writer: &mut heed::RwTxn<MainT>, ids: &sdset::Set<DocumentId>) -> ZResult<()> {
        self.main.put::<_, Str, DocumentsIds>(writer, INTERNAL_DOCIDS_KEY, ids)
    }

    pub fn internal_docids<'txn>(self, reader: &'txn heed::RoTxn<MainT>) -> ZResult<Cow<'txn, sdset::Set<DocumentId>>> {
        match self.main.get::<_, Str, DocumentsIds>(reader, INTERNAL_DOCIDS_KEY)? {
            Some(ids) => Ok(ids),
            None => Ok(Cow::default()),
        }
    }

    pub fn merge_internal_docids(self, writer: &mut heed::RwTxn<MainT>, new_ids: &sdset::Set<DocumentId>) -> ZResult<()> {
        use sdset::SetOperation;

        // We do an union of the old and new internal ids.
        let internal_docids = self.internal_docids(writer)?;
        let internal_docids = sdset::duo::Union::new(&internal_docids, new_ids).into_set_buf();
        self.put_internal_docids(writer, &internal_docids)
    }

    pub fn remove_internal_docids(self, writer: &mut heed::RwTxn<MainT>, ids: &sdset::Set<DocumentId>) -> ZResult<()> {
        use sdset::SetOperation;

        // We do a difference of the old and new internal ids.
        let internal_docids = self.internal_docids(writer)?;
        let internal_docids = sdset::duo::Difference::new(&internal_docids, ids).into_set_buf();
        self.put_internal_docids(writer, &internal_docids)
    }

    pub fn put_external_docids(self, writer: &mut heed::RwTxn<MainT>, ids: &fst::Map) -> ZResult<()> {
        self.main.put::<_, Str, ByteSlice>(writer, EXTERNAL_DOCIDS_KEY, ids.as_fst().as_bytes())
    }

    pub fn merge_external_docids(self, writer: &mut heed::RwTxn<MainT>, new_docids: &fst::Map) -> ZResult<()> {
        use fst::{Streamer, IntoStreamer};

        // Do an union of the old and the new set of external docids.
        let external_docids = self.external_docids(writer)?;
        let mut op = external_docids.op().add(new_docids.into_stream()).r#union();
        let mut build = fst::MapBuilder::memory();
        while let Some((docid, values)) = op.next() {
            build.insert(docid, values[0].value).unwrap();
        }
        let external_docids = build.into_inner().unwrap();

        // TODO prefer using self.put_user_ids
        self.main.put::<_, Str, ByteSlice>(writer, EXTERNAL_DOCIDS_KEY, external_docids.as_slice())
    }

    pub fn remove_external_docids(self, writer: &mut heed::RwTxn<MainT>, ids: &fst::Map) -> ZResult<()> {
        use fst::{Streamer, IntoStreamer};

        // Do an union of the old and the new set of external docids.
        let external_docids = self.external_docids(writer)?;
        let mut op = external_docids.op().add(ids.into_stream()).difference();
        let mut build = fst::MapBuilder::memory();
        while let Some((docid, values)) = op.next() {
            build.insert(docid, values[0].value).unwrap();
        }
        let external_docids = build.into_inner().unwrap();

        // TODO prefer using self.put_external_docids
        self.main.put::<_, Str, ByteSlice>(writer, EXTERNAL_DOCIDS_KEY, external_docids.as_slice())
    }

    pub fn external_docids(self, reader: &heed::RoTxn<MainT>) -> ZResult<fst::Map> {
        match self.main.get::<_, Str, ByteSlice>(reader, EXTERNAL_DOCIDS_KEY)? {
            Some(bytes) => {
                let len = bytes.len();
                let bytes = Arc::new(bytes.to_owned());
                let fst = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(fst::Map::from(fst))
            },
            None => Ok(fst::Map::default()),
        }
    }

    pub fn external_to_internal_docid(self, reader: &heed::RoTxn<MainT>, external_docid: &str) -> ZResult<Option<DocumentId>> {
        let external_ids = self.external_docids(reader)?;
        Ok(external_ids.get(external_docid).map(|id| DocumentId(id as u32)))
    }

    pub fn put_words_fst(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set) -> ZResult<()> {
        self.main.put::<_, Str, ByteSlice>(writer, WORDS_KEY, fst.as_fst().as_bytes())
    }

    pub unsafe fn static_words_fst(self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<fst::Set>> {
        match self.main.get::<_, Str, ByteSlice>(reader, WORDS_KEY)? {
            Some(bytes) => {
                let bytes: &'static [u8] = std::mem::transmute(bytes);
                let set = fst::Set::from_static_slice(bytes).unwrap();
                Ok(Some(set))
            },
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
            },
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

    pub fn attributes_for_faceting<'txn>(&self, reader: &'txn heed::RoTxn<MainT>) -> ZResult<Option<Cow<'txn, Set<FieldId>>>> {
        self.main.get::<_, Str, CowSet<FieldId>>(reader, ATTRIBUTES_FOR_FACETING_KEY)
    }

    pub fn put_attributes_for_faceting(self, writer: &mut heed::RwTxn<MainT>, attributes: &Set<FieldId>) -> ZResult<()> {
        self.main.put::<_, Str, CowSet<FieldId>>(writer, ATTRIBUTES_FOR_FACETING_KEY, attributes)
    }

    pub fn delete_attributes_for_faceting(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<bool> {
        self.main.delete::<_, Str>(writer, ATTRIBUTES_FOR_FACETING_KEY)
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

    pub fn distinct_attribute(&self, reader: &heed::RoTxn<MainT>) -> ZResult<Option<String>> {
        if let Some(value) = self.main.get::<_, Str, Str>(reader, DISTINCT_ATTRIBUTE_KEY)? {
            return Ok(Some(value.to_owned()))
        }
        return Ok(None)
    }

    pub fn put_distinct_attribute(self, writer: &mut heed::RwTxn<MainT>, value: &str) -> ZResult<()> {
        self.main.put::<_, Str, Str>(writer, DISTINCT_ATTRIBUTE_KEY, value)
    }

    pub fn delete_distinct_attribute(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<bool> {
        self.main.delete::<_, Str>(writer, DISTINCT_ATTRIBUTE_KEY)
    }

    pub fn put_customs(self, writer: &mut heed::RwTxn<MainT>, customs: &[u8]) -> ZResult<()> {
        self.main
            .put::<_, Str, ByteSlice>(writer, CUSTOMS_KEY, customs)
    }

    pub fn customs<'txn>(self, reader: &'txn heed::RoTxn<MainT>) -> ZResult<Option<&'txn [u8]>> {
        self.main.get::<_, Str, ByteSlice>(reader, CUSTOMS_KEY)
    }
}
