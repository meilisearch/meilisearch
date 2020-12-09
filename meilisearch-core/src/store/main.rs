use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use heed::types::{ByteSlice, OwnedType, SerdeBincode, Str, CowSlice};
use meilisearch_schema::{FieldId, Schema};
use meilisearch_types::DocumentId;
use sdset::Set;

use crate::database::MainT;
use crate::{RankedMap, MResult};
use crate::settings::RankingRule;
use crate::{FstSetCow, FstMapCow};
use super::{CowSet, DocumentsIds};

const ATTRIBUTES_FOR_FACETING_KEY: &str = "attributes-for-faceting";
const CREATED_AT_KEY: &str = "created-at";
const CUSTOMS_KEY: &str = "customs";
const DISTINCT_ATTRIBUTE_KEY: &str = "distinct-attribute";
const EXTERNAL_DOCIDS_KEY: &str = "external-docids";
const FIELDS_DISTRIBUTION_KEY: &str = "fields-distribution";
const INTERNAL_DOCIDS_KEY: &str = "internal-docids";
const NAME_KEY: &str = "name";
const NUMBER_OF_DOCUMENTS_KEY: &str = "number-of-documents";
const RANKED_MAP_KEY: &str = "ranked-map";
const RANKING_RULES_KEY: &str = "ranking-rules";
const SCHEMA_KEY: &str = "schema";
const SORTED_DOCUMENT_IDS_CACHE_KEY: &str = "sorted-document-ids-cache";
const STOP_WORDS_KEY: &str = "stop-words";
const SYNONYMS_KEY: &str = "synonyms";
const UPDATED_AT_KEY: &str = "updated-at";
const WORDS_KEY: &str = "words";

pub type FreqsMap = BTreeMap<String, usize>;
type SerdeFreqsMap = SerdeBincode<FreqsMap>;
type SerdeDatetime = SerdeBincode<DateTime<Utc>>;

#[derive(Copy, Clone)]
pub struct Main {
    pub(crate) main: heed::PolyDatabase,
}

impl Main {
    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> MResult<()> {
        Ok(self.main.clear(writer)?)
    }

    pub fn put_name(self, writer: &mut heed::RwTxn<MainT>, name: &str) -> MResult<()> {
        Ok(self.main.put::<_, Str, Str>(writer, NAME_KEY, name)?)
    }

    pub fn name(self, reader: &heed::RoTxn<MainT>) -> MResult<Option<String>> {
        Ok(self
            .main
            .get::<_, Str, Str>(reader, NAME_KEY)?
            .map(|name| name.to_owned()))
    }

    pub fn put_created_at(self, writer: &mut heed::RwTxn<MainT>) -> MResult<()> {
        Ok(self.main.put::<_, Str, SerdeDatetime>(writer, CREATED_AT_KEY, &Utc::now())?)
    }

    pub fn created_at(self, reader: &heed::RoTxn<MainT>) -> MResult<Option<DateTime<Utc>>> {
        Ok(self.main.get::<_, Str, SerdeDatetime>(reader, CREATED_AT_KEY)?)
    }

    pub fn put_updated_at(self, writer: &mut heed::RwTxn<MainT>) -> MResult<()> {
        Ok(self.main.put::<_, Str, SerdeDatetime>(writer, UPDATED_AT_KEY, &Utc::now())?)
    }

    pub fn updated_at(self, reader: &heed::RoTxn<MainT>) -> MResult<Option<DateTime<Utc>>> {
        Ok(self.main.get::<_, Str, SerdeDatetime>(reader, UPDATED_AT_KEY)?)
    }

    pub fn put_internal_docids(self, writer: &mut heed::RwTxn<MainT>, ids: &sdset::Set<DocumentId>) -> MResult<()> {
        Ok(self.main.put::<_, Str, DocumentsIds>(writer, INTERNAL_DOCIDS_KEY, ids)?)
    }

    pub fn internal_docids<'txn>(self, reader: &'txn heed::RoTxn<MainT>) -> MResult<Cow<'txn, sdset::Set<DocumentId>>> {
        match self.main.get::<_, Str, DocumentsIds>(reader, INTERNAL_DOCIDS_KEY)? {
            Some(ids) => Ok(ids),
            None => Ok(Cow::default()),
        }
    }

    pub fn merge_internal_docids(self, writer: &mut heed::RwTxn<MainT>, new_ids: &sdset::Set<DocumentId>) -> MResult<()> {
        use sdset::SetOperation;

        // We do an union of the old and new internal ids.
        let internal_docids = self.internal_docids(writer)?;
        let internal_docids = sdset::duo::Union::new(&internal_docids, new_ids).into_set_buf();
        Ok(self.put_internal_docids(writer, &internal_docids)?)
    }

    pub fn remove_internal_docids(self, writer: &mut heed::RwTxn<MainT>, ids: &sdset::Set<DocumentId>) -> MResult<()> {
        use sdset::SetOperation;

        // We do a difference of the old and new internal ids.
        let internal_docids = self.internal_docids(writer)?;
        let internal_docids = sdset::duo::Difference::new(&internal_docids, ids).into_set_buf();
        Ok(self.put_internal_docids(writer, &internal_docids)?)
    }

    pub fn put_external_docids<A>(self, writer: &mut heed::RwTxn<MainT>, ids: &fst::Map<A>) -> MResult<()>
    where A: AsRef<[u8]>,
    {
        Ok(self.main.put::<_, Str, ByteSlice>(writer, EXTERNAL_DOCIDS_KEY, ids.as_fst().as_bytes())?)
    }

    pub fn merge_external_docids<A>(self, writer: &mut heed::RwTxn<MainT>, new_docids: &fst::Map<A>) -> MResult<()>
    where A: AsRef<[u8]>,
    {
        use fst::{Streamer, IntoStreamer};

        // Do an union of the old and the new set of external docids.
        let external_docids = self.external_docids(writer)?;
        let mut op = external_docids.op().add(new_docids.into_stream()).r#union();
        let mut build = fst::MapBuilder::memory();
        while let Some((docid, values)) = op.next() {
            build.insert(docid, values[0].value).unwrap();
        }
        drop(op);

        let external_docids = build.into_map();
        Ok(self.put_external_docids(writer, &external_docids)?)
    }

    pub fn remove_external_docids<A>(self, writer: &mut heed::RwTxn<MainT>, ids: &fst::Map<A>) -> MResult<()>
    where A: AsRef<[u8]>,
    {
        use fst::{Streamer, IntoStreamer};

        // Do an union of the old and the new set of external docids.
        let external_docids = self.external_docids(writer)?;
        let mut op = external_docids.op().add(ids.into_stream()).difference();
        let mut build = fst::MapBuilder::memory();
        while let Some((docid, values)) = op.next() {
            build.insert(docid, values[0].value).unwrap();
        }
        drop(op);

        let external_docids = build.into_map();
        self.put_external_docids(writer, &external_docids)
    }

    pub fn external_docids<'a>(self, reader: &'a heed::RoTxn<'a, MainT>) -> MResult<FstMapCow> {
        match self.main.get::<_, Str, ByteSlice>(reader, EXTERNAL_DOCIDS_KEY)? {
            Some(bytes) => Ok(fst::Map::new(bytes).unwrap().map_data(Cow::Borrowed).unwrap()),
            None => Ok(fst::Map::default().map_data(Cow::Owned).unwrap()),
        }
    }

    pub fn external_to_internal_docid(self, reader: &heed::RoTxn<MainT>, external_docid: &str) -> MResult<Option<DocumentId>> {
        let external_ids = self.external_docids(reader)?;
        Ok(external_ids.get(external_docid).map(|id| DocumentId(id as u32)))
    }

    pub fn words_fst<'a>(self, reader: &'a heed::RoTxn<'a, MainT>) -> MResult<FstSetCow> {
        match self.main.get::<_, Str, ByteSlice>(reader, WORDS_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes).unwrap().map_data(Cow::Borrowed).unwrap()),
            None => Ok(fst::Set::default().map_data(Cow::Owned).unwrap()),
        }
    }

    pub fn put_words_fst<A: AsRef<[u8]>>(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set<A>) -> MResult<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(writer, WORDS_KEY, fst.as_fst().as_bytes())?)
    }

    pub fn put_sorted_document_ids_cache(self, writer: &mut heed::RwTxn<MainT>, documents_ids: &[DocumentId]) -> MResult<()> {
        Ok(self.main.put::<_, Str, CowSlice<DocumentId>>(writer, SORTED_DOCUMENT_IDS_CACHE_KEY, documents_ids)?)
    }

    pub fn sorted_document_ids_cache<'a>(self, reader: &'a heed::RoTxn<'a, MainT>) -> MResult<Option<Cow<[DocumentId]>>> {
        Ok(self.main.get::<_, Str, CowSlice<DocumentId>>(reader, SORTED_DOCUMENT_IDS_CACHE_KEY)?)
    }

    pub fn put_schema(self, writer: &mut heed::RwTxn<MainT>, schema: &Schema) -> MResult<()> {
        Ok(self.main.put::<_, Str, SerdeBincode<Schema>>(writer, SCHEMA_KEY, schema)?)
    }

    pub fn schema(self, reader: &heed::RoTxn<MainT>) -> MResult<Option<Schema>> {
        Ok(self.main.get::<_, Str, SerdeBincode<Schema>>(reader, SCHEMA_KEY)?)
    }

    pub fn delete_schema(self, writer: &mut heed::RwTxn<MainT>) -> MResult<bool> {
        Ok(self.main.delete::<_, Str>(writer, SCHEMA_KEY)?)
    }

    pub fn put_ranked_map(self, writer: &mut heed::RwTxn<MainT>, ranked_map: &RankedMap) -> MResult<()> {
        Ok(self.main.put::<_, Str, SerdeBincode<RankedMap>>(writer, RANKED_MAP_KEY, &ranked_map)?)
    }

    pub fn ranked_map(self, reader: &heed::RoTxn<MainT>) -> MResult<Option<RankedMap>> {
        Ok(self.main.get::<_, Str, SerdeBincode<RankedMap>>(reader, RANKED_MAP_KEY)?)
    }

    pub fn put_synonyms_fst<A: AsRef<[u8]>>(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set<A>) -> MResult<()> {
        let bytes = fst.as_fst().as_bytes();
        Ok(self.main.put::<_, Str, ByteSlice>(writer, SYNONYMS_KEY, bytes)?)
    }

    pub(crate) fn synonyms_fst<'a>(self, reader: &'a heed::RoTxn<'a, MainT>) -> MResult<FstSetCow> {
        match self.main.get::<_, Str, ByteSlice>(reader, SYNONYMS_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes).unwrap().map_data(Cow::Borrowed).unwrap()),
            None => Ok(fst::Set::default().map_data(Cow::Owned).unwrap()),
        }
    }

    pub fn synonyms(self, reader: &heed::RoTxn<MainT>) -> MResult<Vec<String>> {
        let synonyms = self
            .synonyms_fst(&reader)?
            .stream()
            .into_strs()?;
        Ok(synonyms)
    }

    pub fn put_stop_words_fst<A: AsRef<[u8]>>(self, writer: &mut heed::RwTxn<MainT>, fst: &fst::Set<A>) -> MResult<()> {
        let bytes = fst.as_fst().as_bytes();
        Ok(self.main.put::<_, Str, ByteSlice>(writer, STOP_WORDS_KEY, bytes)?)
    }

    pub(crate) fn stop_words_fst<'a>(self, reader: &'a heed::RoTxn<'a, MainT>) -> MResult<FstSetCow> {
        match self.main.get::<_, Str, ByteSlice>(reader, STOP_WORDS_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes).unwrap().map_data(Cow::Borrowed).unwrap()),
            None => Ok(fst::Set::default().map_data(Cow::Owned).unwrap()),
        }
    }

    pub fn stop_words(self, reader: &heed::RoTxn<MainT>) -> MResult<Vec<String>> {
        let stop_word_list = self
            .stop_words_fst(reader)?
            .stream()
            .into_strs()?;
        Ok(stop_word_list)
    }

    pub fn put_number_of_documents<F>(self, writer: &mut heed::RwTxn<MainT>, f: F) -> MResult<u64>
    where
        F: Fn(u64) -> u64,
    {
        let new = self.number_of_documents(&*writer).map(f)?;
        self.main
            .put::<_, Str, OwnedType<u64>>(writer, NUMBER_OF_DOCUMENTS_KEY, &new)?;
        Ok(new)
    }

    pub fn number_of_documents(self, reader: &heed::RoTxn<MainT>) -> MResult<u64> {
        match self
            .main
            .get::<_, Str, OwnedType<u64>>(reader, NUMBER_OF_DOCUMENTS_KEY)? {
            Some(value) => Ok(value),
            None => Ok(0),
        }
    }

    pub fn put_fields_distribution(
        self,
        writer: &mut heed::RwTxn<MainT>,
        fields_frequency: &FreqsMap,
    ) -> MResult<()> {
        Ok(self.main.put::<_, Str, SerdeFreqsMap>(writer, FIELDS_DISTRIBUTION_KEY, fields_frequency)?)
    }

    pub fn fields_distribution(&self, reader: &heed::RoTxn<MainT>) -> MResult<Option<FreqsMap>> {
        match self
            .main
            .get::<_, Str, SerdeFreqsMap>(reader, FIELDS_DISTRIBUTION_KEY)?
        {
            Some(freqs) => Ok(Some(freqs)),
            None => Ok(None),
        }
    }

    pub fn attributes_for_faceting<'txn>(&self, reader: &'txn heed::RoTxn<MainT>) -> MResult<Option<Cow<'txn, Set<FieldId>>>> {
        Ok(self.main.get::<_, Str, CowSet<FieldId>>(reader, ATTRIBUTES_FOR_FACETING_KEY)?)
    }

    pub fn put_attributes_for_faceting(self, writer: &mut heed::RwTxn<MainT>, attributes: &Set<FieldId>) -> MResult<()> {
        Ok(self.main.put::<_, Str, CowSet<FieldId>>(writer, ATTRIBUTES_FOR_FACETING_KEY, attributes)?)
    }

    pub fn delete_attributes_for_faceting(self, writer: &mut heed::RwTxn<MainT>) -> MResult<bool> {
        Ok(self.main.delete::<_, Str>(writer, ATTRIBUTES_FOR_FACETING_KEY)?)
    }

    pub fn ranking_rules(&self, reader: &heed::RoTxn<MainT>) -> MResult<Option<Vec<RankingRule>>> {
        Ok(self.main.get::<_, Str, SerdeBincode<Vec<RankingRule>>>(reader, RANKING_RULES_KEY)?)
    }

    pub fn put_ranking_rules(self, writer: &mut heed::RwTxn<MainT>, value: &[RankingRule]) -> MResult<()> {
        Ok(self.main.put::<_, Str, SerdeBincode<Vec<RankingRule>>>(writer, RANKING_RULES_KEY, &value.to_vec())?)
    }

    pub fn delete_ranking_rules(self, writer: &mut heed::RwTxn<MainT>) -> MResult<bool> {
        Ok(self.main.delete::<_, Str>(writer, RANKING_RULES_KEY)?)
    }

    pub fn distinct_attribute(&self, reader: &heed::RoTxn<MainT>) -> MResult<Option<FieldId>> {
        match self.main.get::<_, Str, OwnedType<u16>>(reader, DISTINCT_ATTRIBUTE_KEY)? {
            Some(value) => Ok(Some(FieldId(value.to_owned()))),
            None => Ok(None),
        }
    }

    pub fn put_distinct_attribute(self, writer: &mut heed::RwTxn<MainT>, value: FieldId) -> MResult<()> {
        Ok(self.main.put::<_, Str, OwnedType<u16>>(writer, DISTINCT_ATTRIBUTE_KEY, &value.0)?)
    }

    pub fn delete_distinct_attribute(self, writer: &mut heed::RwTxn<MainT>) -> MResult<bool> {
        Ok(self.main.delete::<_, Str>(writer, DISTINCT_ATTRIBUTE_KEY)?)
    }

    pub fn put_customs(self, writer: &mut heed::RwTxn<MainT>, customs: &[u8]) -> MResult<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(writer, CUSTOMS_KEY, customs)?)
    }

    pub fn customs<'txn>(self, reader: &'txn heed::RoTxn<MainT>) -> MResult<Option<&'txn [u8]>> {
        Ok(self.main.get::<_, Str, ByteSlice>(reader, CUSTOMS_KEY)?)
    }
}
