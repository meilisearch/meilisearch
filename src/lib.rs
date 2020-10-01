mod criterion;
mod mdfs;
mod query_tokens;
mod search;
pub mod heed_codec;
pub mod proximity;
pub mod tokenizer;

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::Context;
use csv::StringRecord;
use fxhash::{FxHasher32, FxHasher64};
use heed::types::*;
use heed::{PolyDatabase, Database};

pub use self::search::{Search, SearchResult};
pub use self::criterion::{Criterion, default_criteria};
pub use self::heed_codec::{
    RoaringBitmapCodec, BEU32StrCodec, StrStrU8Codec,
    CsvStringRecordCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec,
};

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type Attribute = u32;
pub type Position = u32;

const WORDS_FST_KEY: &str = "words-fst";
const HEADERS_KEY: &str = "headers";
const DOCUMENTS_IDS_KEY: &str = "documents-ids";

#[derive(Clone)]
pub struct Index {
    /// Contains many different types (e.g. the documents CSV headers).
    pub main: PolyDatabase,
    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, RoaringBitmapCodec>,
    /// Maps a word and a document id (u32) to all the positions where the given word appears.
    pub docid_word_positions: Database<BEU32StrCodec, BoRoaringBitmapCodec>,
    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<StrStrU8Codec, CboRoaringBitmapCodec>,
    /// Maps the document id to the document as a CSV line.
    pub documents: Database<OwnedType<BEU32>, ByteSlice>,
}

impl Index {
    pub fn new(env: &heed::Env) -> anyhow::Result<Index> {
        Ok(Index {
            main: env.create_poly_database(None)?,
            word_docids: env.create_database(Some("word-docids"))?,
            docid_word_positions: env.create_database(Some("docid-word-positions"))?,
            word_pair_proximity_docids: env.create_database(Some("word-pair-proximity-docids"))?,
            documents: env.create_database(Some("documents"))?,
        })
    }

    pub fn put_headers(&self, wtxn: &mut heed::RwTxn, headers: &StringRecord) -> heed::Result<()> {
        self.main.put::<_, Str, CsvStringRecordCodec>(wtxn, HEADERS_KEY, headers)
    }

    pub fn headers(&self, rtxn: &heed::RoTxn) -> heed::Result<Option<StringRecord>> {
        self.main.get::<_, Str, CsvStringRecordCodec>(rtxn, HEADERS_KEY)
    }

    pub fn number_of_attributes(&self, rtxn: &heed::RoTxn) -> anyhow::Result<Option<usize>> {
        match self.headers(rtxn)? {
            Some(headers) => Ok(Some(headers.len())),
            None => Ok(None),
        }
    }

    pub fn put_fst<A: AsRef<[u8]>>(&self, wtxn: &mut heed::RwTxn, fst: &fst::Set<A>) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, WORDS_FST_KEY, fst.as_fst().as_bytes())?)
    }

    pub fn fst<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, WORDS_FST_KEY)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<'t>(
        &self,
        rtxn: &'t heed::RoTxn,
        iter: impl IntoIterator<Item=DocumentId>,
    ) -> anyhow::Result<Vec<(DocumentId, StringRecord)>>
    {
        let ids: Vec<_> = iter.into_iter().collect();
        let mut content = Vec::new();

        for id in ids.iter().cloned() {
            let document_content = self.documents.get(rtxn, &BEU32::new(id))?
                .with_context(|| format!("Could not find document {}", id))?;
            content.extend_from_slice(document_content);
        }

        let mut rdr = csv::ReaderBuilder::new().has_headers(false).from_reader(&content[..]);

        let mut documents = Vec::with_capacity(ids.len());
        for (id, result) in ids.into_iter().zip(rdr.records()) {
            documents.push((id, result?));
        }

        Ok(documents)
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<usize> {
        let docids = self.main.get::<_, Str, RoaringBitmapCodec>(rtxn, DOCUMENTS_IDS_KEY)?
            .with_context(|| format!("Could not find the list of documents ids"))?;
        Ok(docids.len() as usize)
    }

    pub fn search<'a>(&'a self, rtxn: &'a heed::RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }
}
