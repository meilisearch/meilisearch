mod criterion;
mod node;
mod query_tokens;
mod search;
pub mod heed_codec;
pub mod lexer;

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::{bail, Context};
use fxhash::{FxHasher32, FxHasher64};
use heed::types::*;
use heed::{PolyDatabase, Database};
use oxidized_mtbl as omtbl;

pub use self::search::{Search, SearchResult};
pub use self::criterion::{Criterion, default_criteria};
use self::heed_codec::{MtblCodec, RoaringBitmapCodec, StrBEU32Codec};

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
const DOCUMENTS_KEY: &str = "documents";

#[derive(Clone)]
pub struct Index {
    /// Contains many different types (e.g. the documents CSV headers).
    pub main: PolyDatabase,
    /// A word and all the positions where it appears in the whole dataset.
    pub word_positions: Database<Str, RoaringBitmapCodec>,
    /// Maps a word at a position (u32) and all the documents ids where the given word appears.
    pub word_position_docids: Database<StrBEU32Codec, RoaringBitmapCodec>,
    /// Maps a word and an attribute (u32) to all the documents ids where the given word appears.
    pub word_attribute_docids: Database<StrBEU32Codec, RoaringBitmapCodec>,
}

impl Index {
    pub fn new(env: &heed::Env) -> anyhow::Result<Index> {
        Ok(Index {
            main: env.create_poly_database(None)?,
            word_positions: env.create_database(Some("word-positions"))?,
            word_position_docids: env.create_database(Some("word-position-docids"))?,
            word_attribute_docids: env.create_database(Some("word-attribute-docids"))?,
        })
    }

    pub fn put_headers(&self, wtxn: &mut heed::RwTxn, headers: &[u8]) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, HEADERS_KEY, headers)?)
    }

    pub fn headers<'t>(&self, rtxn: &'t heed::RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, HEADERS_KEY)
    }

    pub fn number_of_attributes<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<Option<usize>> {
        match self.headers(rtxn)? {
            Some(headers) => {
                let mut rdr = csv::Reader::from_reader(headers);
                let headers = rdr.headers()?;
                Ok(Some(headers.len()))
            }
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
    ) -> anyhow::Result<Vec<(DocumentId, Vec<u8>)>>
    {
        match self.main.get::<_, Str, MtblCodec<&[u8]>>(rtxn, DOCUMENTS_KEY)? {
            Some(documents) => {
                iter.into_iter().map(|id| {
                    let key = id.to_be_bytes();
                    let content = documents.clone().get(&key)?
                        .with_context(|| format!("Could not find document {}", id))?;
                    Ok((id, content.as_ref().to_vec()))
                }).collect()
            },
            None => bail!("No documents database found"),
        }
    }

    pub fn put_documents<A: AsRef<[u8]>>(&self, wtxn: &mut heed::RwTxn, documents: &omtbl::Reader<A>) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, MtblCodec<A>>(wtxn, DOCUMENTS_KEY, documents)?)
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<usize> {
        match self.main.get::<_, Str, MtblCodec<&[u8]>>(rtxn, DOCUMENTS_KEY)? {
            Some(documents) => Ok(documents.metadata().count_entries as usize),
            None => return Ok(0),
        }
    }

    pub fn search<'a>(&'a self, rtxn: &'a heed::RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }
}
