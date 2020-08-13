mod best_proximity;
mod criterion;
mod heed_codec;
mod iter_shortest_paths;
mod query_tokens;
mod search;
mod transitive_arc;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::BuildHasherDefault;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use fxhash::{FxHasher32, FxHasher64};
use heed::types::*;
use heed::{PolyDatabase, Database};
use memmap::Mmap;
use oxidized_mtbl as omtbl;

pub use self::search::{Search, SearchResult};
pub use self::criterion::{Criterion, default_criteria};
use self::heed_codec::RoaringBitmapCodec;
use self::transitive_arc::TransitiveArc;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type Attribute = u32;
pub type Position = u32;

#[derive(Clone)]
pub struct Index {
    // The database path, where the LMDB and MTBL files are.
    path: PathBuf,
    /// Contains many different types (e.g. the documents CSV headers).
    pub main: PolyDatabase,
    /// A word and all the positions where it appears in the whole dataset.
    pub word_positions: Database<Str, RoaringBitmapCodec>,
    pub prefix_word_positions: Database<Str, RoaringBitmapCodec>,
    /// Maps a word at a position (u32) and all the documents ids where it appears.
    pub word_position_docids: Database<ByteSlice, RoaringBitmapCodec>,
    pub prefix_word_position_docids: Database<ByteSlice, RoaringBitmapCodec>,
    /// Maps a word and an attribute (u32) to all the documents ids that it appears in.
    pub word_attribute_docids: Database<ByteSlice, RoaringBitmapCodec>,
    /// The MTBL store that contains the documents content.
    documents: omtbl::Reader<TransitiveArc<Mmap>>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(env: &heed::Env, path: P) -> anyhow::Result<Index> {
        let documents_path = path.as_ref().join("documents.mtbl");
        let mut documents = OpenOptions::new().create(true).write(true).read(true).open(documents_path)?;
        // If the file is empty we must initialize it like an empty MTBL database.
        if documents.metadata()?.len() == 0 {
            omtbl::Writer::new(&mut documents).finish()?;
        }
        let documents = unsafe { memmap::Mmap::map(&documents)? };

        Ok(Index {
            path: path.as_ref().to_path_buf(),
            main: env.create_poly_database(None)?,
            word_positions: env.create_database(Some("word-positions"))?,
            prefix_word_positions: env.create_database(Some("prefix-word-positions"))?,
            word_position_docids: env.create_database(Some("word-position-docids"))?,
            prefix_word_position_docids: env.create_database(Some("prefix-word-position-docids"))?,
            word_attribute_docids: env.create_database(Some("word-attribute-docids"))?,
            documents: omtbl::Reader::new(TransitiveArc(Arc::new(documents)))?,
        })
    }

    pub fn refresh_documents(&mut self) -> anyhow::Result<()> {
        let documents_path = self.path.join("documents.mtbl");
        let documents = File::open(&documents_path)?;
        let documents = unsafe { memmap::Mmap::map(&documents)? };
        self.documents = omtbl::Reader::new(TransitiveArc(Arc::new(documents)))?;
        Ok(())
    }

    pub fn put_headers(&self, wtxn: &mut heed::RwTxn, headers: &[u8]) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, "headers", headers)?)
    }

    pub fn headers<'t>(&self, rtxn: &'t heed::RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, "headers")
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
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, "words-fst", fst.as_fst().as_bytes())?)
    }

    pub fn fst<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, "words-fst")? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<I: IntoIterator<Item=DocumentId>>(&self, iter: I) -> anyhow::Result<Vec<(DocumentId, Vec<u8>)>> {
        iter.into_iter().map(|id| {
            let key = id.to_be_bytes();
            let content = self.documents.clone().get(&key)?.with_context(|| format!("Could not find document {}.", id))?;
            Ok((id, content.as_ref().to_vec()))
        })
        .collect()
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self) -> usize {
        self.documents.metadata().count_entries as usize
    }

    pub fn search<'a>(&'a self, rtxn: &'a heed::RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }
}
