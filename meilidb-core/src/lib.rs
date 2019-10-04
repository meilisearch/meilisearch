mod automaton;
mod error;
mod number;
mod query_builder;
mod ranked_map;
mod raw_document;
mod reordered_attrs;
mod update;
pub mod criterion;
pub mod raw_indexer;
pub mod serde;
pub mod store;

pub use self::query_builder::QueryBuilder;
pub use self::raw_document::RawDocument;
pub use self::error::{Error, MResult};
pub use self::number::{Number, ParseNumberError};
pub use self::ranked_map::RankedMap;
pub use self::store::Index;

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct Database {
    rkv: Arc<RwLock<rkv::Rkv>>,
    main_store: rkv::SingleStore,
    indexes: RwLock<HashMap<String, Index>>,
}

impl Database {
    pub fn open_or_create(path: impl AsRef<Path>) -> io::Result<Database> {
        let manager = rkv::Manager::singleton();
        let mut rkv_write = manager.write().unwrap();
        let rkv = rkv_write
            .get_or_create(path.as_ref(), |path| {
                let mut builder = rkv::Rkv::environment_builder();
                builder.set_max_dbs(3000).set_map_size(10 * 1024 * 1024 * 1024); // 10GB
                rkv::Rkv::from_env(path, builder)
            })
            .unwrap();

        drop(rkv_write);

        let mut indexes = HashMap::new();
        let main_store;

        {
            let rkv_read = rkv.read().unwrap();
            main_store = rkv_read
                .open_single("indexes", rkv::store::Options::create())
                .unwrap();

            let mut must_open = Vec::new();

            let reader = rkv_read.read().unwrap();
            for result in main_store.iter_start(&reader).unwrap() {
                let (key, _) = result.unwrap();
                if let Ok(index_name) = std::str::from_utf8(key) {
                    println!("{:?}", index_name);
                    must_open.push(index_name.to_owned());
                }
            }

            drop(reader);

            for index_name in must_open {
                let index = store::open(&rkv_read, &index_name).unwrap();
                indexes.insert(index_name, index);
            }
        }

        Ok(Database { rkv, main_store, indexes: RwLock::new(indexes) })
    }

    pub fn open_index(&self, name: impl Into<String>) -> MResult<Index> {
        let read = self.indexes.read().unwrap();
        let name = name.into();

        match read.get(&name) {
            Some(index) => Ok(*index),
            None => {
                drop(read);
                let rkv = self.rkv.read().unwrap();
                let mut write = self.indexes.write().unwrap();
                let index = store::create(&rkv, &name).unwrap();

                let mut writer = rkv.write().unwrap();
                let value = rkv::Value::Blob(&[]);
                self.main_store.put(&mut writer, &name, &value).unwrap();
                writer.commit().unwrap();

                Ok(*write.entry(name.clone()).or_insert(index))
            },
        }
    }
}

use zerocopy::{AsBytes, FromBytes};
use ::serde::{Serialize, Deserialize};

/// Represent an internally generated document unique identifier.
///
/// It is used to inform the database the document you want to deserialize.
/// Helpful for custom ranking.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[derive(Serialize, Deserialize)]
#[derive(AsBytes, FromBytes)]
#[repr(C)]
pub struct DocumentId(pub u64);

/// This structure represent the position of a word
/// in a document and its attributes.
///
/// This is stored in the map, generated at index time,
/// extracted and interpreted at search time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(AsBytes, FromBytes)]
#[repr(C)]
pub struct DocIndex {
    /// The document identifier where the word was found.
    pub document_id: DocumentId,

    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: u16,
    pub word_index: u16,

    /// The position in bytes where the word was found
    /// along with the length of it.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_index: u16,
    pub char_length: u16,
}

/// This structure represent a matching word with informations
/// on the location of the word in the document.
///
/// The order of the field is important because it defines
/// the way these structures are ordered between themselves.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Highlight {
    /// The attribute in the document where the word was found
    /// along with the index in it.
    pub attribute: u16,

    /// The position in bytes where the word was found.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_index: u16,

    /// The length in bytes of the found word.
    ///
    /// It informs on the original word area in the text indexed
    /// without needing to run the tokenizer again.
    pub char_length: u16,
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TmpMatch {
    pub query_index: u32,
    pub distance: u8,
    pub attribute: u16,
    pub word_index: u16,
    pub is_exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    #[cfg(test)]
    pub matches: Vec<TmpMatch>,
}

impl Document {
    #[cfg(not(test))]
    fn from_raw(raw: RawDocument) -> Document {
        Document { id: raw.id, highlights: raw.highlights }
    }

    #[cfg(test)]
    fn from_raw(raw: RawDocument) -> Document {
        let len = raw.query_index().len();
        let mut matches = Vec::with_capacity(len);

        let query_index = raw.query_index();
        let distance = raw.distance();
        let attribute = raw.attribute();
        let word_index = raw.word_index();
        let is_exact = raw.is_exact();

        for i in 0..len {
            let match_ = TmpMatch {
                query_index: query_index[i],
                distance: distance[i],
                attribute: attribute[i],
                word_index: word_index[i],
                is_exact: is_exact[i],
            };
            matches.push(match_);
        }

        Document { id: raw.id, matches, highlights: raw.highlights }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn docindex_mem_size() {
        assert_eq!(mem::size_of::<DocIndex>(), 16);
    }
}
