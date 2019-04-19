use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;

use deunicode::deunicode_with_tofu;
use meilidb_core::{DocumentId, DocIndex, Store};
use meilidb_tokenizer::{is_cjk, Tokenizer, SeqTokenizer, Token};
use sdset::{Set, SetBuf};
use sled::Tree;
use zerocopy::{AsBytes, LayoutVerified};

use crate::SchemaAttr;

#[derive(Clone)]
pub struct WordIndexTree(pub Arc<Tree>);

impl Store for WordIndexTree {
    type Error = sled::Error;

    fn get_fst(&self) -> Result<fst::Set, Self::Error> {
        match self.0.get("fst")? {
            Some(bytes) => {
                let bytes: Arc<[u8]> = bytes.into();
                let len = bytes.len();
                let raw = fst::raw::Fst::from_shared_bytes(bytes, 0, len).unwrap();
                Ok(fst::Set::from(raw))
            },
            None => Ok(fst::Set::default()),
        }
    }

    fn set_fst(&self, set: &fst::Set) -> Result<(), Self::Error> {
        let bytes = set.as_fst().to_vec();
        self.0.set("fst", bytes)?;
        Ok(())
    }

    fn get_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
        let mut word_bytes = Vec::from("word-");
        word_bytes.extend_from_slice(word);

        match self.0.get(word_bytes)? {
            Some(bytes) => {
                let layout = LayoutVerified::new_slice(bytes.as_ref()).unwrap();
                let slice = layout.into_slice();
                let setbuf = SetBuf::new_unchecked(slice.to_vec());
                Ok(Some(setbuf))
            },
            None => Ok(None),
        }
    }

    fn set_indexes(&self, word: &[u8], indexes: &Set<DocIndex>) -> Result<(), Self::Error> {
        let mut word_bytes = Vec::from("word-");
        word_bytes.extend_from_slice(word);

        let slice = indexes.as_slice();
        let bytes = slice.as_bytes();

        self.0.set(word_bytes, bytes)?;

        Ok(())
    }

    fn del_indexes(&self, word: &[u8]) -> Result<(), Self::Error> {
        let mut word_bytes = Vec::from("word-");
        word_bytes.extend_from_slice(word);

        self.0.del(word_bytes)?;

        Ok(())
    }

}

type Word = Vec<u8>; // TODO make it be a SmallVec

pub struct Indexer {
    word_limit: usize, // the maximum number of indexed words
    indexed: BTreeMap<Word, Vec<DocIndex>>,
}

impl Indexer {
    pub fn new() -> Indexer {
        Indexer {
            word_limit: 1000,
            indexed: BTreeMap::new(),
        }
    }

    pub fn with_word_limit(limit: usize) -> Indexer {
        Indexer {
            word_limit: limit,
            indexed: BTreeMap::new(),
        }
    }

    pub fn index_text(&mut self, id: DocumentId, attr: SchemaAttr, text: &str) {
        for token in Tokenizer::new(text) {
            let must_continue = index_token(token, id, attr, self.word_limit, &mut self.indexed);
            if !must_continue { break }
        }
    }

    pub fn index_text_seq<'a, I>(&mut self, id: DocumentId, attr: SchemaAttr, iter: I)
    where I: IntoIterator<Item=&'a str>,
    {
        let iter = iter.into_iter();
        for token in SeqTokenizer::new(iter) {
            let must_continue = index_token(token, id, attr, self.word_limit, &mut self.indexed);
            if !must_continue { break }
        }
    }

    pub fn build(self) -> BTreeMap<Word, SetBuf<DocIndex>> {
        self.indexed.into_iter().map(|(word, mut indexes)| {
            indexes.sort_unstable();
            (word, SetBuf::new_unchecked(indexes))
        }).collect()
    }
}

fn index_token(
    token: Token,
    id: DocumentId,
    attr: SchemaAttr,
    word_limit: usize,
    indexed: &mut BTreeMap<Word, Vec<DocIndex>>,
) -> bool
{
    if token.word_index >= word_limit { return false }

    let lower = token.word.to_lowercase();
    let token = Token { word: &lower, ..token };
    match token_to_docindex(id, attr, token) {
        Some(docindex) => {
            let word = Vec::from(token.word);
            indexed.entry(word).or_insert_with(Vec::new).push(docindex);
        },
        None => return false,
    }

    if !lower.contains(is_cjk) {
        let unidecoded = deunicode_with_tofu(&lower, "");
        if unidecoded != lower {
            let token = Token { word: &unidecoded, ..token };
            match token_to_docindex(id, attr, token) {
                Some(docindex) => {
                    let word = Vec::from(token.word);
                    indexed.entry(word).or_insert_with(Vec::new).push(docindex);
                },
                None => return false,
            }
        }
    }

    true
}

fn token_to_docindex(id: DocumentId, attr: SchemaAttr, token: Token) -> Option<DocIndex> {
    let word_index = u16::try_from(token.word_index).ok()?;
    let char_index = u16::try_from(token.char_index).ok()?;
    let char_length = u16::try_from(token.word.chars().count()).ok()?;

    let docindex = DocIndex {
        document_id: id,
        attribute: attr.0,
        word_index: word_index,
        char_index: char_index,
        char_length: char_length,
    };

    Some(docindex)
}
