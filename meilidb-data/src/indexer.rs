use std::collections::BTreeMap;
use std::convert::TryFrom;

use meilidb_core::{DocumentId, DocIndex};
use meilidb_core::{Index as WordIndex, IndexBuilder as WordIndexBuilder};
use meilidb_tokenizer::{Tokenizer, SeqTokenizer, Token};
use crate::SchemaAttr;

use sdset::Set;

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
            if token.word_index >= self.word_limit { break }

            let lower = token.word.to_lowercase();
            let token = Token { word: &lower, ..token };

            let docindex = match token_to_docindex(id, attr, token) {
                Some(docindex) => docindex,
                None => break,
            };

            let word = Vec::from(token.word);
            self.indexed.entry(word).or_insert_with(Vec::new).push(docindex);
        }
    }

    pub fn index_text_seq<'a, I>(&mut self, id: DocumentId, attr: SchemaAttr, iter: I)
    where I: IntoIterator<Item=&'a str>,
    {
        let iter = iter.into_iter();
        for token in SeqTokenizer::new(iter) {
            if token.word_index >= self.word_limit { break }

            let lower = token.word.to_lowercase();
            let token = Token { word: &lower, ..token };

            let docindex = match token_to_docindex(id, attr, token) {
                Some(docindex) => docindex,
                None => break,
            };

            let word = Vec::from(token.word);
            self.indexed.entry(word).or_insert_with(Vec::new).push(docindex);
        }
    }

    pub fn build(self) -> WordIndex {
        let mut builder = WordIndexBuilder::new();

        for (key, mut indexes) in self.indexed {
            indexes.sort_unstable();
            indexes.dedup();

            let indexes = Set::new_unchecked(&indexes);
            builder.insert(key, indexes).unwrap();
        }

        builder.build()
    }
}

fn token_to_docindex<'a>(id: DocumentId, attr: SchemaAttr, token: Token<'a>) -> Option<DocIndex> {
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
