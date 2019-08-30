use std::collections::BTreeMap;
use std::iter::FromIterator;
use std::sync::Arc;

use fst::{SetBuilder, set::OpBuilder};
use meilidb_core::normalize_str;
use sdset::SetBuf;

use crate::database::index::InnerIndex;
use super::{Error, Index};

pub struct SynonymsDeletion<'a> {
    inner: &'a Index,
    synonyms: BTreeMap<String, Option<Vec<String>>>,
}

impl<'a> SynonymsDeletion<'a> {
    pub fn new(inner: &'a Index) -> SynonymsDeletion<'a> {
        SynonymsDeletion { inner, synonyms: BTreeMap::new() }
    }

    pub fn delete_all_alternatives_of<S: AsRef<str>>(&mut self, synonym: S) {
        let synonym = normalize_str(synonym.as_ref());
        self.synonyms.insert(synonym, None);
    }

    pub fn delete_specific_alternatives_of<S, T, I>(&mut self, synonym: S, alternatives: I)
    where S: AsRef<str>,
          T: AsRef<str>,
          I: Iterator<Item=T>,
    {
        let synonym = normalize_str(synonym.as_ref());
        let value = self.synonyms.entry(synonym).or_insert(None);
        let alternatives = alternatives.map(|s| s.as_ref().to_lowercase());
        match value {
            Some(v) => v.extend(alternatives),
            None => *value = Some(Vec::from_iter(alternatives)),
        }
    }

    pub fn finalize(self) -> Result<(), Error> {
        let lease_inner = self.inner.lease_inner();
        let synonyms = &lease_inner.raw.synonyms;
        let main = &lease_inner.raw.main;

        let mut delete_whole_synonym_builder = SetBuilder::memory();

        for (synonym, alternatives) in self.synonyms {
            match alternatives {
                Some(alternatives) => {
                    let prev_alternatives = synonyms.alternatives_to(synonym.as_bytes())?;
                    let prev_alternatives = match prev_alternatives {
                        Some(alternatives) => alternatives,
                        None => continue,
                    };

                    let delta_alternatives = {
                        let alternatives = SetBuf::from_dirty(alternatives);
                        let mut builder = SetBuilder::memory();
                        builder.extend_iter(alternatives).unwrap();
                        builder.into_inner()
                            .and_then(fst::Set::from_bytes)
                            .unwrap()
                    };

                    let op = OpBuilder::new()
                        .add(prev_alternatives.stream())
                        .add(delta_alternatives.stream())
                        .difference();

                    let (alternatives, empty_alternatives) = {
                        let mut builder = SetBuilder::memory();
                        let len = builder.get_ref().len();
                        builder.extend_stream(op).unwrap();
                        let is_empty = len == builder.get_ref().len();
                        let alternatives = builder.into_inner().unwrap();
                        (alternatives, is_empty)
                    };

                    if empty_alternatives {
                        delete_whole_synonym_builder.insert(synonym.as_bytes())?;
                    } else {
                        synonyms.set_alternatives_to(synonym.as_bytes(), alternatives)?;
                    }
                },
                None => {
                    delete_whole_synonym_builder.insert(&synonym).unwrap();
                    synonyms.del_alternatives_of(synonym.as_bytes())?;
                }
            }
        }

        let delta_synonyms = delete_whole_synonym_builder
            .into_inner()
            .and_then(fst::Set::from_bytes)
            .unwrap();

        let synonyms = match main.synonyms_set()? {
            Some(synonyms) => {
                let op = OpBuilder::new()
                    .add(synonyms.stream())
                    .add(delta_synonyms.stream())
                    .difference();

                let mut synonyms_builder = SetBuilder::memory();
                synonyms_builder.extend_stream(op).unwrap();
                synonyms_builder
                    .into_inner()
                    .and_then(fst::Set::from_bytes)
                    .unwrap()
            },
            None => fst::Set::default(),
        };

        main.set_synonyms_set(&synonyms)?;

        // update the "consistent" view of the Index
        let words = Arc::new(main.words_set()?.unwrap_or_default());
        let ranked_map = lease_inner.ranked_map.clone();
        let synonyms = Arc::new(synonyms);
        let schema = lease_inner.schema.clone();
        let raw = lease_inner.raw.clone();
        lease_inner.raw.compact();

        let inner = InnerIndex { words, synonyms, schema, ranked_map, raw };
        self.inner.0.store(Arc::new(inner));

        Ok(())
    }
}
