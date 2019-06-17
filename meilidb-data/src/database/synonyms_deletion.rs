use std::collections::BTreeSet;
use std::sync::Arc;

use fst::{SetBuilder, set::OpBuilder};

use crate::database::index::InnerIndex;
use super::{Error, Index};

pub struct SynonymsDeletion<'a> {
    inner: &'a Index,
    synonyms: BTreeSet<String>,
}

impl<'a> SynonymsDeletion<'a> {
    pub fn new(inner: &'a Index) -> SynonymsDeletion<'a> {
        SynonymsDeletion { inner, synonyms: BTreeSet::new() }
    }

    pub fn delete_alternatives_of<I>(&mut self, synonym: String) {
        self.synonyms.insert(synonym);
    }

    pub fn finalize(self) -> Result<(), Error> {
        let lease_inner = self.inner.lease_inner();
        let synonyms = &lease_inner.raw.synonyms;
        let main = &lease_inner.raw.main;

        let mut synonyms_builder = SetBuilder::memory();

        for synonym in self.synonyms {
            synonyms_builder.insert(&synonym).unwrap();
            synonyms.del_alternatives_of(synonym.as_bytes())?;
        }

        let delta_synonyms = synonyms_builder
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
        let words = main.words_set()?.unwrap_or_default();
        let ranked_map = lease_inner.ranked_map.clone();
        let schema = lease_inner.schema.clone();
        let raw = lease_inner.raw.clone();
        lease_inner.raw.compact();

        let inner = InnerIndex { words, synonyms, schema, ranked_map, raw };
        self.inner.0.store(Arc::new(inner));

        Ok(())
    }
}
