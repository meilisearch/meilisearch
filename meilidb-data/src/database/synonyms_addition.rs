use std::collections::BTreeMap;
use std::sync::Arc;

use fst::{SetBuilder, set::OpBuilder};
use meilidb_core::normalize_str;
use sdset::SetBuf;

use crate::database::index::InnerIndex;
use super::{Error, Index};

pub struct SynonymsAddition<'a> {
    inner: &'a Index,
    synonyms: BTreeMap<String, Vec<String>>,
}

impl<'a> SynonymsAddition<'a> {
    pub fn new(inner: &'a Index) -> SynonymsAddition<'a> {
        SynonymsAddition { inner, synonyms: BTreeMap::new() }
    }

    pub fn add_synonym<S, T, I>(&mut self, synonym: S, alternatives: I)
    where S: AsRef<str>,
          T: AsRef<str>,
          I: IntoIterator<Item=T>,
    {
        let synonym = normalize_str(synonym.as_ref());
        let alternatives = alternatives.into_iter().map(|s| s.as_ref().to_lowercase());
        self.synonyms.entry(synonym).or_insert_with(Vec::new).extend(alternatives);
    }

    pub fn finalize(self) -> Result<(), Error> {
        let lease_inner = self.inner.lease_inner();
        let synonyms = &lease_inner.raw.synonyms;
        let main = &lease_inner.raw.main;

        let mut synonyms_builder = SetBuilder::memory();

        for (synonym, alternatives) in self.synonyms {
            synonyms_builder.insert(&synonym).unwrap();

            let alternatives = {
                let alternatives = SetBuf::from_dirty(alternatives);
                let mut alternatives_builder = SetBuilder::memory();
                alternatives_builder.extend_iter(alternatives).unwrap();
                alternatives_builder.into_inner().unwrap()
            };
            synonyms.set_alternatives_to(synonym.as_bytes(), alternatives)?;
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
                    .r#union();

                let mut synonyms_builder = SetBuilder::memory();
                synonyms_builder.extend_stream(op).unwrap();
                synonyms_builder
                    .into_inner()
                    .and_then(fst::Set::from_bytes)
                    .unwrap()
            },
            None => delta_synonyms,
        };

        main.set_synonyms_set(&synonyms)?;

        // update the "consistent" view of the Index
        let words = main.words_set()?.unwrap_or_default();
        let ranked_map = lease_inner.ranked_map.clone();;
        let schema = lease_inner.schema.clone();
        let raw = lease_inner.raw.clone();
        lease_inner.raw.compact();

        let inner = InnerIndex { words, synonyms, schema, ranked_map, raw };
        self.inner.0.store(Arc::new(inner));

        Ok(())
    }
}
