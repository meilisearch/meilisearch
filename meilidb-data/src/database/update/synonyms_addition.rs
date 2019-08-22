use std::collections::BTreeMap;
use std::sync::Arc;

use fst::{SetBuilder, set::OpBuilder};
use meilidb_core::normalize_str;
use sdset::SetBuf;

use crate::database::{Error, Index,index::Cache};

pub struct SynonymsAddition<'a> {
    index: &'a Index,
    synonyms: BTreeMap<String, Vec<String>>,
}

impl<'a> SynonymsAddition<'a> {
    pub fn new(index: &'a Index) -> SynonymsAddition<'a> {
        SynonymsAddition { index, synonyms: BTreeMap::new() }
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

    pub fn finalize(self) -> Result<u64, Error> {
        self.index.push_synonyms_addition(self.synonyms)
    }
}

pub fn apply_synonyms_addition(
    index: &Index,
    addition: BTreeMap<String, Vec<String>>,
) -> Result<(), Error>
{
    let ref_index = index.as_ref();
    let synonyms = ref_index.synonyms_index;
    let main = ref_index.main_index;

    let mut synonyms_builder = SetBuilder::memory();

    for (synonym, alternatives) in addition {
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
    let cache = ref_index.cache;
    let words = Arc::new(main.words_set()?.unwrap_or_default());
    let ranked_map = cache.ranked_map.clone();
    let synonyms = Arc::new(synonyms);
    let schema = cache.schema.clone();

    let cache = Cache { words, synonyms, schema, ranked_map };
    index.cache.store(Arc::new(cache));

    Ok(())
}
