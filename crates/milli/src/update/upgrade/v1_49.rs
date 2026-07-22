use std::collections::BTreeMap;

use charabia::TokenizerBuilder;
use heed::types::Str;
use heed::RwTxn;

use super::{UpgradeIndex, UpgradeParams};
use crate::index::AssociatedSynonyms;
use crate::update::settings::normalize;
use crate::{Index, Result, MAX_LMDB_KEY_LENGTH};

pub const SYNONYMS_KEY: &str = "synonyms";

/// Migrate the synonyms from the old format to the new database.
pub(super) struct MigrateSynonymsToDedicatedDatabase();

impl UpgradeIndex for MigrateSynonymsToDedicatedDatabase {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, _params: UpgradeParams<'_>) -> Result<bool> {
        let user_defined_synonyms = index.user_defined_synonyms(wtxn)?;

        index.main.remap_key_type::<Str>().delete(wtxn, SYNONYMS_KEY)?;

        if user_defined_synonyms.is_empty() {
            return Ok(false);
        }

        let mut builder = TokenizerBuilder::new();
        let stop_words = index.stop_words(wtxn)?;
        if let Some(ref stop_words) = stop_words {
            builder.stop_words(stop_words);
        }

        let separators = index.allowed_separators(wtxn)?;
        let separators: Option<Vec<_>> =
            separators.as_ref().map(|x| x.iter().map(String::as_str).collect());
        if let Some(ref separators) = separators {
            builder.separators(separators);
        }

        let dictionary = index.dictionary(wtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
        if let Some(ref dictionary) = dictionary {
            builder.words_dict(dictionary);
        }

        let tokenizer = builder.build();

        let mut entries = BTreeMap::new();
        for (original_key, synonyms) in user_defined_synonyms {
            let normalized = normalize(&tokenizer, &original_key);
            if normalized.is_empty() {
                continue;
            }

            let synonyms = AssociatedSynonyms::new(synonyms);
            if synonyms.synonyms(&tokenizer).is_empty() {
                continue;
            }

            entries.insert(normalized, synonyms);
        }

        for (key, synonyms) in entries {
            // length of all words + count of delimiters
            let total_length = key.iter().map(String::len).sum::<usize>() + key.len();
            if total_length < MAX_LMDB_KEY_LENGTH && total_length != 0 {
                index.synonyms.put(wtxn, &key, &synonyms)?;
            }
        }

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 49, 0)
    }

    fn description(&self) -> &'static str {
        "Migrate synonyms to dedicated database"
    }
}
