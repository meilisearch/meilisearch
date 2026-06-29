use charabia::TokenizerBuilder;
use heed::types::Str;
use heed::RwTxn;

use super::{UpgradeIndex, UpgradeParams};
use crate::{index::Synonyms, update::settings::normalize, Index, Result};

pub const SYNONYMS_KEY: &str = "synonyms";

/// Migrate the synonyms from the old format to the new database.
pub(super) struct MigrateSynonymsToDedicatedDatabase();

impl UpgradeIndex for MigrateSynonymsToDedicatedDatabase {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, _params: UpgradeParams<'_>) -> Result<bool> {
        let rtxn = index.read_txn()?;
        let user_defined_synonyms = index.user_defined_synonyms(&rtxn)?;

        index.main.remap_key_type::<Str>().delete(wtxn, SYNONYMS_KEY)?;

        if user_defined_synonyms.is_empty() {
            return Ok(false);
        }

        let mut builder = TokenizerBuilder::new();
        let stop_words = index.stop_words(&rtxn)?;
        if let Some(ref stop_words) = stop_words {
            builder.stop_words(stop_words);
        }

        let separators = index.allowed_separators(&rtxn)?;
        let separators: Option<Vec<_>> =
            separators.as_ref().map(|x| x.iter().map(String::as_str).collect());
        if let Some(ref separators) = separators {
            builder.separators(separators);
        }

        let dictionary = index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
        if let Some(ref dictionary) = dictionary {
            builder.words_dict(dictionary);
        }

        let tokenizer = builder.build();

        for (original_key, synonyms) in user_defined_synonyms {
            let normalized = normalize(&tokenizer, &original_key);
            let key: Vec<_> = normalized.iter().map(AsRef::as_ref).collect();
            let synonyms = Synonyms::new(synonyms);
            if synonyms.synonyms(&tokenizer).is_empty() {
                continue;
            }

            index.synonyms.put(wtxn, &key, &synonyms)?;
        }

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 50, 0)
    }

    fn description(&self) -> &'static str {
        "Migrate synonyms to dedicated database"
    }
}
