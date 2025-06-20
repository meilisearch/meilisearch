use heed::types::{SerdeJson, Str};
use heed::{RoTxn, RwTxn};
use serde::{Deserialize, Serialize};

use crate::index::main_key;
use crate::Index;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DisabledTyposTerms {
    pub disable_on_numbers: bool,
}

impl Index {
    pub fn disabled_typos_terms(&self, txn: &RoTxn<'_>) -> heed::Result<DisabledTyposTerms> {
        self.main
            .remap_types::<Str, SerdeJson<DisabledTyposTerms>>()
            .get(txn, main_key::DISABLED_TYPOS_TERMS)
            .map(|option| option.unwrap_or_default())
    }

    pub(crate) fn put_disabled_typos_terms(
        &self,
        txn: &mut RwTxn<'_>,
        disabled_typos_terms: &DisabledTyposTerms,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<DisabledTyposTerms>>().put(
            txn,
            main_key::DISABLED_TYPOS_TERMS,
            disabled_typos_terms,
        )?;

        Ok(())
    }
}

impl DisabledTyposTerms {
    pub fn is_exact(&self, word: &str) -> bool {
        // If disable_on_numbers is true, we disable the word if it contains only numbers or punctuation
        self.disable_on_numbers && word.chars().all(|c| c.is_numeric() || c.is_ascii_punctuation())
    }
}
