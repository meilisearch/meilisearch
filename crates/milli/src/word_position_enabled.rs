use heed::types::{SerdeJson, Str};
use heed::{RoTxn, RwTxn};
use serde::{Deserialize, Serialize};

use crate::index::main_key;
use crate::Index;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WordPosition {
    pub enabled: bool,
}

impl Index {
    pub fn word_position(&self, txn: &RoTxn<'_>) -> heed::Result<WordPosition> {
        self.main
            .remap_types::<Str, SerdeJson<WordPosition>>()
            .get(txn, main_key::WORD_POSITION)
            .map(|option| option.unwrap_or_default())
    }

    pub(crate) fn put_word_position(
        &self,
        txn: &mut RwTxn<'_>,
        disabled_typos_terms: &WordPosition,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<WordPosition>>().put(
            txn,
            main_key::WORD_POSITION,
            disabled_typos_terms,
        )?;

        Ok(())
    }
}

impl Default for WordPosition {
    fn default() -> Self {
        Self { enabled: true }
    }
}
