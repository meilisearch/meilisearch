use std::mem;

use heed::{Database, DatabaseStat, RoTxn, Unspecified};
use serde::{Deserialize, Serialize};

use crate::BEU32;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
/// The stats of a database.
pub struct DatabaseStats {
    /// The number of entries in the database.
    number_of_entries: u64,
    /// The total size of the keys in the database.
    total_key_size: u64,
    /// The total size of the values in the database.
    total_value_size: u64,
}

impl DatabaseStats {
    /// Returns the stats of the database.
    ///
    /// This function iterates over the whole database and computes the stats.
    /// It is not efficient and should be cached somewhere.
    pub(crate) fn new(
        database: Database<BEU32, Unspecified>,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<Self> {
        let DatabaseStat { page_size, depth: _, branch_pages, leaf_pages, overflow_pages, entries } =
            database.stat(rtxn)?;

        // We first take the total size without overflow pages as the overflow pages contains the values and only that.
        let total_size = (branch_pages + leaf_pages + overflow_pages) * page_size as usize;
        // We compute an estimated size for the keys.
        let total_key_size = entries * (mem::size_of::<u32>() + 4);
        let total_value_size = total_size - total_key_size;

        Ok(Self {
            number_of_entries: entries as u64,
            total_key_size: total_key_size as u64,
            total_value_size: total_value_size as u64,
        })
    }

    pub fn average_key_size(&self) -> u64 {
        self.total_key_size.checked_div(self.number_of_entries).unwrap_or(0)
    }

    pub fn average_value_size(&self) -> u64 {
        self.total_value_size.checked_div(self.number_of_entries).unwrap_or(0)
    }

    pub fn number_of_entries(&self) -> u64 {
        self.number_of_entries
    }

    pub fn total_size(&self) -> u64 {
        self.total_key_size + self.total_value_size
    }

    pub fn total_key_size(&self) -> u64 {
        self.total_key_size
    }

    pub fn total_value_size(&self) -> u64 {
        self.total_value_size
    }
}
