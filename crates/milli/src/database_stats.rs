use heed::types::Bytes;
use heed::Database;
use heed::RoTxn;
use serde::{Deserialize, Serialize};

use crate::Result;

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
    /// The maximum size of a key in the database.
    max_key_size: u64,
    /// The maximum size of a value in the database.
    max_value_size: u64,
    /// The minimum size of a key in the database.
    min_key_size: u64,
    /// The minimum size of a value in the database.
    min_value_size: u64,
}

impl DatabaseStats {
    /// Returns the stats of the database.
    ///
    /// This function iterates over the whole database and computes the stats.
    /// It is not efficient and should be cached somewhere.
    pub(crate) fn new(database: Database<Bytes, Bytes>, rtxn: &RoTxn<'_>) -> Result<Self> {
        let mut database_stats = Self {
            number_of_entries: 0,
            total_key_size: 0,
            total_value_size: 0,
            max_key_size: 0,
            max_value_size: 0,
            min_key_size: u64::MAX,
            min_value_size: u64::MAX,
        };

        let mut iter = database.iter(rtxn)?;
        while let Some((key, value)) = iter.next().transpose()? {
            let key_size = key.len() as u64;
            let value_size = value.len() as u64;
            database_stats.number_of_entries += 1;
            database_stats.total_key_size += key_size;
            database_stats.total_value_size += value_size;
            database_stats.max_key_size = database_stats.max_key_size.max(key_size);
            database_stats.max_value_size = database_stats.max_value_size.max(value_size);
            database_stats.min_key_size = database_stats.min_key_size.min(key_size);
            database_stats.min_value_size = database_stats.min_value_size.min(value_size);
        }

        if database_stats.number_of_entries == 0 {
            database_stats.min_key_size = 0;
            database_stats.min_value_size = 0;
        }

        Ok(database_stats)
    }

    pub fn average_key_size(&self) -> u64 {
        self.total_key_size / self.number_of_entries
    }

    pub fn average_value_size(&self) -> u64 {
        self.total_value_size / self.number_of_entries
    }

    pub fn number_of_entries(&self) -> u64 {
        self.number_of_entries
    }

    pub fn total_key_size(&self) -> u64 {
        self.total_key_size
    }

    pub fn total_value_size(&self) -> u64 {
        self.total_value_size
    }

    pub fn max_key_size(&self) -> u64 {
        self.max_key_size
    }

    pub fn max_value_size(&self) -> u64 {
        self.max_value_size
    }

    pub fn min_key_size(&self) -> u64 {
        self.min_key_size
    }

    pub fn min_value_size(&self) -> u64 {
        self.min_value_size
    }
}
