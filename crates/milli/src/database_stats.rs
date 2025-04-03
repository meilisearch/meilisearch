use heed::types::Bytes;
use heed::Database;
use heed::RoTxn;
use serde::{Deserialize, Serialize};

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
    pub(crate) fn new(database: Database<Bytes, Bytes>, rtxn: &RoTxn<'_>) -> heed::Result<Self> {
        let mut database_stats =
            Self { number_of_entries: 0, total_key_size: 0, total_value_size: 0 };

        let mut iter = database.iter(rtxn)?;
        while let Some((key, value)) = iter.next().transpose()? {
            let key_size = key.len() as u64;
            let value_size = value.len() as u64;
            database_stats.total_key_size += key_size;
            database_stats.total_value_size += value_size;
        }

        database_stats.number_of_entries = database.len(rtxn)?;

        Ok(database_stats)
    }

    /// Recomputes the stats of the database and returns the new stats.
    ///
    /// This function is used to update the stats of the database when some keys are modified.
    /// It is more efficient than the `new` function because it does not iterate over the whole database but only the modified keys comparing the before and after states.
    pub(crate) fn recompute<I, K>(
        mut stats: Self,
        database: Database<Bytes, Bytes>,
        before_rtxn: &RoTxn<'_>,
        after_rtxn: &RoTxn<'_>,
        modified_keys: I,
    ) -> heed::Result<Self>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        for key in modified_keys {
            let key = key.as_ref();
            if let Some(value) = database.get(after_rtxn, key)? {
                let key_size = key.len() as u64;
                let value_size = value.len() as u64;
                stats.total_key_size = stats.total_key_size.saturating_add(key_size);
                stats.total_value_size = stats.total_value_size.saturating_add(value_size);
            }

            if let Some(value) = database.get(before_rtxn, key)? {
                let key_size = key.len() as u64;
                let value_size = value.len() as u64;
                stats.total_key_size = stats.total_key_size.saturating_sub(key_size);
                stats.total_value_size = stats.total_value_size.saturating_sub(value_size);
            }
        }

        stats.number_of_entries = database.len(after_rtxn)?;

        Ok(stats)
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

    pub fn total_key_size(&self) -> u64 {
        self.total_key_size
    }

    pub fn total_value_size(&self) -> u64 {
        self.total_value_size
    }
}
