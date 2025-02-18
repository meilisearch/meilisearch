use meilisearch_types::heed::types::Str;
use meilisearch_types::heed::{self, Database, Env, RoTxn, RwTxn};
use meilisearch_types::milli::heed_codec::version::VersionCodec;
use meilisearch_types::versioning;

use crate::upgrade::upgrade_index_scheduler;
use crate::Result;

/// The number of database used by queue itself
const NUMBER_OF_DATABASES: u32 = 1;
/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const VERSION: &str = "version";
}
mod entry_name {
    pub const MAIN: &str = "main";
}

#[derive(Clone)]
pub struct Versioning {
    pub version: Database<Str, VersionCodec>,
}

impl Versioning {
    pub const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn get_version(&self, rtxn: &RoTxn) -> Result<Option<(u32, u32, u32)>, heed::Error> {
        self.version.get(rtxn, entry_name::MAIN)
    }

    pub fn set_version(
        &self,
        wtxn: &mut RwTxn,
        version: (u32, u32, u32),
    ) -> Result<(), heed::Error> {
        self.version.put(wtxn, entry_name::MAIN, &version)
    }

    pub fn set_current_version(&self, wtxn: &mut RwTxn) -> Result<(), heed::Error> {
        let major = versioning::VERSION_MAJOR.parse().unwrap();
        let minor = versioning::VERSION_MINOR.parse().unwrap();
        let patch = versioning::VERSION_PATCH.parse().unwrap();
        self.set_version(wtxn, (major, minor, patch))
    }

    /// Return `Self` without checking anything about the version
    pub fn raw_new(env: &Env, wtxn: &mut RwTxn) -> Result<Self, heed::Error> {
        let version = env.create_database(wtxn, Some(db_name::VERSION))?;
        Ok(Self { version })
    }

    pub(crate) fn new(env: &Env, db_version: (u32, u32, u32)) -> Result<Self> {
        let mut wtxn = env.write_txn()?;
        let this = Self::raw_new(env, &mut wtxn)?;
        let from = match this.get_version(&wtxn)? {
            Some(version) => version,
            // fresh DB: use the db version
            None => {
                this.set_version(&mut wtxn, db_version)?;
                db_version
            }
        };
        wtxn.commit()?;

        let bin_major: u32 = versioning::VERSION_MAJOR.parse().unwrap();
        let bin_minor: u32 = versioning::VERSION_MINOR.parse().unwrap();
        let bin_patch: u32 = versioning::VERSION_PATCH.parse().unwrap();
        let to = (bin_major, bin_minor, bin_patch);

        if from != to {
            upgrade_index_scheduler(env, &this, from, to)?;
        }

        // Once we reach this point it means the upgrade process, if there was one is entirely finished
        // we can safely say we reached the latest version of the index scheduler
        let mut wtxn = env.write_txn()?;
        this.set_current_version(&mut wtxn)?;
        wtxn.commit()?;

        Ok(this)
    }
}
