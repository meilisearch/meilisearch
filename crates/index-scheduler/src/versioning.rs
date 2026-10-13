use meilisearch_types::heed::types::Str;
use meilisearch_types::heed::{
    self, CommitOrAbort, Database, DatabaseOpener, Env, RoTxn, RwTxn, WithoutTls,
};
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
pub struct Versioning<'e> {
    pub version: Database<'e, Str, VersionCodec>,
}

impl<'e> Versioning<'e> {
    pub const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn used_size(&self, rtxn: &RoTxn) -> Result<u64> {
        Ok(self.version.stat(rtxn)?.non_free_page_size() as u64)
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
        let major = versioning::VERSION_MAJOR;
        let minor = versioning::VERSION_MINOR;
        let patch = versioning::VERSION_PATCH;
        self.set_version(wtxn, (major, minor, patch))
    }

    /// Return `Self` without checking anything about the version
    pub fn raw_new(
        dbo: &DatabaseOpener<'e, WithoutTls>,
        wtxn: &mut RwTxn<'e>,
    ) -> Result<Self, heed::Error> {
        let version = dbo.create_database(wtxn, Some(db_name::VERSION))?;
        Ok(Self { version })
    }

    pub(crate) fn new(env: &'e Env<WithoutTls>, db_version: (u32, u32, u32)) -> Result<Self> {
        let wtxn = env.write_txn()?;
        let mut from = None;
        let this = env
            .create_databases_and_commit(wtxn, |dbo, wtxn| {
                let this = Self::raw_new(dbo, wtxn)?;
                from = match this.get_version(wtxn)? {
                    Some(version) => Some(version),
                    // fresh DB: use the db version
                    None => {
                        this.set_version(wtxn, db_version)?;
                        Some(db_version)
                    }
                };

                Ok(CommitOrAbort::<_, ()>::Commit(this))
            })?
            .unwrap_commit();

        let from = from.unwrap();
        upgrade_index_scheduler(env, &this, from)?;

        // Once we reach this point it means the upgrade process, if there was one is entirely finished
        // we can safely say we reached the latest version of the index scheduler
        let mut wtxn = env.write_txn()?;
        this.set_current_version(&mut wtxn)?;
        wtxn.commit()?;

        Ok(this)
    }
}

impl<'e> meilisearch_types::heed::OnCommit for Versioning<'e> {
    type Committed = Versioning<'static>;

    fn on_commit(self, token: &heed::CommitToken) -> Self::Committed {
        let Self { version } = self;
        Versioning { version: version.on_commit(token) }
    }
}
