use heed::RwTxn;

use super::{UpgradeIndex, UpgradeParams};
use crate::{vector::db::migrate_embedder_info_format, Index, Result};

/// Migrate the embedder info from the old format.
pub(super) struct MigrateEmbedderInfoFormat();

impl UpgradeIndex for MigrateEmbedderInfoFormat {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, _params: UpgradeParams<'_>) -> Result<bool> {
        migrate_embedder_info_format(wtxn, index)?;

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 59, 0)
    }

    fn description(&self) -> &'static str {
        "Migrate embedder info format"
    }
}
