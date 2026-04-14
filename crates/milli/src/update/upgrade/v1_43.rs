use heed::RwTxn;

use super::UpgradeIndex;
use crate::update::upgrade::UpgradeParams;
use crate::{Index, Result};

/// Clear now unused databases
pub(super) struct ClearFieldDocidFacetDbs();

impl UpgradeIndex for ClearFieldDocidFacetDbs {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        UpgradeParams { .. }: UpgradeParams<'_>,
    ) -> Result<bool> {
        index.removed_dbs.clear_field_id_docid_facet_dbs(wtxn)?;

        Ok(true)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 43, 0)
    }

    fn description(&self) -> &'static str {
        "clearing removed databases"
    }
}
