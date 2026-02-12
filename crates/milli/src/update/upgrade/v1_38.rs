use crate::update::upgrade::UpgradeParams;

pub struct AddShards {}

impl super::UpgradeIndex for AddShards {
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 38, 0)
    }

    fn upgrade(
        &self,
        wtxn: &mut heed::RwTxn,
        index: &crate::Index,
        UpgradeParams { shards, .. }: UpgradeParams<'_>,
    ) -> crate::Result<bool> {
        let Some(shards) = shards else {
            return Ok(false);
        };

        // before this upgrade, there is at most one shard owned by the remote.
        // if we find it, we can associate all docids to that shard.
        let Some(own_shard) = shards.as_sorted_slice().iter().find(|shard| shard.is_own) else {
            return Ok(false);
        };

        let shard_docids = index.shard_docids();

        let docids = index.documents_ids(wtxn)?;

        shard_docids.put_docids(wtxn, &own_shard.name, &docids)?;
        Ok(false)
    }

    fn description(&self) -> &'static str {
        "adding shards to network objects"
    }
}
