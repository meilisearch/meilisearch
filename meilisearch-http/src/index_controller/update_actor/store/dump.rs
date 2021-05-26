use std::{
    collections::HashSet,
    fs::{copy, create_dir_all, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Context;
use heed::RoTxn;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{State, codec::UpdateKeyCodec};
use super::UpdateStore;
use crate::index_controller::{index_actor::IndexActorHandle, UpdateStatus};

#[derive(Serialize, Deserialize)]
pub struct UpdateEntry {
    uuid: Uuid,
    update: UpdateStatus,
}

impl UpdateStore {
    pub fn dump(
        &self,
        uuids: &HashSet<Uuid>,
        path: PathBuf,
        handle: impl IndexActorHandle,
    ) -> anyhow::Result<()> {
        let state_lock = self.state.write();
        state_lock.swap(State::Dumping);

        // txn must *always* be acquired after state lock, or it will dead lock.
        let txn = self.env.write_txn()?;

        let dump_path = path.join("updates");
        create_dir_all(&dump_path)?;

        self.dump_updates(&txn, uuids, &dump_path)?;

        let fut = dump_indexes(uuids, handle, &path);
        tokio::runtime::Handle::current().block_on(fut)?;

        state_lock.swap(State::Idle);

        Ok(())
    }

    fn dump_updates(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let dump_data_path = path.as_ref().join("data.jsonl");
        let mut dump_data_file = File::create(dump_data_path)?;

        let update_files_path = path.as_ref().join("update_files");
        create_dir_all(&update_files_path)?;

        self.dump_pending(&txn, uuids, &mut dump_data_file, &update_files_path)?;
        self.dump_completed(&txn, uuids, &mut dump_data_file)?;

        Ok(())
    }

    fn dump_pending(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        mut file: &mut File,
        update_files_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let pendings = self.pending_queue.iter(txn)?.lazily_decode_data();

        for pending in pendings {
            let ((_, uuid, _), data) = pending?;
            if uuids.contains(&uuid) {
                let mut update = data.decode()?;

                if let Some(content) = update.content.take() {
                    update.content = Some(dump_update_file(content, &update_files_path)?);
                }

                let update_json = UpdateEntry {
                    uuid,
                    update: update.into(),
                };

                serde_json::to_writer(&mut file, &update_json)?;
                file.write(b"\n")?;
            }
        }

        Ok(())
    }

    fn dump_completed(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        mut file: &mut File,
    ) -> anyhow::Result<()> {
        let updates = self
            .updates
            .iter(txn)?
            .remap_key_type::<UpdateKeyCodec>()
            .lazily_decode_data();

        for update in updates {
            let ((uuid, _), data) = update?;
            if uuids.contains(&uuid) {
                let update = data.decode()?.into();

                let update_json = UpdateEntry { uuid, update };

                serde_json::to_writer(&mut file, &update_json)?;
                file.write(b"\n")?;
            }
        }

        Ok(())
    }
}

async fn dump_indexes(uuids: &HashSet<Uuid>, handle: impl IndexActorHandle, path: impl AsRef<Path>)-> anyhow::Result<()> {
    for uuid in uuids {
        handle.dump(*uuid, path.as_ref().to_owned()).await?;
    }

    Ok(())
}

fn dump_update_file(
    file_path: impl AsRef<Path>,
    dump_path: impl AsRef<Path>,
) -> anyhow::Result<PathBuf> {
    let filename: PathBuf = file_path
        .as_ref()
        .file_name()
        .context("invalid update file name")?
        .into();
    let dump_file_path = dump_path.as_ref().join(&filename);
    copy(file_path, dump_file_path)?;
    Ok(filename)
}
