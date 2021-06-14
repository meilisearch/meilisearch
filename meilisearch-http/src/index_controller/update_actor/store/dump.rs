use std::{
    collections::HashSet,
    fs::{create_dir_all, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use heed::{EnvOpenOptions, RoTxn};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{State, UpdateStore, Result};
use crate::index_controller::{
    index_actor::IndexActorHandle, update_actor::store::update_uuid_to_file_path, Enqueued,
    UpdateStatus,
};

#[derive(Serialize, Deserialize)]
struct UpdateEntry {
    uuid: Uuid,
    update: UpdateStatus,
}

impl UpdateStore {
    pub fn dump(
        &self,
        uuids: &HashSet<Uuid>,
        path: PathBuf,
        handle: impl IndexActorHandle,
    ) -> Result<()> {
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
    ) -> Result<()> {
        let dump_data_path = path.as_ref().join("data.jsonl");
        let mut dump_data_file = File::create(dump_data_path)?;

        let update_files_path = path.as_ref().join(super::UPDATE_DIR);
        create_dir_all(&update_files_path)?;

        self.dump_pending(&txn, uuids, &mut dump_data_file, &path)?;
        self.dump_completed(&txn, uuids, &mut dump_data_file)?;

        Ok(())
    }

    fn dump_pending(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        mut file: &mut File,
        dst_path: impl AsRef<Path>,
    ) -> Result<()> {
        let pendings = self.pending_queue.iter(txn)?.lazily_decode_data();

        for pending in pendings {
            let ((_, uuid, _), data) = pending?;
            if uuids.contains(&uuid) {
                let update = data.decode()?;

                if let Some(ref update_uuid) = update.content {
                    let src = super::update_uuid_to_file_path(&self.path, *update_uuid);
                    let dst = super::update_uuid_to_file_path(&dst_path, *update_uuid);
                    std::fs::copy(src, dst)?;
                }

                let update_json = UpdateEntry {
                    uuid,
                    update: update.into(),
                };

                serde_json::to_writer(&mut file, &update_json)?;
                file.write_all(b"\n")?;
            }
        }

        Ok(())
    }

    fn dump_completed(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        mut file: &mut File,
    ) -> Result<()> {
        let updates = self.updates.iter(txn)?.lazily_decode_data();

        for update in updates {
            let ((uuid, _), data) = update?;
            if uuids.contains(&uuid) {
                let update = data.decode()?;

                let update_json = UpdateEntry { uuid, update };

                serde_json::to_writer(&mut file, &update_json)?;
                file.write_all(b"\n")?;
            }
        }

        Ok(())
    }

    pub fn load_dump(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        db_size: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dst_update_path = dst.as_ref().join("updates/");
        create_dir_all(&dst_update_path)?;

        let mut options = EnvOpenOptions::new();
        options.map_size(db_size as usize);
        let (store, _) = UpdateStore::new(options, &dst_update_path)?;

        let src_update_path = src.as_ref().join("updates");
        let update_data = File::open(&src_update_path.join("data.jsonl"))?;
        let mut update_data = BufReader::new(update_data);

        std::fs::create_dir_all(dst_update_path.join("update_files/"))?;

        let mut wtxn = store.env.write_txn()?;
        let mut line = String::new();
        loop {
            match update_data.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let UpdateEntry { uuid, update } = serde_json::from_str(&line)?;
                    store.register_raw_updates(&mut wtxn, &update, uuid)?;

                    // Copy ascociated update path if it exists
                    if let UpdateStatus::Enqueued(Enqueued {
                        content: Some(uuid),
                        ..
                    }) = update
                    {
                        let src = update_uuid_to_file_path(&src_update_path, uuid);
                        let dst = update_uuid_to_file_path(&dst_update_path, uuid);
                        std::fs::copy(src, dst)?;
                    }
                }
                _ => break,
            }

            line.clear();
        }

        wtxn.commit()?;

        Ok(())
    }
}

async fn dump_indexes(
    uuids: &HashSet<Uuid>,
    handle: impl IndexActorHandle,
    path: impl AsRef<Path>,
) -> Result<()> {
    for uuid in uuids {
        handle.dump(*uuid, path.as_ref().to_owned()).await?;
    }

    Ok(())
}
