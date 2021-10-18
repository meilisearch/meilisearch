use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

use heed::{EnvOpenOptions, RoTxn};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use tempfile::{NamedTempFile, TempDir};
use uuid::Uuid;

use super::{Result, State, UpdateStore};
use crate::{
    index::Index,
    index_controller::{
        update_file_store::UpdateFileStore,
        updates::status::{Enqueued, UpdateStatus},
    },
    Update,
};

#[derive(Serialize, Deserialize)]
pub struct UpdateEntry {
    pub uuid: Uuid,
    pub update: UpdateStatus,
}

impl UpdateStore {
    pub fn dump(&self, indexes: &[Index], path: PathBuf) -> Result<()> {
        let state_lock = self.state.write();
        state_lock.swap(State::Dumping);

        // txn must *always* be acquired after state lock, or it will dead lock.
        let txn = self.env.write_txn()?;

        let uuids = indexes.iter().map(|i| i.uuid()).collect();

        self.dump_updates(&txn, &uuids, &path)?;

        indexes
            .par_iter()
            .try_for_each(|index| index.dump(&path))
            .unwrap();

        Ok(())
    }

    fn dump_updates(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let mut dump_data_file = NamedTempFile::new_in(&path)?;

        self.dump_pending(txn, uuids, &mut dump_data_file, &path)?;
        self.dump_completed(txn, uuids, &mut dump_data_file)?;

        let mut dst_path = path.as_ref().join("updates");
        create_dir_all(&dst_path)?;
        dst_path.push("data.jsonl");
        dump_data_file.persist(dst_path).unwrap();

        Ok(())
    }

    fn dump_pending(
        &self,
        txn: &RoTxn,
        uuids: &HashSet<Uuid>,
        mut file: impl Write,
        dst_path: impl AsRef<Path>,
    ) -> Result<()> {
        let pendings = self.pending_queue.iter(txn)?.lazily_decode_data();

        for pending in pendings {
            let ((_, uuid, _), data) = pending?;
            if uuids.contains(&uuid) {
                let update = data.decode()?;

                if let Enqueued {
                    meta: Update::DocumentAddition { content_uuid, .. },
                    ..
                } = update
                {
                    self.update_file_store
                        .dump(content_uuid, &dst_path)
                        .unwrap();
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
        mut file: impl Write,
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
    ) -> anyhow::Result<()> {
        let mut options = EnvOpenOptions::new();
        options.map_size(db_size as usize);

        // create a dummy update fiel store, since it is not needed right now.
        let tmp = TempDir::new().unwrap();
        let update_file_store = UpdateFileStore::new(tmp.path()).unwrap();
        let (store, _) = UpdateStore::new(options, &dst, update_file_store)?;

        let src_update_path = src.as_ref().join("updates");
        let update_data = File::open(&src_update_path.join("data.jsonl"))?;
        let update_data = BufReader::new(update_data);

        let stream = Deserializer::from_reader(update_data).into_iter::<UpdateEntry>();
        let mut wtxn = store.env.write_txn()?;

        for entry in stream {
            let UpdateEntry { uuid, update } = entry?;
            store.register_raw_updates(&mut wtxn, &update, uuid)?;
        }

        wtxn.commit()?;

        Ok(())
    }
}
