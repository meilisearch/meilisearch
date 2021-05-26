use std::{fs::File, io::BufReader, marker::PhantomData, path::Path};

use anyhow::Context;
use chrono::{DateTime, Utc};
use log::info;
use serde::{Deserialize, Serialize};

use crate::index_controller::uuid_resolver::store::UuidStore;

#[derive(Serialize, Deserialize, Debug)]
pub struct MetadataV2<U> {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    dump_date: DateTime<Utc>,
    _pth: PhantomData<U>,
}

impl<U> MetadataV2<U>
where U: UuidStore,
{
    pub fn load_dump(self, src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
        info!(
            "Loading dump from {}, dump database version: {}, dump version: V2",
            self.dump_date, self.db_version
        );
        // get dir in which to load the db:
        let dst_dir = dst
            .as_ref()
            .parent()
            .with_context(|| format!("Invalid db path: {}", dst.as_ref().display()))?;

        let tmp_dst = tempfile::tempdir_in(dst_dir)?;

        self.load_index_resolver(&src, tmp_dst.path())?;
        load_updates(&src, tmp_dst.path())?;
        load_indexes(&src, tmp_dst.path())?;
        Ok(())
    }

    fn load_index_resolver(
        &self,
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        info!("Loading index database.");
        let uuid_resolver_path = dst.as_ref().join("uuid_resolver/");
        std::fs::create_dir_all(&uuid_resolver_path)?;

        U::load_dump(src.as_ref(), dst.as_ref())?;

        Ok(())
    }
}


fn load_updates(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
    info!("Loading updates.");
    todo!()
}

fn load_indexes(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
    info!("Loading indexes");
    todo!()
}

// Extract Settings from `settings.json` file present at provided `dir_path`
//fn import_settings(dir_path: &Path) -> anyhow::Result<Settings<Checked>> {
//let path = dir_path.join("settings.json");
//let file = File::open(path)?;
//let reader = BufReader::new(file);
//let metadata: Settings<Unchecked> = serde_json::from_reader(reader)?;

//Ok(metadata.check())
//}

//pub fn import_dump(
//_db_size: usize,
//update_db_size: usize,
//_uuid: Uuid,
//dump_path: impl AsRef<Path>,
//db_path: impl AsRef<Path>,
//_primary_key: Option<&str>,
//) -> anyhow::Result<()> {
//info!("Dump import started.");
//info!("Importing outstanding updates...");

//import_updates(&dump_path, &db_path, update_db_size)?;

//info!("done importing updates");

//Ok(())
////let index_path = db_path.join(&format!("indexes/index-{}", uuid));
////std::fs::create_dir_all(&index_path)?;
////let mut options = EnvOpenOptions::new();
////options.map_size(size);
////let index = milli::Index::new(options, index_path)?;
////let index = Index(Arc::new(index));

////let mut txn = index.write_txn()?;

////info!("importing the settings...");
////// extract `settings.json` file and import content
////let settings = import_settings(&dump_path)?;
////let update_builder = UpdateBuilder::new(0);
////index.update_settings_txn(&mut txn, &settings, update_builder)?;

////// import the documents in the index
////let update_builder = UpdateBuilder::new(1);
////let file = File::open(&dump_path.join("documents.jsonl"))?;
////let reader = std::io::BufReader::new(file);

////info!("importing the documents...");
////// TODO: TAMO: currently we ignore any error caused by the importation of the documents because
////// if there is no documents nor primary key it'll throw an anyhow error, but we must remove
////// this before the merge on main
////index.update_documents_txn(
////&mut txn,
////UpdateFormat::JsonStream,
////IndexDocumentsMethod::ReplaceDocuments,
////Some(reader),
////update_builder,
////primary_key,
////)?;

////txn.commit()?;

////// the last step: we extract the original milli::Index and close it
////Arc::try_unwrap(index.0)
////.map_err(|_e| "[dumps] At this point no one is supposed to have a reference on the index")
////.unwrap()
////.prepare_for_closing()
////.wait();

////info!("importing the updates...");
////import_updates(dump_path, db_path)
//}

//fn import_updates(
//src_path: impl AsRef<Path>,
//dst_path: impl AsRef<Path>,
//_update_db_size: usize
//) -> anyhow::Result<()> {
//let dst_update_path = dst_path.as_ref().join("updates");
//std::fs::create_dir_all(&dst_update_path)?;

//let dst_update_files_path = dst_update_path.join("update_files");
//std::fs::create_dir_all(&dst_update_files_path)?;

//let options = EnvOpenOptions::new();
//let (update_store, _) = UpdateStore::create(options, &dst_update_path)?;

//let src_update_path = src_path.as_ref().join("updates");
//let src_update_files_path = src_update_path.join("update_files");
//let update_data = File::open(&src_update_path.join("data.jsonl"))?;
//let mut update_data = BufReader::new(update_data);

//let mut wtxn = update_store.env.write_txn()?;
//let mut line = String::new();
//loop {
//match update_data.read_line(&mut line) {
//Ok(_) => {
//let UpdateEntry { uuid, mut update } = serde_json::from_str(&line)?;

//if let Some(path) = update.content_path_mut() {
//let dst_file_path = dst_update_files_path.join(&path);
//let src_file_path = src_update_files_path.join(&path);
//*path = dst_update_files_path.join(&path);
//std::fs::copy(src_file_path, dst_file_path)?;
//}

//update_store.register_raw_updates(&mut wtxn, update, uuid)?;
//}
//_ => break,
//}
//}
//wtxn.commit()?;
//Ok(())
//}
