use std::ops::Deref;
use std::sync::Arc;

use crate::index::{Checked, Settings};
use crate::index_controller::{
    error::Result, DumpInfo, IndexController, IndexMetadata, IndexStats, Stats,
};
use crate::option::Opt;

pub mod search;
mod updates;

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct DataInner {
    pub index_controller: IndexController,
    //pub api_keys: ApiKeys,
}

impl Data {
    pub fn new(options: Opt) -> anyhow::Result<Data> {
        let path = options.db_path.clone();

        let index_controller = IndexController::new(&path, &options)?;

        let inner = DataInner {
            index_controller,
        };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub async fn settings(&self, uid: String) -> Result<Settings<Checked>> {
        self.index_controller.settings(uid).await
    }

    pub async fn list_indexes(&self) -> Result<Vec<IndexMetadata>> {
        self.index_controller.list_indexes().await
    }

    pub async fn index(&self, uid: String) -> Result<IndexMetadata> {
        self.index_controller.get_index(uid).await
    }

    //pub async fn create_index(
        //&self,
        //uid: String,
        //primary_key: Option<String>,
    //) -> Result<IndexMetadata> {
        //let settings = IndexSettings {
            //uid: Some(uid),
            //primary_key,
        //};

        //let meta = self.index_controller.create_index(settings).await?;
        //Ok(meta)
    //}

    pub async fn get_index_stats(&self, uid: String) -> Result<IndexStats> {
        Ok(self.index_controller.get_index_stats(uid).await?)
    }

    pub async fn get_all_stats(&self) -> Result<Stats> {
        Ok(self.index_controller.get_all_stats().await?)
    }

    pub async fn create_dump(&self) -> Result<DumpInfo> {
        Ok(self.index_controller.create_dump().await?)
    }

    pub async fn dump_status(&self, uid: String) -> Result<DumpInfo> {
        Ok(self.index_controller.dump_info(uid).await?)
    }
}
