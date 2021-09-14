use crate::index_controller::Update;
use crate::index_controller::{error::Result, IndexMetadata, IndexSettings, UpdateStatus};
use crate::Data;

impl Data {
    pub async fn register_update(&self, index_uid: &str, update: Update) -> Result<UpdateStatus> {
        let status = self.index_controller.register_update(index_uid, update).await?;
        Ok(status)
    }

    pub async fn get_update_status(&self, index: String, uid: u64) -> Result<UpdateStatus> {
        self.index_controller.update_status(index, uid).await
    }

    pub async fn get_updates_status(&self, index: String) -> Result<Vec<UpdateStatus>> {
        self.index_controller.all_update_status(index).await
    }

    pub async fn update_index(
        &self,
        uid: String,
        primary_key: Option<String>,
        new_uid: Option<String>,
    ) -> Result<IndexMetadata> {
        let settings = IndexSettings {
            uid: new_uid,
            primary_key,
        };

        self.index_controller.update_index(uid, settings).await
    }
}
