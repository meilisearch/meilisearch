use std::sync::Arc;
use std::ops::Deref;

#[derive(Clone)]
pub struct CustomSettings(pub Arc<rocksdb::DB>, pub String);

impl Deref for CustomSettings {
    type Target = rocksdb::DB;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
