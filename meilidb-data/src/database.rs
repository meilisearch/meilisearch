use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Database(sled::Db);

impl Database {
    pub fn start_default<P: AsRef<Path>>(path: P) -> sled::Result<Database> {
        sled::Db::start_default(path).map(Database)
    }

    pub fn open_index(&self, name: &str) -> sled::Result<Index> {
        let name = format!("index-{}", name);
        let bytes = name.into_bytes();

        self.0.open_tree(bytes).map(Index)
    }
}

#[derive(Debug, Clone)]
pub struct Index(Arc<sled::Tree>);
