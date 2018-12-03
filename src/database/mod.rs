use std::error::Error;
use std::path::Path;

use rocksdb::rocksdb::DB;

use crate::index::update::Update;
use crate::database::database_view::DatabaseView;

pub mod document_key;
pub mod database_view;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

pub struct Database(DB);

impl Database {
    pub fn create(path: &Path) -> Result<Database, ()> {
        unimplemented!()
    }

    pub fn open(path: &Path) -> Result<Database, ()> {
        unimplemented!()
    }

    pub fn ingest_update_file(&self, update: Update) -> Result<(), ()> {
        unimplemented!()
    }

    pub fn view(&self) -> Result<DatabaseView, Box<Error>> {
        let snapshot = self.0.snapshot();
        DatabaseView::new(snapshot)
    }
}
