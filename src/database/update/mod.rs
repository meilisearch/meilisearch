use std::path::{Path, PathBuf};

mod builder;

pub use self::builder::UpdateBuilder;

pub struct Update {
    sst_file: PathBuf,
}

impl Update {
    pub fn path(&self) -> &Path {
        &self.sst_file
    }
}
