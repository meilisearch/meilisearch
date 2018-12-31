use std::path::{Path, PathBuf};

mod builder;
mod raw_builder;

pub use self::builder::UpdateBuilder;
pub use self::raw_builder::RawUpdateBuilder;

pub struct Update {
    sst_file: PathBuf,
}

impl Update {
    pub fn path(&self) -> &Path {
        &self.sst_file
    }
}
