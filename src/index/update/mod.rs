use std::path::PathBuf;
use std::error::Error;

mod negative_update;
mod positive_update;

pub use self::positive_update::{PositiveUpdateBuilder, NewState};
pub use self::negative_update::NegativeUpdateBuilder;

pub struct Update {
    path: PathBuf,
}

impl Update {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into() })
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.path
    }
}
