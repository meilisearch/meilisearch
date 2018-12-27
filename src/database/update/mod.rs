use std::path::PathBuf;
use std::error::Error;

mod negative;
mod positive;

pub use self::positive::{PositiveUpdateBuilder, NewState, SerializerError};
pub use self::negative::NegativeUpdateBuilder;

pub struct Update {
    path: PathBuf,
    can_be_moved: bool,
}

impl Update {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into(), can_be_moved: false })
    }

    pub fn open_and_move<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into(), can_be_moved: true })
    }

    pub fn set_move(&mut self, can_be_moved: bool) {
        self.can_be_moved = can_be_moved
    }

    pub fn can_be_moved(&self) -> bool {
        self.can_be_moved
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.path
    }
}
