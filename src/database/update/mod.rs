use std::path::PathBuf;
use std::error::Error;

mod negative;
mod positive;

pub use self::positive::{PositiveUpdateBuilder, NewState};
pub use self::negative::NegativeUpdateBuilder;

/// Represent an update that can be ingested by the database.
pub struct Update {
    path: PathBuf,
    can_be_moved: bool,
}

impl Update {
    /// Create an update from the file path where it is located.
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into(), can_be_moved: false })
    }

    /// Create an update from a file path and specify that it can be moved.
    pub fn open_and_move<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into(), can_be_moved: true })
    }

    /// Change the status of moveability of the update.
    pub fn set_move(&mut self, can_be_moved: bool) {
        self.can_be_moved = can_be_moved
    }

    /// Returns true if the update can be moved.
    pub fn can_be_moved(&self) -> bool {
        self.can_be_moved
    }

    /// Convert the update into its internal path.
    pub fn into_path_buf(self) -> PathBuf {
        self.path
    }
}
