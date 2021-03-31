use std::convert::TryFrom;
use std::fmt;

/// This is just before the lowest printable character (space, sp, 32)
const MAX_VALUE: u8 = 31;

#[derive(Debug, Copy, Clone)]
pub enum Error {
    LevelTooHigh(u8),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TreeLevel(u8);

impl TreeLevel {
    pub const fn max_value() -> TreeLevel {
        TreeLevel(MAX_VALUE)
    }

    pub const fn min_value() -> TreeLevel {
        TreeLevel(0)
    }

    pub fn saturating_sub(&self, lhs: u8) -> TreeLevel {
        TreeLevel(self.0.saturating_sub(lhs))
    }
}

impl Into<u8> for TreeLevel {
    fn into(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for TreeLevel {
    type Error = Error;

    fn try_from(value: u8) -> Result<TreeLevel, Error> {
        match value {
            0..=MAX_VALUE => Ok(TreeLevel(value)),
            _ => Err(Error::LevelTooHigh(value)),
        }
    }
}

impl fmt::Display for TreeLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
