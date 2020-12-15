mod error;
mod fields_map;
mod schema;
mod position_map;

pub use error::{Error, SResult};
use fields_map::FieldsMap;
pub use schema::Schema;
use serde::{Deserialize, Serialize};
use zerocopy::{AsBytes, FromBytes};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct IndexedPos(pub u16);

impl IndexedPos {
    pub const fn new(value: u16) -> IndexedPos {
        IndexedPos(value)
    }

    pub const fn min() -> IndexedPos {
        IndexedPos(u16::min_value())
    }

    pub const fn max() -> IndexedPos {
        IndexedPos(u16::max_value())
    }
}

impl From<u16> for IndexedPos {
    fn from(value: u16) -> IndexedPos {
        IndexedPos(value)
    }
}

impl Into<u16> for IndexedPos {
    fn into(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, Default, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
#[derive(AsBytes, FromBytes)]
#[repr(C)]
pub struct FieldId(pub u16);

impl FieldId {
    pub const fn new(value: u16) -> FieldId {
        FieldId(value)
    }

    pub const fn min() -> FieldId {
        FieldId(u16::min_value())
    }

    pub const fn max() -> FieldId {
        FieldId(u16::max_value())
    }

    pub fn next(self) -> SResult<FieldId> {
        self.0.checked_add(1).map(FieldId).ok_or(Error::MaxFieldsLimitExceeded)
    }
}

impl From<u16> for FieldId {
    fn from(value: u16) -> FieldId {
        FieldId(value)
    }
}

impl From<FieldId> for u16 {
    fn from(other: FieldId) -> u16 {
        other.0
    }
}
