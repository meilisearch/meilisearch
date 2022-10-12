// pub mod v2;
// pub mod v3;
// pub mod v4;
use crate::Result;

use self::{
    v4_to_v5::CompatV4ToV5,
    v5_to_v6::{CompatIndexV5ToV6, CompatV5ToV6},
};

use super::{
    v5::V5Reader,
    v6::{self, V6IndexReader, V6Reader},
    Document, UpdateFile,
};

pub mod v2_to_v3;
pub mod v3_to_v4;
pub mod v4_to_v5;
pub mod v5_to_v6;

pub enum Compat {
    Current(V6Reader),
    Compat(CompatV5ToV6),
}

impl Compat {
    pub fn version(&self) -> crate::Version {
        match self {
            Compat::Current(current) => current.version(),
            Compat::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            Compat::Current(current) => current.date(),
            Compat::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        match self {
            Compat::Current(current) => current.instance_uid(),
            Compat::Compat(compat) => compat.instance_uid(),
        }
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<CompatIndex>> + '_>> {
        match self {
            Compat::Current(current) => {
                let indexes = Box::new(current.indexes()?.map(|res| res.map(CompatIndex::from)))
                    as Box<dyn Iterator<Item = Result<CompatIndex>> + '_>;
                Ok(indexes)
            }
            Compat::Compat(compat) => {
                let indexes = Box::new(compat.indexes()?.map(|res| res.map(CompatIndex::from)))
                    as Box<dyn Iterator<Item = Result<CompatIndex>> + '_>;
                Ok(indexes)
            }
        }
    }

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(v6::Task, Option<Box<UpdateFile>>)>> + '_> {
        match self {
            Compat::Current(current) => current.tasks(),
            Compat::Compat(compat) => compat.tasks(),
        }
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<v6::Key>> + '_> {
        match self {
            Compat::Current(current) => current.keys(),
            Compat::Compat(compat) => compat.keys(),
        }
    }
}

impl From<V6Reader> for Compat {
    fn from(value: V6Reader) -> Self {
        Compat::Current(value)
    }
}

impl From<CompatV5ToV6> for Compat {
    fn from(value: CompatV5ToV6) -> Self {
        Compat::Compat(value)
    }
}

impl From<V5Reader> for Compat {
    fn from(value: V5Reader) -> Self {
        Compat::Compat(value.to_v6())
    }
}

impl From<CompatV4ToV5> for Compat {
    fn from(value: CompatV4ToV5) -> Self {
        Compat::Compat(value.to_v6())
    }
}

pub enum CompatIndex {
    Current(v6::V6IndexReader),
    Compat(CompatIndexV5ToV6),
}

impl CompatIndex {
    pub fn new_v6(v6: v6::V6IndexReader) -> CompatIndex {
        CompatIndex::Current(v6)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            CompatIndex::Current(v6) => v6.metadata(),
            CompatIndex::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        match self {
            CompatIndex::Current(v6) => v6
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
            CompatIndex::Compat(compat) => compat
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v6::Settings<v6::Checked>> {
        match self {
            CompatIndex::Current(v6) => v6.settings(),
            CompatIndex::Compat(compat) => compat.settings(),
        }
    }
}

impl From<V6IndexReader> for CompatIndex {
    fn from(value: V6IndexReader) -> Self {
        CompatIndex::Current(value)
    }
}

impl From<CompatIndexV5ToV6> for CompatIndex {
    fn from(value: CompatIndexV5ToV6) -> Self {
        CompatIndex::Compat(value)
    }
}
