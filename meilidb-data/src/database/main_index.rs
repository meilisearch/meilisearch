use std::sync::Arc;

use crate::ranked_map::RankedMap;
use crate::schema::Schema;

use super::Error;

#[derive(Clone)]
pub struct MainIndex(pub(crate) Arc<sled::Tree>);

impl MainIndex {
    pub fn schema(&self) -> Result<Option<Schema>, Error> {
        match self.0.get("schema")? {
            Some(bytes) => {
                let schema = Schema::read_from_bin(bytes.as_ref())?;
                Ok(Some(schema))
            },
            None => Ok(None),
        }
    }

    pub fn set_schema(&self, schema: &Schema) -> Result<(), Error> {
        let mut bytes = Vec::new();
        schema.write_to_bin(&mut bytes)?;
        self.0.set("schema", bytes)?;
        Ok(())
    }

    pub fn words_set(&self) -> Result<Option<fst::Set>, Error> {
        match self.0.get("words")? {
            Some(bytes) => {
                let len = bytes.len();
                let value = bytes.into();
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            None => Ok(None),
        }
    }

    pub fn set_words_set(&self, value: &fst::Set) -> Result<(), Error> {
        self.0.set("words", value.as_fst().as_bytes())?;
        Ok(())
    }

    pub fn ranked_map(&self) -> Result<Option<RankedMap>, Error> {
        match self.0.get("ranked-map")? {
            Some(bytes) => {
                let ranked_map = RankedMap::read_from_bin(bytes.as_ref())?;
                Ok(Some(ranked_map))
            },
            None => Ok(None),
        }
    }

    pub fn set_ranked_map(&self, value: &RankedMap) -> Result<(), Error> {
        let mut bytes = Vec::new();
        value.write_to_bin(&mut bytes)?;
        self.0.set("ranked_map", bytes)?;
        Ok(())
    }
}
