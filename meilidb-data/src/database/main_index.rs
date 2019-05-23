use std::sync::Arc;

use crate::ranked_map::RankedMap;
use crate::schema::Schema;

use super::Error;

#[derive(Clone)]
pub struct MainIndex(pub Arc<rocksdb::DB>, pub String);

impl MainIndex {
    pub fn schema(&self) -> Result<Option<Schema>, Error> {
        let cf = self.0.cf_handle(&self.1).unwrap();
        match self.0.get_cf(cf, "schema")? {
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
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.put_cf(cf, "schema", bytes)?;
        Ok(())
    }

    pub fn words_set(&self) -> Result<Option<fst::Set>, Error> {
        let cf = self.0.cf_handle(&self.1).unwrap();
        match self.0.get_pinned_cf(cf, "words")? {
            Some(bytes) => {
                let len = bytes.len();
                let value = Arc::from(bytes.as_ref());
                let fst = fst::raw::Fst::from_shared_bytes(value, 0, len)?;
                Ok(Some(fst::Set::from(fst)))
            },
            None => Ok(None),
        }
    }

    pub fn set_words_set(&self, value: &fst::Set) -> Result<(), Error> {
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.put_cf(cf, "words", value.as_fst().as_bytes())?;
        Ok(())
    }

    pub fn ranked_map(&self) -> Result<Option<RankedMap>, Error> {
        let cf = self.0.cf_handle(&self.1).unwrap();
        match self.0.get_cf(cf, "ranked-map")? {
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
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.put_cf(cf, "ranked_map", bytes)?;
        Ok(())
    }
}
