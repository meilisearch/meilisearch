use std::io::{Read, Write};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{SResult, FieldId};


#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldsMap {
    name_map: HashMap<String, FieldId>,
    id_map: HashMap<FieldId, String>,
    next_id: FieldId
}

impl FieldsMap {
    pub fn len(&self) -> usize {
        self.name_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.name_map.is_empty()
    }

    pub fn insert<S: Into<String>>(&mut self, name: S) -> SResult<FieldId> {
        let name = name.into();
        if let Some(id) = self.name_map.get(&name) {
            return Ok(*id)
        }
        let id = self.next_id.into();
        self.next_id = self.next_id.next()?;
        self.name_map.insert(name.clone(), id);
        self.id_map.insert(id, name);
        Ok(id)
    }

    pub fn remove<S: Into<String>>(&mut self, name: S) {
        let name = name.into();
        if let Some(id) = self.name_map.get(&name) {
            self.id_map.remove(&id);
        }
        self.name_map.remove(&name);
    }

    pub fn get_id<S: Into<String>>(&self, name: S) -> Option<FieldId> {
        let name = name.into();
        self.name_map.get(&name).map(|s| *s)
    }

    pub fn get_name<I: Into<FieldId>>(&self, id: I) -> Option<String> {
        self.id_map.get(&id.into()).map(|s| s.to_string())
    }

    pub fn read_from_bin<R: Read>(reader: R) -> bincode::Result<FieldsMap> {
        bincode::deserialize_from(reader)
    }

    pub fn write_to_bin<W: Write>(&self, writer: W) -> bincode::Result<()> {
        bincode::serialize_into(writer, &self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_map() {
        let mut fields_map = FieldsMap::default();
        assert_eq!(fields_map.insert("id").unwrap(), 0.into());
        assert_eq!(fields_map.insert("title").unwrap(), 1.into());
        assert_eq!(fields_map.insert("descritpion").unwrap(), 2.into());
        assert_eq!(fields_map.insert("id").unwrap(), 0.into());
        assert_eq!(fields_map.insert("title").unwrap(), 1.into());
        assert_eq!(fields_map.insert("descritpion").unwrap(), 2.into());
        assert_eq!(fields_map.get_id("id"), Some(0.into()));
        assert_eq!(fields_map.get_id("title"), Some(1.into()));
        assert_eq!(fields_map.get_id("descritpion"), Some(2.into()));
        assert_eq!(fields_map.get_id("date"), None);
        assert_eq!(fields_map.len(), 3);
        assert_eq!(fields_map.get_name(0), Some("id".to_owned()));
        assert_eq!(fields_map.get_name(1), Some("title".to_owned()));
        assert_eq!(fields_map.get_name(2), Some("descritpion".to_owned()));
        assert_eq!(fields_map.get_name(4), None);
        fields_map.remove("title");
        assert_eq!(fields_map.get_id("title"), None);
        assert_eq!(fields_map.insert("title").unwrap(), 3.into());
        assert_eq!(fields_map.len(), 3);
    }
}
