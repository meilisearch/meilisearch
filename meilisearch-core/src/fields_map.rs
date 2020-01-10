use std::io::{Read, Write};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use crate::{MResult, Error};


#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldsMap {
    name_map: HashMap<String, u16>,
    id_map: HashMap<u16, String>,
    next_id: u16
}

impl FieldsMap {
    pub fn len(&self) -> usize {
        self.name_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.name_map.is_empty()
    }

    pub fn insert<T: ToString>(&mut self, name: T) -> MResult<u16> {
        let name = name.to_string();
        if let Some(id) = self.name_map.get(&name) {
            return Ok(*id)
        }
        let id = self.next_id;
        if self.next_id.checked_add(1).is_none() {
            return Err(Error::MaxFieldsLimitExceeded)
        } else {
            self.next_id += 1;
        }
        self.name_map.insert(name.clone(), id);
        self.id_map.insert(id, name);
        Ok(id)
    }

    pub fn remove<T: ToString>(&mut self, name: T) {
        let name = name.to_string();
        if let Some(id) = self.name_map.get(&name) {
            self.id_map.remove(&id);
        }
        self.name_map.remove(&name);
    }

    pub fn get_id<T: ToString>(&self, name: T) -> Option<&u16> {
        let name = name.to_string();
        self.name_map.get(&name)
    }

    pub fn get_name(&self, id: u16) -> Option<&String> {
        self.id_map.get(&id)
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

        assert_eq!(fields_map.insert("id").unwrap(), 0);
        assert_eq!(fields_map.insert("title").unwrap(), 1);
        assert_eq!(fields_map.insert("descritpion").unwrap(), 2);
        assert_eq!(fields_map.insert("id").unwrap(), 0);
        assert_eq!(fields_map.insert("title").unwrap(), 1);
        assert_eq!(fields_map.insert("descritpion").unwrap(), 2);
        assert_eq!(fields_map.get_id("id"), Some(&0));
        assert_eq!(fields_map.get_id("title"), Some(&1));
        assert_eq!(fields_map.get_id("descritpion"), Some(&2));
        assert_eq!(fields_map.get_id("date"), None);
        assert_eq!(fields_map.len(), 3);
        assert_eq!(fields_map.get_name(0), Some(&"id".to_owned()));
        assert_eq!(fields_map.get_name(1), Some(&"title".to_owned()));
        assert_eq!(fields_map.get_name(2), Some(&"descritpion".to_owned()));
        assert_eq!(fields_map.get_name(4), None);
        fields_map.remove("title");
        assert_eq!(fields_map.get_id("title"), None);
        assert_eq!(fields_map.insert("title").unwrap(), 3);
        assert_eq!(fields_map.len(), 3);
    }
}
