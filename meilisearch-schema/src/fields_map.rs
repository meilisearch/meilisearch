use std::collections::HashMap;
use std::collections::hash_map::Iter;

use serde::{Deserialize, Serialize};

use crate::{SResult, FieldId};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FieldsMap {
    name_map: HashMap<String, FieldId>,
    id_map: HashMap<FieldId, String>,
    next_id: FieldId
}

impl FieldsMap {
    pub(crate) fn insert(&mut self, name: &str) -> SResult<FieldId> {
        if let Some(id) = self.name_map.get(name) {
            return Ok(*id)
        }
        let id = self.next_id;
        self.next_id = self.next_id.next()?;
        self.name_map.insert(name.to_string(), id);
        self.id_map.insert(id, name.to_string());
        Ok(id)
    }

    pub(crate) fn id(&self, name: &str) -> Option<FieldId> {
        self.name_map.get(name).copied()
    }

    pub(crate) fn name<I: Into<FieldId>>(&self, id: I) -> Option<&str> {
        self.id_map.get(&id.into()).map(|s| s.as_str())
    }

    pub(crate) fn iter(&self) -> Iter<'_, String, FieldId> {
        self.name_map.iter()
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
        assert_eq!(fields_map.id("id"), Some(0.into()));
        assert_eq!(fields_map.id("title"), Some(1.into()));
        assert_eq!(fields_map.id("descritpion"), Some(2.into()));
        assert_eq!(fields_map.id("date"), None);
        assert_eq!(fields_map.name(0), Some("id"));
        assert_eq!(fields_map.name(1), Some("title"));
        assert_eq!(fields_map.name(2), Some("descritpion"));
        assert_eq!(fields_map.name(4), None);
        assert_eq!(fields_map.insert("title").unwrap(), 1.into());
    }
}
