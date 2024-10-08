use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::{FieldId, FieldsIdsMap};

/// A fields ids map that can be globally updated to add fields
#[derive(Debug, Clone)]
pub struct GlobalFieldsIdsMap<'indexing> {
    global: &'indexing RwLock<FieldsIdsMap>,
    local: LocalFieldsIdsMap,
}

#[derive(Debug, Clone)]
struct LocalFieldsIdsMap {
    names_ids: BTreeMap<String, FieldId>,
    ids_names: BTreeMap<FieldId, String>,
}

impl LocalFieldsIdsMap {
    fn new(global: &RwLock<FieldsIdsMap>) -> Self {
        let global = global.read().unwrap();
        Self { names_ids: global.names_ids.clone(), ids_names: global.ids_names.clone() }
    }

    fn insert(&mut self, name: &str, field_id: FieldId) {
        self.names_ids.insert(name.to_owned(), field_id);
        self.ids_names.insert(field_id, name.to_owned());
    }

    fn name(&self, id: FieldId) -> Option<&str> {
        self.ids_names.get(&id).map(String::as_str)
    }

    fn id(&self, name: &str) -> Option<FieldId> {
        self.names_ids.get(name).copied()
    }
}

impl<'indexing> GlobalFieldsIdsMap<'indexing> {
    pub fn new(global: &'indexing RwLock<FieldsIdsMap>) -> Self {
        Self { local: LocalFieldsIdsMap::new(global), global }
    }

    /// Returns the field id related to a field name, it will create a new field id if the
    /// name is not already known. Returns `None` if the maximum field id as been reached.
    pub fn id_or_insert(&mut self, name: &str) -> Option<FieldId> {
        if let Some(field_id) = self.local.id(name) {
            return Some(field_id);
        }

        {
            // optimistically lookup the global map
            let global = self.global.read().unwrap();

            if let Some(field_id) = global.id(name) {
                self.local.insert(name, field_id);
                return Some(field_id);
            }
        }

        {
            let mut global = self.global.write().unwrap();

            if let Some(field_id) = global.id(name) {
                self.local.insert(name, field_id);
                return Some(field_id);
            }

            let field_id = global.insert(name)?;
            self.local.insert(name, field_id);
            Some(field_id)
        }
    }

    /// Get the name of a field based on its id.
    pub fn name(&mut self, id: FieldId) -> Option<&str> {
        if self.local.name(id).is_none() {
            let global = self.global.read().unwrap();

            let name = global.name(id)?;
            self.local.insert(name, id);
        }

        self.local.name(id)
    }
}
