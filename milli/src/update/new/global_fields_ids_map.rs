use std::sync::{Arc, RwLock};

use crate::{FieldId, FieldsIdsMap};

/// A fields ids map that can be globally updated to add fields
pub struct GlobalFieldsIdsMap {
    global: Arc<RwLock<FieldsIdsMap>>,
    local: FieldsIdsMap,
}

impl GlobalFieldsIdsMap {
    pub fn new(global: FieldsIdsMap) -> Self {
        Self { local: global.clone(), global: Arc::new(RwLock::new(global)) }
    }

    /// Returns the number of fields ids in the map.
    pub fn global_len(&self) -> usize {
        todo!()
    }

    /// Returns `true` if the map is empty.
    pub fn global_is_empty(&self) -> bool {
        todo!()
    }

    /// Returns the field id related to a field name, it will create a new field id if the
    /// name is not already known. Returns `None` if the maximum field id as been reached.
    pub fn insert(&mut self, name: &str) -> Option<FieldId> {
        match self.names_ids.get(name) {
            Some(id) => Some(*id),
            None => {
                let id = self.next_id?;
                self.next_id = id.checked_add(1);
                self.names_ids.insert(name.to_owned(), id);
                self.ids_names.insert(id, name.to_owned());
                Some(id)
            }
        }
    }

    /// Get the id of a field based on its name.
    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.names_ids.get(name).copied()
    }

    /// Get the name of a field based on its id.
    pub fn name(&self, id: FieldId) -> Option<&str> {
        self.ids_names.get(&id).map(String::as_str)
    }

    /// Iterate over the ids and names in the ids order.
    pub fn iter(&self) -> impl Iterator<Item = (FieldId, &str)> {
        self.ids_names.iter().map(|(id, name)| (*id, name.as_str()))
    }

    /// Iterate over the ids in the order of the ids.
    pub fn ids(&'_ self) -> impl Iterator<Item = FieldId> + '_ {
        self.ids_names.keys().copied()
    }

    /// Iterate over the names in the order of the ids.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.ids_names.values().map(AsRef::as_ref)
    }
}
