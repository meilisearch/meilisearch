use std::collections::BTreeMap;
use std::sync::RwLock;

use super::metadata::{FieldIdMapWithMetadata, Metadata};
use super::MutFieldIdMapper;
use crate::FieldId;

/// A fields ids map that can be globally updated to add fields
#[derive(Debug, Clone)]
pub struct GlobalFieldsIdsMap<'indexing> {
    global: &'indexing RwLock<FieldIdMapWithMetadata>,
    local: LocalFieldsIdsMap,
}

#[derive(Debug, Clone)]
struct LocalFieldsIdsMap {
    names_ids: BTreeMap<String, FieldId>,
    ids_names: BTreeMap<FieldId, String>,
    metadata: BTreeMap<FieldId, Metadata>,
}

impl LocalFieldsIdsMap {
    fn new(global: &RwLock<FieldIdMapWithMetadata>) -> Self {
        let global = global.read().unwrap();
        Self {
            names_ids: global.as_fields_ids_map().names_ids.clone(),
            ids_names: global.as_fields_ids_map().ids_names.clone(),
            metadata: global.iter_id_metadata().collect(),
        }
    }

    fn insert(&mut self, name: &str, field_id: FieldId, metadata: Metadata) {
        self.names_ids.insert(name.to_owned(), field_id);
        self.ids_names.insert(field_id, name.to_owned());
        self.metadata.insert(field_id, metadata);
    }

    fn name(&self, id: FieldId) -> Option<&str> {
        self.ids_names.get(&id).map(String::as_str)
    }

    fn id(&self, name: &str) -> Option<FieldId> {
        self.names_ids.get(name).copied()
    }

    fn id_with_metadata(&self, name: &str) -> Option<(FieldId, Metadata)> {
        let id = self.id(name)?;
        Some((id, self.metadata(id).unwrap()))
    }

    fn metadata(&self, id: FieldId) -> Option<Metadata> {
        self.metadata.get(&id).copied()
    }
}

impl<'indexing> GlobalFieldsIdsMap<'indexing> {
    pub fn new(global: &'indexing RwLock<FieldIdMapWithMetadata>) -> Self {
        Self { local: LocalFieldsIdsMap::new(global), global }
    }

    /// Returns the field id related to a field name, it will create a new field id if the
    /// name is not already known. Returns `None` if the maximum field id as been reached.
    pub fn id_or_insert(&mut self, name: &str) -> Option<FieldId> {
        self.id_with_metadata_or_insert(name).map(|(fid, _meta)| fid)
    }

    pub fn id_with_metadata_or_insert(&mut self, name: &str) -> Option<(FieldId, Metadata)> {
        if let Some(entry) = self.local.id_with_metadata(name) {
            return Some(entry);
        }

        {
            // optimistically lookup the global map
            let global = self.global.read().unwrap();

            if let Some((field_id, metadata)) = global.id_with_metadata(name) {
                self.local.insert(name, field_id, metadata);
                return Some((field_id, metadata));
            }
        }

        {
            let mut global = self.global.write().unwrap();

            if let Some((field_id, metadata)) = global.id_with_metadata(name) {
                self.local.insert(name, field_id, metadata);
                return Some((field_id, metadata));
            }

            let field_id = global.insert(name)?;
            let metadata = global.metadata(field_id).unwrap();
            self.local.insert(name, field_id, metadata);
            Some((field_id, metadata))
        }
    }

    /// Get the name of a field based on its id.
    pub fn name(&mut self, id: FieldId) -> Option<&str> {
        if self.local.name(id).is_none() {
            let global = self.global.read().unwrap();

            let (name, metadata) = global.name_with_metadata(id)?;
            self.local.insert(name, id, metadata);
        }

        self.local.name(id)
    }
}

impl<'indexing> MutFieldIdMapper for GlobalFieldsIdsMap<'indexing> {
    fn insert(&mut self, name: &str) -> Option<FieldId> {
        self.id_or_insert(name)
    }
}
