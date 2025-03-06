//! The fieldids weights map is in charge of storing linking the searchable fields with their weights.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::constants::RESERVED_VECTORS_FIELD_NAME;
use crate::{FieldId, FieldsIdsMap, Weight};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FieldidsWeightsMap {
    map: HashMap<FieldId, Weight>,
}

impl FieldidsWeightsMap {
    /// Insert a field id -> weigth into the map.
    /// If the map did not have this key present, `None` is returned.
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&mut self, fid: FieldId, weight: Weight) -> Option<Weight> {
        self.map.insert(fid, weight)
    }

    /// Create the map from the fields ids maps.
    /// Should only be called in the case there are NO searchable attributes.
    /// All the fields will be inserted in the order of the fields ids map with a weight of 0.
    pub fn from_field_id_map_without_searchable(fid_map: &FieldsIdsMap) -> Self {
        FieldidsWeightsMap {
            map: fid_map
                .iter()
                .filter(|(_fid, name)| !crate::is_faceted_by(name, RESERVED_VECTORS_FIELD_NAME))
                .map(|(fid, _name)| (fid, 0))
                .collect(),
        }
    }

    /// Removes a field id from the map, returning the associated weight previously in the map.
    pub fn remove(&mut self, fid: FieldId) -> Option<Weight> {
        self.map.remove(&fid)
    }

    /// Returns weight corresponding to the key.
    pub fn weight(&self, fid: FieldId) -> Option<Weight> {
        self.map.get(&fid).copied()
    }

    /// Returns highest weight contained in the map if any.
    pub fn max_weight(&self) -> Option<Weight> {
        self.map.values().copied().max()
    }

    /// Return an iterator visiting all field ids in arbitrary order.
    pub fn ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        self.map.keys().copied()
    }
}
