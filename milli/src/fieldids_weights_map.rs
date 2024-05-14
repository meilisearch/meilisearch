use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{FieldId, Weight};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FieldidsWeightsMap {
    map: HashMap<FieldId, Weight>,
}

impl FieldidsWeightsMap {
    pub fn insert(&mut self, fid: FieldId, weight: Weight) -> Option<Weight> {
        self.map.insert(fid, weight)
    }

    pub fn remove(&mut self, fid: FieldId) -> Option<Weight> {
        self.map.remove(&fid)
    }

    pub fn weight(&self, fid: FieldId) -> Option<Weight> {
        self.map.get(&fid).copied()
    }

    pub fn max_weight(&self) -> Option<Weight> {
        self.map.values().copied().max()
    }

    pub fn ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        self.map.keys().copied()
    }
}
