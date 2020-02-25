use crate::{FieldsMap, FieldId, SResult, Error, IndexedPos};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    fields_map: FieldsMap,

    identifier: FieldId,
    ranked: HashSet<FieldId>,
    displayed: HashSet<FieldId>,

    indexed: Vec<FieldId>,
    indexed_map: HashMap<FieldId, IndexedPos>,

    accept_new_fields: bool,
}

impl Schema {
    pub fn with_identifier(name: &str) -> Schema {
        let mut fields_map = FieldsMap::default();
        let field_id = fields_map.insert(name).unwrap();

        Schema {
            fields_map,
            identifier: field_id,
            ranked: HashSet::new(),
            displayed: HashSet::new(),
            indexed: Vec::new(),
            indexed_map: HashMap::new(),
            accept_new_fields: true,
        }
    }

    pub fn identifier(&self) -> &str {
        self.fields_map.name(self.identifier).unwrap()
    }

    pub fn set_identifier(&mut self, id: &str) -> SResult<()> {
        match self.id(id) {
            Some(id) => {
                self.identifier = id;
                Ok(())
            },
            None => Err(Error::FieldNameNotFound(id.to_string()))
        }
    }

    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.fields_map.id(name)
    }

    pub fn name<I: Into<FieldId>>(&self, id: I) -> Option<&str> {
        self.fields_map.name(id)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.fields_map.id(name).is_some()
    }

    pub fn insert(&mut self, name: &str) -> SResult<FieldId> {
        self.fields_map.insert(name)
    }

    pub fn insert_and_index(&mut self, name: &str) -> SResult<FieldId> {
        match self.fields_map.id(name) {
            Some(id) => {
                Ok(id)
            }
            None => {
                if self.accept_new_fields {
                    self.set_indexed(name)?;
                    self.set_displayed(name)
                } else {
                    self.fields_map.insert(name)
                }
            }
        }
    }

    pub fn ranked(&self) -> &HashSet<FieldId> {
        &self.ranked
    }

    pub fn ranked_name(&self) -> HashSet<&str> {
        self.ranked.iter().filter_map(|a| self.name(*a)).collect()
    }

    pub fn displayed(&self) -> &HashSet<FieldId> {
        &self.displayed
    }

    pub fn displayed_name(&self) -> HashSet<&str> {
        self.displayed.iter().filter_map(|a| self.name(*a)).collect()
    }

    pub fn indexed(&self) -> &Vec<FieldId> {
        &self.indexed
    }

    pub fn indexed_name(&self) -> Vec<&str> {
        self.indexed.iter().filter_map(|a| self.name(*a)).collect()
    }

    pub fn set_ranked(&mut self, name: &str) -> SResult<FieldId> {
        let id = self.fields_map.insert(name)?;
        self.ranked.insert(id);
        Ok(id)
    }

    pub fn set_displayed(&mut self, name: &str) -> SResult<FieldId> {
        let id = self.fields_map.insert(name)?;
        self.displayed.insert(id);
        Ok(id)
    }

    pub fn set_indexed(&mut self, name: &str) -> SResult<(FieldId, IndexedPos)> {
        let id = self.fields_map.insert(name)?;
        if let Some(indexed_pos) = self.indexed_map.get(&id) {
            return Ok((id, *indexed_pos))
        };
        let pos = self.indexed.len() as u16;
        self.indexed.push(id);
        self.indexed_map.insert(id, pos.into());
        Ok((id, pos.into()))
    }

    pub fn remove_ranked(&mut self, name: &str) {
        if let Some(id) = self.fields_map.id(name) {
            self.ranked.remove(&id);
        }
    }

    pub fn remove_displayed(&mut self, name: &str) {
        if let Some(id) = self.fields_map.id(name) {
            self.displayed.remove(&id);
        }
    }

    pub fn remove_indexed(&mut self, name: &str) {
        if let Some(id) = self.fields_map.id(name) {
            self.indexed_map.remove(&id);
            self.indexed.retain(|x| *x != id);
        }
    }

    pub fn is_ranked(&self, id: FieldId) -> bool {
        self.ranked.get(&id).is_some()
    }

    pub fn is_displayed(&self, id: FieldId) -> bool {
        self.displayed.get(&id).is_some()
    }

    pub fn is_indexed(&self, id: FieldId) -> Option<&IndexedPos> {
        self.indexed_map.get(&id)
    }

    pub fn indexed_pos_to_field_id<I: Into<IndexedPos>>(&self, pos: I) -> Option<FieldId> {
        let indexed_pos = pos.into().0 as usize;
        if indexed_pos < self.indexed.len() {
            Some(self.indexed[indexed_pos as usize])
        } else {
            None
        }
    }

    pub fn update_ranked<S: AsRef<str>>(&mut self, data: impl IntoIterator<Item = S>) -> SResult<()> {
        self.ranked.clear();
        for name in data {
            self.set_ranked(name.as_ref())?;
        }
        Ok(())
    }

    pub fn update_displayed<S: AsRef<str>>(&mut self, data: impl IntoIterator<Item = S>) -> SResult<()> {
        self.displayed.clear();
        for name in data {
            self.set_displayed(name.as_ref())?;
        }
        Ok(())
    }

    pub fn update_indexed<S: AsRef<str>>(&mut self, data: Vec<S>) -> SResult<()> {
        self.indexed.clear();
        self.indexed_map.clear();
        for name in data {
            self.set_indexed(name.as_ref())?;
        }
        Ok(())
    }

    pub fn accept_new_fields(&self) -> bool {
        self.accept_new_fields
    }

    pub fn set_accept_new_fields(&mut self, value: bool) {
        self.accept_new_fields = value;
    }
}
