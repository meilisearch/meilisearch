mod search;
mod updates;

use std::sync::Arc;
use std::ops::Deref;

pub use search::{SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{Settings, Facets, UpdateResult};

#[derive(Clone)]
pub struct Index(pub Arc<milli::Index>);

impl Deref for Index {
    type Target = milli::Index;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Index {
    pub fn settings(&self) -> anyhow::Result<Settings> {
        let txn = self.read_txn()?;

        let displayed_attributes = self
            .displayed_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let searchable_attributes = self
            .searchable_fields(&txn)?
            .map(|fields| fields.into_iter().map(String::from).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);

        let faceted_attributes = self
            .faceted_fields(&txn)?
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        Ok(Settings {
            displayed_attributes: Some(Some(displayed_attributes)),
            searchable_attributes: Some(Some(searchable_attributes)),
            faceted_attributes: Some(Some(faceted_attributes)),
            criteria: None,
        })
    }
}
