use std::sync::Arc;
use std::collections::HashMap;

use anyhow::Result;
use milli::{Index, FieldsIdsMap, SearchResult, FieldId, facet::FacetType};
use ouroboros::self_referencing;

use crate::data::SearchQuery;

#[self_referencing]
pub struct IndexView {
    pub index: Arc<Index>,
    #[borrows(index)]
    #[covariant]
    pub txn: heed::RoTxn<'this>,
    uuid: String,
}

impl IndexView {
    pub fn search(&self, search_query: &SearchQuery) -> Result<SearchResult> {
        self.with(|this| {
            let mut search = this.index.search(&this.txn);
            if let Some(query) = &search_query.q {
                search.query(query);
            }

            if let Some(offset) = search_query.offset {
                search.offset(offset);
            }

            let limit = search_query.limit;
            search.limit(limit);

            Ok(search.execute()?)
        })
    }

    #[inline]
    pub fn fields_ids_map(&self) -> Result<FieldsIdsMap> {
        self.with(|this| Ok(this.index.fields_ids_map(&this.txn)?))

    }

    #[inline]
    pub fn displayed_fields_ids(&self) -> Result<Option<Vec<FieldId>>> {
        self.with(|this| Ok(this.index.displayed_fields_ids(&this.txn)?))
    }

    #[inline]
    pub fn displayed_fields(&self) -> Result<Option<Vec<String>>> {
        self.with(|this| Ok(this.index
                .displayed_fields(&this.txn)?
                .map(|fields| fields.into_iter().map(String::from).collect())))
    }

    #[inline]
    pub fn searchable_fields(&self) -> Result<Option<Vec<String>>> {
        self.with(|this| Ok(this.index
                .searchable_fields(&this.txn)?
                .map(|fields| fields.into_iter().map(String::from).collect())))
    }

    #[inline]
    pub fn faceted_fields(&self) -> Result<HashMap<std::string::String, FacetType>> {
        self.with(|this| Ok(this.index.faceted_fields(&this.txn)?))
    }

    pub fn documents(&self, ids: &[u32]) -> Result<Vec<(u32, obkv::KvReader<'_>)>> {
        let txn = self.borrow_txn();
        let index = self.borrow_index();
        Ok(index.documents(txn, ids.into_iter().copied())?)
    }
}

