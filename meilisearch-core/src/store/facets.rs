use std::borrow::Cow;
use std::collections::HashMap;

use heed::{RwTxn, RoTxn, Result as ZResult, RoRange};
use sdset::{SetBuf, Set, SetOperation};

use meilisearch_types::DocumentId;
use meilisearch_schema::FieldId;

use crate::database::MainT;
use crate::facets::FacetKey;
use super::cow_set::CowSet;

/// contains facet info
#[derive(Clone, Copy)]
pub struct Facets {
    pub(crate) facets: heed::Database<FacetKey, CowSet<DocumentId>>,
}

impl Facets {
    // we use sdset::SetBuf to ensure the docids are sorted.
    pub fn put_facet_document_ids(&self, writer: &mut RwTxn<MainT>, facet_key: FacetKey, doc_ids: &Set<DocumentId>) -> ZResult<()> {
        self.facets.put(writer, &facet_key, doc_ids)
    }

    pub fn field_document_ids<'txn>(&self, reader: &'txn RoTxn<MainT>, field_id: FieldId) -> ZResult<RoRange<'txn, FacetKey, CowSet<DocumentId>>> {
        self.facets.prefix_iter(reader, &FacetKey::new(field_id, String::new()))
    }

    pub fn facet_document_ids<'txn>(&self, reader: &'txn RoTxn<MainT>, facet_key: &FacetKey) -> ZResult<Option<Cow<'txn, Set<DocumentId>>>> {
        self.facets.get(reader, &facet_key)
    }

    /// updates the facets  store, revmoving the documents from the facets provided in the
    /// `facet_map` argument
    pub fn remove(&self, writer: &mut RwTxn<MainT>, facet_map: HashMap<FacetKey, Vec<DocumentId>>) -> ZResult<()> {
        for (key, document_ids) in facet_map {
            if let Some(old) = self.facets.get(writer, &key)? {
                let to_remove = SetBuf::from_dirty(document_ids);
                let new = sdset::duo::OpBuilder::new(old.as_ref(), to_remove.as_set()).difference().into_set_buf();
                self.facets.put(writer, &key, new.as_set())?;
            }
        }
        Ok(())
    }

    pub fn add(&self, writer: &mut RwTxn<MainT>, facet_map: HashMap<FacetKey, Vec<DocumentId>>) -> ZResult<()> {
        for (key, document_ids) in facet_map {
            let set = SetBuf::from_dirty(document_ids);
            self.put_facet_document_ids(writer, key, set.as_set())?;
        }
        Ok(())
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> ZResult<()> {
        self.facets.clear(writer)
    }
}
