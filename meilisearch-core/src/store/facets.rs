use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;

use heed::{RwTxn, RoTxn, RoPrefix, types::Str, BytesEncode, BytesDecode};
use sdset::{SetBuf, Set, SetOperation};

use meilisearch_types::DocumentId;
use meilisearch_schema::FieldId;

use crate::MResult;
use crate::database::MainT;
use crate::facets::FacetKey;
use super::cow_set::CowSet;

/// contains facet info
#[derive(Clone, Copy)]
pub struct Facets {
    pub(crate) facets: heed::Database<FacetKey, FacetData>,
}

pub struct FacetData;

impl<'a> BytesEncode<'a> for FacetData {
    type EItem = (&'a str, &'a Set<DocumentId>);

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        // get size of the first item
        let first_size =  item.0.as_bytes().len();
        let size = mem::size_of::<u64>()
            + first_size
            + item.1.len() * mem::size_of::<DocumentId>();
        let mut buffer = Vec::with_capacity(size);
        // encode the length of the first item
        buffer.extend_from_slice(&first_size.to_be_bytes());
        buffer.extend_from_slice(Str::bytes_encode(&item.0)?.as_ref());
        let second_slice = CowSet::bytes_encode(&item.1)?;
        buffer.extend_from_slice(second_slice.as_ref());
        Some(Cow::Owned(buffer))
    }
}

impl<'a> BytesDecode<'a> for FacetData {
    type DItem = (&'a str, Cow<'a, Set<DocumentId>>);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        const LEN: usize = mem::size_of::<u64>();
        let mut size_buf = [0; LEN];
        size_buf.copy_from_slice(bytes.get(0..LEN)?);
        // decode size of the first item from the bytes
        let first_size = usize::from_be_bytes(size_buf);
        // decode first and second items
        let first_item = Str::bytes_decode(bytes.get(LEN..(LEN + first_size))?)?;
        let second_item = CowSet::bytes_decode(bytes.get((LEN + first_size)..)?)?;
        Some((first_item, second_item))
    }
}

impl Facets {
    // we use sdset::SetBuf to ensure the docids are sorted.
    pub fn put_facet_document_ids(&self, writer: &mut RwTxn<MainT>, facet_key: FacetKey, doc_ids: &Set<DocumentId>, facet_value: &str) -> MResult<()> {
        Ok(self.facets.put(writer, &facet_key, &(facet_value, doc_ids))?)
    }

    pub fn field_document_ids<'txn>(&self, reader: &'txn RoTxn<MainT>, field_id: FieldId) -> MResult<RoPrefix<'txn, FacetKey, FacetData>> {
        Ok(self.facets.prefix_iter(reader, &FacetKey::new(field_id, String::new()))?)
    }

    pub fn facet_document_ids<'txn>(&self, reader: &'txn RoTxn<MainT>, facet_key: &FacetKey) -> MResult<Option<(&'txn str,Cow<'txn, Set<DocumentId>>)>> {
        Ok(self.facets.get(reader, &facet_key)?)
    }

    /// updates the facets  store, revmoving the documents from the facets provided in the
    /// `facet_map` argument
    pub fn remove(&self, writer: &mut RwTxn<MainT>, facet_map: HashMap<FacetKey, (String, Vec<DocumentId>)>) -> MResult<()> {
        for (key, (name, document_ids)) in facet_map {
            if let Some((_, old)) = self.facets.get(writer, &key)? {
                let to_remove = SetBuf::from_dirty(document_ids);
                let new = sdset::duo::OpBuilder::new(old.as_ref(), to_remove.as_set()).difference().into_set_buf();
                self.facets.put(writer, &key, &(&name, new.as_set()))?;
            }
        }
        Ok(())
    }

    pub fn add(&self, writer: &mut RwTxn<MainT>, facet_map: HashMap<FacetKey, (String, Vec<DocumentId>)>) -> MResult<()> {
        for (key, (facet_name, document_ids)) in facet_map {
            let set = SetBuf::from_dirty(document_ids);
            self.put_facet_document_ids(writer, key, set.as_set(), &facet_name)?;
        }
        Ok(())
    }

    pub fn clear(self, writer: &mut heed::RwTxn<MainT>) -> MResult<()> {
        Ok(self.facets.clear(writer)?)
    }
}
