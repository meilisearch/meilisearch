use std::collections::HashMap;

use roaring::RoaringBitmap;
use serde_json::Value;

use super::{Distinct, DocIter};
use crate::{DocumentId, FieldId, Index};

/// A distinct implementer that is backed by an `HashMap`. Each time a document is seen, the value
/// for its distinct field is added to the map. If the map already contains an entry for this
/// value, then the document is filtered out, and is added to the excluded set.
pub struct MapDistinct<'a> {
    distinct: FieldId,
    map: HashMap<String, usize>,
    index: &'a Index,
    txn: &'a heed::RoTxn<'a>,
}

impl<'a> MapDistinct<'a> {
    pub fn new(distinct: FieldId, index: &'a Index, txn: &'a heed::RoTxn<'a>) -> Self {
        let map = HashMap::new();
        Self {
            distinct,
            map,
            index,
            txn,
        }
    }
}

pub struct MapDistinctIter<'a, 'b> {
    distinct: FieldId,
    map: &'b mut HashMap<String, usize>,
    index: &'a Index,
    txn: &'a heed::RoTxn<'a>,
    candidates: roaring::bitmap::IntoIter,
    excluded: RoaringBitmap,
}

impl<'a, 'b> MapDistinctIter<'a, 'b> {
    fn next_inner(&mut self) -> anyhow::Result<Option<DocumentId>> {
        let map = &mut self.map;
        let mut filter = |value: Value| {
            let entry = map.entry(value.to_string()).or_insert(0);
            *entry += 1;
            *entry <= 1
        };

        while let Some(id) = self.candidates.next() {
            let document = self.index.documents(&self.txn, Some(id))?[0].1;
            let value = document
                .get(self.distinct)
                .map(serde_json::from_slice::<Value>)
                .transpose()?;

            let accept = match value {
                Some(value) => {
                    match value {
                        // Since we can't distinct these values, we always accept them
                        Value::Null | Value::Object(_) => true,
                        Value::Array(values) => {
                            let mut accept = true;
                            for value in values {
                                accept &= filter(value);
                            }
                            accept
                        }
                        value => filter(value),
                    }
                }
                // Accept values by default.
                _ => true,
            };

            if accept {
                return Ok(Some(id));
            } else {
                self.excluded.insert(id);
            }
        }
        Ok(None)
    }
}

impl Iterator for MapDistinctIter<'_, '_> {
    type Item = anyhow::Result<DocumentId>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}

impl DocIter for MapDistinctIter<'_, '_> {
    fn into_excluded(self) -> RoaringBitmap {
        self.excluded
    }
}

impl<'a, 'b> Distinct<'b> for MapDistinct<'a> {
    type Iter = MapDistinctIter<'a, 'b>;

    fn distinct(&'b mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter {
        MapDistinctIter {
            distinct: self.distinct,
            map: &mut self.map,
            index: &self.index,
            txn: &self.txn,
            candidates: candidates.into_iter(),
            excluded,
        }
    }
}
