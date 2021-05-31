use std::collections::HashMap;

use roaring::RoaringBitmap;
use serde_json::Value;

use super::{Distinct, DocIter};
use crate::{DocumentId, FieldId, Index};

/// A distinct implementer that is backed by an `HashMap`.
///
/// Each time a document is seen, the value
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
        Self {
            distinct,
            map: HashMap::new(),
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
    /// Performs the next iteration of the mafacetp distinct. This is a convenience method that is
    /// called by the Iterator::next implementation that transposes the result. It makes error
    /// handling easier.
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
                Some(Value::Array(values)) => {
                    let mut accept = true;
                    for value in values {
                        accept &= filter(value);
                    }
                    accept
                }
                Some(Value::Null) | Some(Value::Object(_)) | None => true,
                Some(value) => filter(value),
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

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;
    use super::super::test::{generate_index, validate_distinct_candidates};

    macro_rules! test_map_distinct {
        ($name:ident, $distinct:literal) => {
            #[test]
            fn $name() {
                let (index, fid, candidates) = generate_index($distinct, HashSet::new());
                let txn = index.read_txn().unwrap();
                let mut map_distinct = MapDistinct::new(fid, &index, &txn);
                let excluded = RoaringBitmap::new();
                let mut iter = map_distinct.distinct(candidates.clone(), excluded);
                let count = validate_distinct_candidates(iter.by_ref(), fid, &index);
                let excluded = iter.into_excluded();
                assert_eq!(count as u64 + excluded.len(), candidates.len());
            }
        };
    }

    test_map_distinct!(test_string, "txt");
    test_map_distinct!(test_strings, "txts");
    test_map_distinct!(test_int, "cat-int");
    test_map_distinct!(test_ints, "cat-ints");
}
