mod facet_distinct;
mod map_distinct;
mod noop_distinct;

use roaring::RoaringBitmap;

use crate::DocumentId;
pub use facet_distinct::FacetDistinct;
pub use map_distinct::MapDistinct;
pub use noop_distinct::NoopDistinct;

/// A trait implemented by document interators that are returned by calls to `Distinct::distinct`.
/// It provides a way to get back the ownership to the excluded set.
pub trait DocIter: Iterator<Item = anyhow::Result<DocumentId>> {
    /// Returns ownership on the internal exluded set.
    fn into_excluded(self) -> RoaringBitmap;
}

/// A trait that is implemented by structs that perform a distinct on `candidates`. Calling distinct
/// must return an iterator containing only distinct documents, and add the discarded documents to
/// the excluded set. The excluded set can later be retrieved by calling `DocIter::excluded` on the
/// returned iterator.
pub trait Distinct<'a> {
    type Iter: DocIter;

    fn distinct(&'a mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter;
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use once_cell::sync::Lazy;
    use rand::{seq::SliceRandom, Rng};
    use roaring::RoaringBitmap;
    use serde_json::{json, Value};

    use crate::index::{Index, tests::TempIndex};
    use crate::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
    use crate::{BEU32, FieldId, DocumentId};

    static JSON: Lazy<Value> = Lazy::new(generate_json);

    fn generate_json() -> Value {
        let mut rng = rand::thread_rng();
        let num_docs = rng.gen_range(10..30);

        let mut documents = Vec::new();

        let txts = ["toto", "titi", "tata"];
        let cats = (1..10).map(|i| i.to_string()).collect::<Vec<_>>();
        let cat_ints = (1..10).collect::<Vec<_>>();

        for i in 0..num_docs {
            let txt = txts.choose(&mut rng).unwrap();
            let mut sample_txts = cats.clone();
            sample_txts.shuffle(&mut rng);

            let mut sample_ints = cat_ints.clone();
            sample_ints.shuffle(&mut rng);

            let doc = json!({
                "id": i,
                "txt": txt,
                "cat-int": rng.gen_range(0..3),
                "txts": sample_txts[..(rng.gen_range(0..3))],
                "cat-ints": sample_ints[..(rng.gen_range(0..3))],
            });
            documents.push(doc);
        }

        Value::Array(documents)
    }

    /// Returns a temporary index populated with random test documents, the FieldId for the
    /// distinct attribute, and the RoaringBitmap with the document ids.
    pub(crate) fn generate_index(distinct: &str, facets: HashSet<String>) -> (TempIndex, FieldId, RoaringBitmap) {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();

        // set distinct and faceted attributes for the index.
        let builder = UpdateBuilder::new(0);
        let mut update = builder.settings(&mut txn, &index);
        update.set_distinct_attribute(distinct.to_string());
        if !facets.is_empty() {
            update.set_filterable_fields(facets)
        }
        update.execute(|_, _| ()).unwrap();

        // add documents to the index
        let builder = UpdateBuilder::new(1);
        let mut addition = builder.index_documents(&mut txn, &index);

        addition.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);
        addition.update_format(UpdateFormat::Json);

        addition
            .execute(JSON.to_string().as_bytes(), |_, _| ())
            .unwrap();

        let fields_map = index.fields_ids_map(&txn).unwrap();
        let fid = fields_map.id(&distinct).unwrap();

        let map = (0..JSON.as_array().unwrap().len() as u32).collect();

        txn.commit().unwrap();

        (index, fid, map)
    }


    /// Checks that all the candidates are distinct, and returns the candidates number.
    pub(crate) fn validate_distinct_candidates(
        candidates: impl Iterator<Item=anyhow::Result<DocumentId>>,
        distinct: FieldId,
        index: &Index,
        ) -> usize {
        fn test(seen: &mut HashSet<String>, value: &Value) {
            match value {
                Value::Null | Value::Object(_) | Value::Bool(_) => (),
                Value::Number(_) | Value::String(_) => {
                    let s = value.to_string();
                    assert!(seen.insert(s));
                }
                Value::Array(values) => {values.into_iter().for_each(|value| test(seen, value))}
            }
        }

        let mut seen = HashSet::<String>::new();

        let txn = index.read_txn().unwrap();
        let mut count = 0;
        for candidate in candidates {
            count += 1;
            let candidate = candidate.unwrap();
            let id = BEU32::new(candidate);
            let document = index.documents.get(&txn, &id).unwrap().unwrap();
            let value = document.get(distinct).unwrap();
            let value = serde_json::from_slice(value).unwrap();
            test(&mut seen, &value);
        }
        count
    }
}
