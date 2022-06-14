mod facet_distinct;
mod noop_distinct;

pub use facet_distinct::FacetDistinct;
pub use noop_distinct::NoopDistinct;
use roaring::RoaringBitmap;

use crate::{DocumentId, Result};

/// A trait implemented by document interators that are returned by calls to `Distinct::distinct`.
/// It provides a way to get back the ownership to the excluded set.
pub trait DocIter: Iterator<Item = Result<DocumentId>> {
    /// Returns ownership on the internal exluded set.
    fn into_excluded(self) -> RoaringBitmap;
}

/// A trait that is implemented by structs that perform a distinct on `candidates`. Calling distinct
/// must return an iterator containing only distinct documents, and add the discarded documents to
/// the excluded set. The excluded set can later be retrieved by calling `DocIter::excluded` on the
/// returned iterator.
pub trait Distinct {
    type Iter: DocIter;

    fn distinct(&mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter;
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::io::Cursor;

    use once_cell::sync::Lazy;
    use rand::seq::SliceRandom;
    use rand::Rng;
    use roaring::RoaringBitmap;
    use serde_json::{json, Value};

    use crate::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
    use crate::index::tests::TempIndex;
    use crate::index::Index;
    use crate::update::{
        IndexDocuments, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig, Settings,
    };
    use crate::{DocumentId, FieldId, BEU32};

    static JSON: Lazy<Vec<u8>> = Lazy::new(|| {
        let mut rng = rand::thread_rng();
        let num_docs = rng.gen_range(10..30);

        let mut builder = DocumentsBatchBuilder::new(Vec::new());
        let txts = ["Toto", "Titi", "Tata"];
        let cats = (1..10).map(|i| i.to_string()).collect::<Vec<_>>();
        let cat_ints = (1..10).collect::<Vec<_>>();

        for i in 0..num_docs {
            let txt = txts.choose(&mut rng).unwrap();
            let mut sample_txts = cats.clone();
            sample_txts.shuffle(&mut rng);

            let mut sample_ints = cat_ints.clone();
            sample_ints.shuffle(&mut rng);

            let json = json!({
                "id": i,
                "txt": txt,
                "cat-int": rng.gen_range(0..3),
                "txts": sample_txts[..(rng.gen_range(0..3))],
                "cat-ints": sample_ints[..(rng.gen_range(0..3))],
            });

            let object = match json {
                Value::Object(object) => object,
                _ => panic!(),
            };

            builder.append_json_object(&object).unwrap();
        }

        builder.into_inner().unwrap()
    });

    /// Returns a temporary index populated with random test documents, the FieldId for the
    /// distinct attribute, and the RoaringBitmap with the document ids.
    pub(crate) fn generate_index(distinct: &str) -> (TempIndex, FieldId, RoaringBitmap) {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();

        // set distinct and faceted attributes for the index.
        let config = IndexerConfig::default();
        let mut update = Settings::new(&mut txn, &index, &config);
        update.set_distinct_field(distinct.to_string());
        update.execute(|_| ()).unwrap();

        // add documents to the index
        let config = IndexerConfig::default();
        let indexing_config = IndexDocumentsConfig {
            update_method: IndexDocumentsMethod::ReplaceDocuments,
            ..Default::default()
        };
        let mut addition =
            IndexDocuments::new(&mut txn, &index, &config, indexing_config, |_| ()).unwrap();

        let reader =
            crate::documents::DocumentsBatchReader::from_reader(Cursor::new(JSON.as_slice()))
                .unwrap();

        addition.add_documents(reader).unwrap();
        addition.execute().unwrap();

        let fields_map = index.fields_ids_map(&txn).unwrap();
        let fid = fields_map.id(&distinct).unwrap();

        let documents = DocumentsBatchReader::from_reader(Cursor::new(JSON.as_slice())).unwrap();
        let map = (0..documents.documents_count() as u32).collect();

        txn.commit().unwrap();

        (index, fid, map)
    }

    /// Checks that all the candidates are distinct, and returns the candidates number.
    pub(crate) fn validate_distinct_candidates(
        candidates: impl Iterator<Item = crate::Result<DocumentId>>,
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
                Value::Array(values) => values.into_iter().for_each(|value| test(seen, value)),
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
