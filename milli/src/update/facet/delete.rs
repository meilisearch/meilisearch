use std::collections::{HashMap, HashSet};

use heed::RwTxn;
use log::debug;
use roaring::RoaringBitmap;
use time::OffsetDateTime;

use super::{FACET_GROUP_SIZE, FACET_MAX_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::heed_codec::ByteSliceRefCodec;
use crate::update::{FacetsUpdateBulk, FacetsUpdateIncrementalInner};
use crate::{FieldId, Index, Result};

/// A builder used to remove elements from the `facet_id_string_docids` or `facet_id_f64_docids` databases.
///
/// Depending on the number of removed elements and the existing size of the database, we use either
/// a bulk delete method or an incremental delete method.
pub struct FacetsDelete<'i, 'b> {
    index: &'i Index,
    database: heed::Database<FacetGroupKeyCodec<ByteSliceRefCodec>, FacetGroupValueCodec>,
    facet_type: FacetType,
    affected_facet_values: HashMap<FieldId, HashSet<Vec<u8>>>,
    docids_to_delete: &'b RoaringBitmap,
    group_size: u8,
    max_group_size: u8,
    min_level_size: u8,
}
impl<'i, 'b> FacetsDelete<'i, 'b> {
    pub fn new(
        index: &'i Index,
        facet_type: FacetType,
        affected_facet_values: HashMap<FieldId, HashSet<Vec<u8>>>,
        docids_to_delete: &'b RoaringBitmap,
    ) -> Self {
        let database = match facet_type {
            FacetType::String => index
                .facet_id_string_docids
                .remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>(),
            FacetType::Number => {
                index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>()
            }
        };
        Self {
            index,
            database,
            facet_type,
            affected_facet_values,
            docids_to_delete,
            group_size: FACET_GROUP_SIZE,
            max_group_size: FACET_MAX_GROUP_SIZE,
            min_level_size: FACET_MIN_LEVEL_SIZE,
        }
    }

    pub fn execute(self, wtxn: &mut RwTxn) -> Result<()> {
        debug!("Computing and writing the facet values levels docids into LMDB on disk...");
        self.index.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;

        for (field_id, affected_facet_values) in self.affected_facet_values {
            // This is an incorrect condition, since we assume that the length of the database is equal
            // to the number of facet values for the given field_id. It means that in some cases, we might
            // wrongly choose the incremental indexer over the bulk indexer. But the only case where that could
            // really be a performance problem is when we fully delete a large ratio of all facet values for
            // each field id. This would almost never happen. Still, to be overly cautious, I have added a
            // 2x penalty to the incremental indexer. That is, instead of assuming a 70x worst-case performance
            // penalty to the incremental indexer, we assume a 150x worst-case performance penalty instead.
            if affected_facet_values.len() >= (self.database.len(wtxn)? / 150) {
                // Bulk delete
                let mut modified = false;

                for facet_value in affected_facet_values {
                    let key =
                        FacetGroupKey { field_id, level: 0, left_bound: facet_value.as_slice() };
                    let mut old = self.database.get(wtxn, &key)?.unwrap();
                    let previous_len = old.bitmap.len();
                    old.bitmap -= self.docids_to_delete;
                    if old.bitmap.is_empty() {
                        modified = true;
                        self.database.delete(wtxn, &key)?;
                    } else if old.bitmap.len() != previous_len {
                        modified = true;
                        self.database.put(wtxn, &key, &old)?;
                    }
                }
                if modified {
                    let builder = FacetsUpdateBulk::new_not_updating_level_0(
                        self.index,
                        vec![field_id],
                        self.facet_type,
                    );
                    builder.execute(wtxn)?;
                }
            } else {
                // Incremental
                let inc = FacetsUpdateIncrementalInner {
                    db: self.database,
                    group_size: self.group_size,
                    min_level_size: self.min_level_size,
                    max_group_size: self.max_group_size,
                };
                for facet_value in affected_facet_values {
                    inc.delete(wtxn, field_id, facet_value.as_slice(), self.docids_to_delete)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use big_s::S;
    use maplit::hashset;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use roaring::RoaringBitmap;

    use crate::db_snap;
    use crate::documents::documents_batch_reader_from_objects;
    use crate::index::tests::TempIndex;
    use crate::update::facet::test_helpers::ordered_string;
    use crate::update::DeleteDocuments;

    #[test]
    fn delete_mixed_incremental_and_bulk() {
        // The point of this test is to create an index populated with documents
        // containing different filterable attributes. Then, we delete a bunch of documents
        // such that a mix of the incremental and bulk indexer is used (depending on the field id)
        let index = TempIndex::new_with_map_size(4096 * 1000 * 100);

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(
                    hashset! { S("id"), S("label"), S("timestamp"), S("colour") },
                );
            })
            .unwrap();

        let mut documents = vec![];
        for i in 0..1000 {
            documents.push(
                serde_json::json! {
                    {
                        "id": i,
                        "label": i / 10,
                        "colour": i / 100,
                        "timestamp": i / 2,
                    }
                }
                .as_object()
                .unwrap()
                .clone(),
            );
        }

        let documents = documents_batch_reader_from_objects(documents);
        index.add_documents(documents).unwrap();

        db_snap!(index, facet_id_f64_docids, 1, @"550cd138d6fe31ccdd42cd5392fbd576");
        db_snap!(index, number_faceted_documents_ids, 1, @"9a0ea88e7c9dcf6dc0ef0b601736ffcf");

        let mut wtxn = index.env.write_txn().unwrap();

        let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        builder.disable_soft_deletion(true);
        builder.delete_documents(&RoaringBitmap::from_iter(0..100));
        // by deleting the first 100 documents, we expect that:
        // - the "id" part of the DB will be updated in bulk, since #affected_facet_value = 100 which is > database_len / 150 (= 13)
        // - the "label" part will be updated incrementally, since #affected_facet_value = 10 which is < 13
        // - the "colour" part will also be updated incrementally, since #affected_values = 1 which is < 13
        // - the "timestamp" part will be updated in bulk, since #affected_values = 50 which is > 13
        // This has to be verified manually by inserting breakpoint/adding print statements to the code when running the test
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, soft_deleted_documents_ids, @"[]");
        db_snap!(index, facet_id_f64_docids, 2, @"d4d5f14e7f1e1f09b86821a0b6defcc6");
        db_snap!(index, number_faceted_documents_ids, 2, @"3570e0ac0fdb21be9ebe433f59264b56");
    }

    // Same test as above but working with string values for the facets
    #[test]
    fn delete_mixed_incremental_and_bulk_string() {
        // The point of this test is to create an index populated with documents
        // containing different filterable attributes. Then, we delete a bunch of documents
        // such that a mix of the incremental and bulk indexer is used (depending on the field id)
        let index = TempIndex::new_with_map_size(4096 * 1000 * 100);

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(
                    hashset! { S("id"), S("label"), S("timestamp"), S("colour") },
                );
            })
            .unwrap();

        let mut documents = vec![];
        for i in 0..1000 {
            documents.push(
                serde_json::json! {
                    {
                        "id": i,
                        "label": ordered_string(i / 10),
                        "colour": ordered_string(i / 100),
                        "timestamp": ordered_string(i / 2),
                    }
                }
                .as_object()
                .unwrap()
                .clone(),
            );
        }

        let documents = documents_batch_reader_from_objects(documents);
        index.add_documents(documents).unwrap();

        // Note that empty strings are not stored in the facet db due to commit 4860fd452965 (comment written on 29 Nov 2022)
        db_snap!(index, facet_id_string_docids, 1, @"5fd1bd0724c65a6dc1aafb6db93c7503");
        db_snap!(index, string_faceted_documents_ids, 1, @"54bc15494fa81d93339f43c08fd9d8f5");

        let mut wtxn = index.env.write_txn().unwrap();

        let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        builder.disable_soft_deletion(true);
        builder.delete_documents(&RoaringBitmap::from_iter(0..100));
        // by deleting the first 100 documents, we expect that:
        // - the "id" part of the DB will be updated in bulk, since #affected_facet_value = 100 which is > database_len / 150 (= 13)
        // - the "label" part will be updated incrementally, since #affected_facet_value = 10 which is < 13
        // - the "colour" part will also be updated incrementally, since #affected_values = 1 which is < 13
        // - the "timestamp" part will be updated in bulk, since #affected_values = 50 which is > 13
        // This has to be verified manually by inserting breakpoint/adding print statements to the code when running the test
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, soft_deleted_documents_ids, @"[]");
        db_snap!(index, facet_id_string_docids, 2, @"7f9c00b29e04d58c1821202a5dda0ebc");
        db_snap!(index, string_faceted_documents_ids, 2, @"504152afa5c94fd4e515dcdfa4c7161f");
    }

    #[test]
    fn delete_almost_all_incrementally_string() {
        let index = TempIndex::new_with_map_size(4096 * 1000 * 100);

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(
                    hashset! { S("id"), S("label"), S("timestamp"), S("colour") },
                );
            })
            .unwrap();

        let mut documents = vec![];
        for i in 0..1000 {
            documents.push(
                serde_json::json! {
                    {
                        "id": i,
                        "label": ordered_string(i / 10),
                        "colour": ordered_string(i / 100),
                        "timestamp": ordered_string(i / 2),
                    }
                }
                .as_object()
                .unwrap()
                .clone(),
            );
        }

        let documents = documents_batch_reader_from_objects(documents);
        index.add_documents(documents).unwrap();

        // Note that empty strings are not stored in the facet db due to commit 4860fd452965 (comment written on 29 Nov 2022)
        db_snap!(index, facet_id_string_docids, 1, @"5fd1bd0724c65a6dc1aafb6db93c7503");
        db_snap!(index, string_faceted_documents_ids, 1, @"54bc15494fa81d93339f43c08fd9d8f5");

        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);

        let mut docids_to_delete = (0..1000).collect::<Vec<_>>();
        docids_to_delete.shuffle(&mut rng);
        for docid in docids_to_delete.into_iter().take(990) {
            let mut wtxn = index.env.write_txn().unwrap();
            let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
            builder.disable_soft_deletion(true);
            builder.delete_documents(&RoaringBitmap::from_iter([docid]));
            builder.execute().unwrap();
            wtxn.commit().unwrap();
        }

        db_snap!(index, soft_deleted_documents_ids, @"[]");
        db_snap!(index, facet_id_string_docids, 2, @"ece56086e76d50e661fb2b58475b9f7d");
        db_snap!(index, string_faceted_documents_ids, 2, @r###"
        0   []
        1   [11, 20, 73, 292, 324, 358, 381, 493, 839, 852, ]
        2   [292, 324, 358, 381, 493, 839, 852, ]
        3   [11, 20, 73, 292, 324, 358, 381, 493, 839, 852, ]
        "###);
    }
}

#[allow(unused)]
#[cfg(test)]
mod comparison_bench {
    use std::iter::once;

    use rand::Rng;
    use roaring::RoaringBitmap;

    use crate::heed_codec::facet::OrderedF64Codec;
    use crate::update::facet::test_helpers::FacetIndex;

    // This is a simple test to get an intuition on the relative speed
    // of the incremental vs. bulk indexer.
    //
    // The benchmark shows the worst-case scenario for the incremental indexer, since
    // each facet value contains only one document ID.
    //
    // In that scenario, it appears that the incremental indexer is about 70 times slower than the
    // bulk indexer.
    // #[test]
    fn benchmark_facet_indexing_delete() {
        let mut r = rand::thread_rng();

        for i in 1..=20 {
            let size = 50_000 * i;
            let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);

            let mut txn = index.env.write_txn().unwrap();
            let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
            for i in 0..size {
                // field id = 0, left_bound = i, docids = [i]
                elements.push(((0, i as f64), once(i).collect()));
            }
            let timer = std::time::Instant::now();
            index.bulk_insert(&mut txn, &[0], elements.iter());
            let time_spent = timer.elapsed().as_millis();
            println!("bulk {size} : {time_spent}ms");

            txn.commit().unwrap();

            for nbr_doc in [1, 100, 1000, 10_000] {
                let mut txn = index.env.write_txn().unwrap();
                let timer = std::time::Instant::now();
                //
                // delete one document
                //
                for _ in 0..nbr_doc {
                    let deleted_u32 = r.gen::<u32>() % size;
                    let deleted_f64 = deleted_u32 as f64;
                    index.delete_single_docid(&mut txn, 0, &deleted_f64, deleted_u32)
                }
                let time_spent = timer.elapsed().as_millis();
                println!("    delete {nbr_doc} : {time_spent}ms");
                txn.abort().unwrap();
            }
        }
    }
}
