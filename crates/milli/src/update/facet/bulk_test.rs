use std::iter::once;

use roaring::RoaringBitmap;

use crate::documents::mmap_from_objects;
use crate::heed_codec::facet::OrderedF64Codec;
use crate::heed_codec::StrRefCodec;
use crate::index::tests::TempIndex;
use crate::update::facet::test_helpers::{ordered_string, FacetIndex};
use crate::{db_snap, milli_snap, FilterableAttributesRule};

#[test]
fn insert() {
    let test = |name: &str, group_size: u8, min_level_size: u8| {
        let index =
            FacetIndex::<OrderedF64Codec>::new(group_size, 0 /*NA*/, min_level_size);

        let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
        for i in 0..1_000u32 {
            // field id = 0, left_bound = i, docids = [i]
            elements.push(((0, i as f64), once(i).collect()));
        }
        for i in 0..100u32 {
            // field id = 1, left_bound = i, docids = [i]
            elements.push(((1, i as f64), once(i).collect()));
        }
        let mut wtxn = index.env.write_txn().unwrap();
        index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

        index.verify_structure_validity(&wtxn, 0);
        index.verify_structure_validity(&wtxn, 1);

        wtxn.commit().unwrap();

        milli_snap!(format!("{index}"), name);
    };

    test("default", 4, 5);
    test("small_group_small_min_level", 2, 2);
    test("small_group_large_min_level", 2, 128);
    test("large_group_small_min_level", 16, 2);
    test("odd_group_odd_min_level", 7, 3);
}
#[test]
fn insert_delete_field_insert() {
    let test = |name: &str, group_size: u8, min_level_size: u8| {
        let index =
            FacetIndex::<OrderedF64Codec>::new(group_size, 0 /*NA*/, min_level_size);
        let mut wtxn = index.env.write_txn().unwrap();

        let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
        for i in 0..100u32 {
            // field id = 0, left_bound = i, docids = [i]
            elements.push(((0, i as f64), once(i).collect()));
        }
        for i in 0..100u32 {
            // field id = 1, left_bound = i, docids = [i]
            elements.push(((1, i as f64), once(i).collect()));
        }
        index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

        index.verify_structure_validity(&wtxn, 0);
        index.verify_structure_validity(&wtxn, 1);
        // delete all the elements for the facet id 0
        for i in 0..100u32 {
            index.delete_single_docid(&mut wtxn, 0, &(i as f64), i);
        }
        index.verify_structure_validity(&wtxn, 0);
        index.verify_structure_validity(&wtxn, 1);

        let mut elements = Vec::<((u16, f64), RoaringBitmap)>::new();
        // then add some elements again for the facet id 1
        for i in 0..110u32 {
            // field id = 1, left_bound = i, docids = [i]
            elements.push(((1, i as f64), once(i).collect()));
        }
        index.verify_structure_validity(&wtxn, 0);
        index.verify_structure_validity(&wtxn, 1);
        index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

        wtxn.commit().unwrap();

        milli_snap!(format!("{index}"), name);
    };

    test("default", 4, 5);
    test("small_group_small_min_level", 2, 2);
    test("small_group_large_min_level", 2, 128);
    test("large_group_small_min_level", 16, 2);
    test("odd_group_odd_min_level", 7, 3);
}

#[test]
fn bug_3165() {
    // Indexing a number of facet values that falls within certains ranges (e.g. 22_540 qualifies)
    // would lead to a facet DB which was missing some levels.
    // That was because before writing a level into the database, we would
    // check that its size was higher than the minimum level size using
    // a lossy integer conversion: `level_size as u8 >= min_level_size`.
    //
    // This missing level in the facet DBs would make the incremental indexer
    // (and other search algorithms) crash.
    //
    // https://github.com/meilisearch/meilisearch/issues/3165
    let index = TempIndex::new_with_map_size(4096 * 1000 * 100);

    index
        .update_settings(|settings| {
            settings.set_primary_key("id".to_owned());
            settings
                .set_filterable_fields(vec![FilterableAttributesRule::Field("id".to_string())]);
        })
        .unwrap();

    let mut documents = vec![];
    for i in 0..=22_540 {
        documents.push(
            serde_json::json! {
                {
                    "id": i as u64,
                }
            }
            .as_object()
            .unwrap()
            .clone(),
        );
    }

    let documents = mmap_from_objects(documents);
    index.add_documents(documents).unwrap();

    db_snap!(index, facet_id_f64_docids, "initial", @"c34f499261f3510d862fa0283bbe843a");
}

#[test]
fn insert_string() {
    let test = |name: &str, group_size: u8, min_level_size: u8| {
        let index = FacetIndex::<StrRefCodec>::new(group_size, 0 /*NA*/, min_level_size);

        let strings = (0..1_000).map(|i| ordered_string(i as usize)).collect::<Vec<_>>();
        let mut elements = Vec::<((u16, &str), RoaringBitmap)>::new();
        for i in 0..1_000u32 {
            // field id = 0, left_bound = i, docids = [i]
            elements.push(((0, &strings[i as usize]), once(i).collect()));
        }
        for i in 0..100u32 {
            // field id = 1, left_bound = i, docids = [i]
            elements.push(((1, &strings[i as usize]), once(i).collect()));
        }
        let mut wtxn = index.env.write_txn().unwrap();
        index.bulk_insert(&mut wtxn, &[0, 1], elements.iter());

        index.verify_structure_validity(&wtxn, 0);
        index.verify_structure_validity(&wtxn, 1);

        wtxn.commit().unwrap();

        milli_snap!(format!("{index}"), name);
    };

    test("default", 4, 5);
    test("small_group_small_min_level", 2, 2);
    test("small_group_large_min_level", 2, 128);
    test("large_group_small_min_level", 16, 2);
    test("odd_group_odd_min_level", 7, 3);
}
