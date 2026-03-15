use std::ops::Bound;

use roaring::RoaringBitmap;

use crate::heed_codec::facet::{FacetGroupKeyCodec, OrderedF64Codec};
use crate::milli_snap;
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::facet::tests::{
    get_random_looking_index, get_random_looking_index_with_multiple_field_ids,
    get_simple_index, get_simple_index_with_multiple_field_ids,
};
use crate::snapshot_tests::display_bitmap;

#[test]
fn random_looking_index_snap() {
    let index = get_random_looking_index();
    milli_snap!(format!("{index}"), @"3256c76a7c1b768a013e78d5fa6e9ff9");
}

#[test]
fn random_looking_index_with_multiple_field_ids_snap() {
    let index = get_random_looking_index_with_multiple_field_ids();
    milli_snap!(format!("{index}"), @"c3e5fe06a8f1c404ed4935b32c90a89b");
}

#[test]
fn simple_index_snap() {
    let index = get_simple_index();
    milli_snap!(format!("{index}"), @"5dbfa134cc44abeb3ab6242fc182e48e");
}

#[test]
fn simple_index_with_multiple_field_ids_snap() {
    let index = get_simple_index_with_multiple_field_ids();
    milli_snap!(format!("{index}"), @"a4893298218f682bc76357f46777448c");
}

#[test]
fn filter_range_increasing() {
    let indexes = [
        get_simple_index(),
        get_random_looking_index(),
        get_simple_index_with_multiple_field_ids(),
        get_random_looking_index_with_multiple_field_ids(),
    ];
    for (i, index) in indexes.iter().enumerate() {
        let txn = index.env.read_txn().unwrap();
        let mut results = String::new();
        for i in 0..=255 {
            let i = i as f64;
            let start = Bound::Included(0.);
            let end = Bound::Included(i);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            #[allow(clippy::format_push_string)]
            results.push_str(&format!("0 <= . <= {i} : {}\n", display_bitmap(&docids)));
        }
        milli_snap!(results, format!("included_{i}"));
        let mut results = String::new();
        for i in 0..=255 {
            let i = i as f64;
            let start = Bound::Excluded(0.);
            let end = Bound::Excluded(i);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            #[allow(clippy::format_push_string)]
            results.push_str(&format!("0 < . < {i} : {}\n", display_bitmap(&docids)));
        }
        milli_snap!(results, format!("excluded_{i}"));
        txn.commit().unwrap();
    }
}
#[test]
fn filter_range_decreasing() {
    let indexes = [
        get_simple_index(),
        get_random_looking_index(),
        get_simple_index_with_multiple_field_ids(),
        get_random_looking_index_with_multiple_field_ids(),
    ];
    for (i, index) in indexes.iter().enumerate() {
        let txn = index.env.read_txn().unwrap();

        let mut results = String::new();

        for i in (0..=255).rev() {
            let i = i as f64;
            let start = Bound::Included(i);
            let end = Bound::Included(255.);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            results.push_str(&format!("{i} <= . <= 255 : {}\n", display_bitmap(&docids)));
        }

        milli_snap!(results, format!("included_{i}"));

        let mut results = String::new();

        for i in (0..=255).rev() {
            let i = i as f64;
            let start = Bound::Excluded(i);
            let end = Bound::Excluded(255.);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            results.push_str(&format!("{i} < . < 255 : {}\n", display_bitmap(&docids)));
        }

        milli_snap!(results, format!("excluded_{i}"));

        txn.commit().unwrap();
    }
}
#[test]
fn filter_range_pinch() {
    let indexes = [
        get_simple_index(),
        get_random_looking_index(),
        get_simple_index_with_multiple_field_ids(),
        get_random_looking_index_with_multiple_field_ids(),
    ];
    for (i, index) in indexes.iter().enumerate() {
        let txn = index.env.read_txn().unwrap();

        let mut results = String::new();

        for i in (0..=128).rev() {
            let i = i as f64;
            let start = Bound::Included(i);
            let end = Bound::Included(255. - i);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            results.push_str(&format!(
                "{i} <= . <= {r} : {docids}\n",
                r = 255. - i,
                docids = display_bitmap(&docids)
            ));
        }

        milli_snap!(results, format!("included_{i}"));

        let mut results = String::new();

        for i in (0..=128).rev() {
            let i = i as f64;
            let start = Bound::Excluded(i);
            let end = Bound::Excluded(255. - i);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            results.push_str(&format!(
                "{i} <  . < {r} {docids}\n",
                r = 255. - i,
                docids = display_bitmap(&docids)
            ));
        }

        milli_snap!(results, format!("excluded_{i}"));

        txn.commit().unwrap();
    }
}

#[test]
fn filter_range_unbounded() {
    let indexes = [
        get_simple_index(),
        get_random_looking_index(),
        get_simple_index_with_multiple_field_ids(),
        get_random_looking_index_with_multiple_field_ids(),
    ];
    for (i, index) in indexes.iter().enumerate() {
        let txn = index.env.read_txn().unwrap();
        let mut results = String::new();
        for i in 0..=255 {
            let i = i as f64;
            let start = Bound::Included(i);
            let end = Bound::Unbounded;
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            #[allow(clippy::format_push_string)]
            results.push_str(&format!(">= {i}: {}\n", display_bitmap(&docids)));
        }
        milli_snap!(results, format!("start_from_included_{i}"));
        let mut results = String::new();
        for i in 0..=255 {
            let i = i as f64;
            let start = Bound::Unbounded;
            let end = Bound::Included(i);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            #[allow(clippy::format_push_string)]
            results.push_str(&format!("<= {i}: {}\n", display_bitmap(&docids)));
        }
        milli_snap!(results, format!("end_at_included_{i}"));

        let mut docids = RoaringBitmap::new();
        find_docids_of_facet_within_bounds::<OrderedF64Codec>(
            &txn,
            index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
            0,
            &Bound::Unbounded,
            &Bound::Unbounded,
            None,
            &mut docids,
        )
        .unwrap();
        milli_snap!(
            &format!("all field_id 0: {}\n", display_bitmap(&docids)),
            format!("unbounded_field_id_0_{i}")
        );

        let mut docids = RoaringBitmap::new();
        find_docids_of_facet_within_bounds::<OrderedF64Codec>(
            &txn,
            index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
            1,
            &Bound::Unbounded,
            &Bound::Unbounded,
            None,
            &mut docids,
        )
        .unwrap();
        milli_snap!(
            &format!("all field_id 1:  {}\n", display_bitmap(&docids)),
            format!("unbounded_field_id_1_{i}")
        );

        drop(txn);
    }
}

#[test]
fn filter_range_exact() {
    let indexes = [
        get_simple_index(),
        get_random_looking_index(),
        get_simple_index_with_multiple_field_ids(),
        get_random_looking_index_with_multiple_field_ids(),
    ];
    for (i, index) in indexes.iter().enumerate() {
        let txn = index.env.read_txn().unwrap();
        let mut results_0 = String::new();
        let mut results_1 = String::new();
        for i in 0..=255 {
            let i = i as f64;
            let start = Bound::Included(i);
            let end = Bound::Included(i);
            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                0,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            #[allow(clippy::format_push_string)]
            results_0.push_str(&format!("{i}: {}\n", display_bitmap(&docids)));

            let mut docids = RoaringBitmap::new();
            find_docids_of_facet_within_bounds::<OrderedF64Codec>(
                &txn,
                index.content.remap_key_type::<FacetGroupKeyCodec<OrderedF64Codec>>(),
                1,
                &start,
                &end,
                None,
                &mut docids,
            )
            .unwrap();
            #[allow(clippy::format_push_string)]
            results_1.push_str(&format!("{i}: {}\n", display_bitmap(&docids)));
        }
        milli_snap!(results_0, format!("field_id_0_exact_{i}"));
        milli_snap!(results_1, format!("field_id_1_exact_{i}"));

        drop(txn);
    }
}
