use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use roaring::RoaringBitmap;

use crate::heed_codec::facet::OrderedF64Codec;
use crate::heed_codec::StrRefCodec;
use crate::milli_snap;
use crate::update::facet::test_helpers::FacetIndex;

#[test]
fn append() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    for i in 0..256u16 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    let txn = index.env.read_txn().unwrap();
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}
#[test]
fn many_field_ids_append() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    for i in 0..256u16 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    for i in 0..256u16 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 2, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    for i in 0..256u16 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 1, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    let txn = index.env.read_txn().unwrap();
    index.verify_structure_validity(&txn, 0);
    index.verify_structure_validity(&txn, 1);
    index.verify_structure_validity(&txn, 2);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}
#[test]
fn many_field_ids_prepend() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    for i in (0..256).rev() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    for i in (0..256).rev() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 2, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    for i in (0..256).rev() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);
        let mut txn = index.env.write_txn().unwrap();
        index.insert(&mut txn, 1, &(i as f64), &bitmap);
        txn.commit().unwrap();
    }
    let txn = index.env.read_txn().unwrap();
    index.verify_structure_validity(&txn, 0);
    index.verify_structure_validity(&txn, 1);
    index.verify_structure_validity(&txn, 2);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}

#[test]
fn prepend() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    for i in (0..256).rev() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i);
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
    }

    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}

#[test]
fn shuffled() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    let mut keys = (0..256).collect::<Vec<_>>();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
    keys.shuffle(&mut rng);

    for key in keys {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(key);
        index.insert(&mut txn, 0, &(key as f64), &bitmap);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}

#[test]
fn merge_values() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    let mut keys = (0..256).collect::<Vec<_>>();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
    keys.shuffle(&mut rng);

    for key in keys {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(key);
        bitmap.insert(rng.gen_range(256..512));
        index.verify_structure_validity(&txn, 0);
        index.insert(&mut txn, 0, &(key as f64), &bitmap);
    }

    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}

#[test]
fn delete_from_end() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();
    for i in 0..256 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i);
        index.verify_structure_validity(&txn, 0);
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
    }

    for i in (200..256).rev() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 200);
    let mut txn = index.env.write_txn().unwrap();

    for i in (150..200).rev() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 150);
    let mut txn = index.env.write_txn().unwrap();
    for i in (100..150).rev() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 100);
    let mut txn = index.env.write_txn().unwrap();
    for i in (17..100).rev() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 17);
    let mut txn = index.env.write_txn().unwrap();
    for i in (15..17).rev() {
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 15);
    let mut txn = index.env.write_txn().unwrap();
    for i in (0..15).rev() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 0);
}

#[test]
fn delete_from_start() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    for i in 0..256 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i);
        index.verify_structure_validity(&txn, 0);
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
    }

    for i in 0..128 {
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 127);
    let mut txn = index.env.write_txn().unwrap();
    for i in 128..216 {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 215);
    let mut txn = index.env.write_txn().unwrap();
    for i in 216..256 {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(i as f64), i as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 255);
}

#[test]
#[allow(clippy::needless_range_loop)]
fn delete_shuffled() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();
    for i in 0..256 {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i);
        index.verify_structure_validity(&txn, 0);
        index.insert(&mut txn, 0, &(i as f64), &bitmap);
    }

    let mut keys = (0..256).collect::<Vec<_>>();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
    keys.shuffle(&mut rng);

    for i in 0..128 {
        let key = keys[i];
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(key as f64), key as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 127);
    let mut txn = index.env.write_txn().unwrap();
    for i in 128..216 {
        let key = keys[i];
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(key as f64), key as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    let mut txn = index.env.write_txn().unwrap();
    milli_snap!(format!("{index}"), 215);
    for i in 216..256 {
        let key = keys[i];
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(key as f64), key as u32);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), 255);
}

#[test]
fn in_place_level0_insert() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    let mut keys = (0..16).collect::<Vec<_>>();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
    keys.shuffle(&mut rng);
    for i in 0..4 {
        for &key in keys.iter() {
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(rng.gen_range(i * 256..(i + 1) * 256));
            index.verify_structure_validity(&txn, 0);
            index.insert(&mut txn, 0, &(key as f64), &bitmap);
        }
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"));
}

#[test]
fn in_place_level0_delete() {
    let index = FacetIndex::<OrderedF64Codec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    let mut keys = (0..64).collect::<Vec<_>>();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
    keys.shuffle(&mut rng);

    for &key in keys.iter() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(key);
        bitmap.insert(key + 100);
        index.verify_structure_validity(&txn, 0);

        index.insert(&mut txn, 0, &(key as f64), &bitmap);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), "before_delete");

    let mut txn = index.env.write_txn().unwrap();

    for &key in keys.iter() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &(key as f64), key + 100);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), "after_delete");
}

#[test]
fn shuffle_merge_string_and_delete() {
    let index = FacetIndex::<StrRefCodec>::new(4, 8, 5);
    let mut txn = index.env.write_txn().unwrap();

    let mut keys = (1000..1064).collect::<Vec<_>>();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
    keys.shuffle(&mut rng);

    for &key in keys.iter() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(key);
        bitmap.insert(key + 100);
        index.verify_structure_validity(&txn, 0);
        index.insert(&mut txn, 0, &format!("{key:x}").as_str(), &bitmap);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), "before_delete");

    let mut txn = index.env.write_txn().unwrap();

    for &key in keys.iter() {
        index.verify_structure_validity(&txn, 0);
        index.delete_single_docid(&mut txn, 0, &format!("{key:x}").as_str(), key + 100);
    }
    index.verify_structure_validity(&txn, 0);
    txn.commit().unwrap();
    milli_snap!(format!("{index}"), "after_delete");
}
