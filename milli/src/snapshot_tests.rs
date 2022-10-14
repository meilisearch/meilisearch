use std::borrow::Cow;
use std::fmt::Write;
use std::path::Path;

use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::{make_db_snap_from_iter, ExternalDocumentsIds, Index};

#[track_caller]
pub fn default_db_snapshot_settings_for_test(name: Option<&str>) -> (insta::Settings, String) {
    let mut settings = insta::Settings::clone_current();
    settings.set_prepend_module_to_snapshot(false);
    let path = Path::new(std::panic::Location::caller().file());
    let filename = path.file_name().unwrap().to_str().unwrap();
    settings.set_omit_expression(true);
    let test_name = std::thread::current().name().unwrap().rsplit("::").next().unwrap().to_owned();

    if let Some(name) = name {
        settings
            .set_snapshot_path(Path::new("snapshots").join(filename).join(&test_name).join(name));
    } else {
        settings.set_snapshot_path(Path::new("snapshots").join(filename).join(&test_name));
    }

    (settings, test_name)
}
#[macro_export]
macro_rules! milli_snap {
    ($value:expr, $name:expr) => {
        let (settings, _) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(None);
        settings.bind(|| {
            let snap = $value;
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(&format!("{}", $name), &snap, false);
            for (name, snap) in snaps {
                insta::assert_snapshot!(name, snap);
            }
        });
    };
    ($value:expr) => {
        let (settings, test_name) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(None);
        settings.bind(|| {
            let snap = $value;
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(&format!("{}", test_name), &snap, false);
            for (name, snap) in snaps {
                insta::assert_snapshot!(name, snap);
            }
        });
    };
    ($value:expr, @$inline:literal) => {
        let (settings, test_name) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(None);
        settings.bind(|| {
            let snap = $value;
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(&format!("{}", test_name), &snap, true);
            for (name, snap) in snaps {
                if !name.ends_with(".full") {
                    insta::assert_snapshot!(snap, @$inline);
                } else {
                    insta::assert_snapshot!(name, snap);
                }
            }
        });
    };
    ($value:expr, $name:expr, @$inline:literal) => {
        let (settings, _) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(None);
        settings.bind(|| {
            let snap = $value;
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(&format!("{}", $name), &snap, true);
            for (name, snap) in snaps {
                if !name.ends_with(".full") {
                    insta::assert_snapshot!(snap, @$inline);
                } else {
                    insta::assert_snapshot!(name, snap);
                }
            }
        });
    };
}

/**
Create a snapshot test of the given database.

## Arguments
1. The identifier for the `Index`
2. The content of the index to snapshot. Available options are:
    - `settings`
    - `word_docids`
    - `exact_word_docids`
    - `word_prefix_docids`
    - `exact_word_prefix_docids`
    - `docid_word_positions`
    - `word_pair_proximity_docids`
    - `word_prefix_pair_proximity_docids`
    - `word_position_docids`
    - `field_id_word_count_docids`
    - `word_prefix_position_docids`
    - `facet_id_f64_docids`
    - `facet_id_string_docids`
    - `documents_ids`
    - `stop_words`
    - `soft_deleted_documents_ids`
    - `field_distribution`
    - `fields_ids_map`
    - `geo_faceted_documents_ids`
    - `external_documents_ids`
    - `number_faceted_documents_ids`
    - `string_faceted_documents_ids`
    - `words_fst`
    - `words_prefixes_fst`

3. The identifier for the snapshot test (optional)
4. `@""` to write the snapshot inline (optional)

## Behaviour
The content of the database will be printed either inline or to the file system
at `test_directory/test_file.rs/test_name/db_name.snap`.

If the database is too large, then only the hash of the database will be saved, with
the name `db_name.hash.snap`. To *also* save the full content of the database anyway,
set the `MILLI_TEST_FULL_SNAPS` environment variable to `true`. The full snapshot will
be saved with the name `db_name.full.snap` but will not be saved to the git repository.

Running `cargo test` will check whether the old snapshot is identical to the
current one. If they are equal, the test passes. Otherwise, the test fails.

Use the command line `cargo insta` to approve or reject new snapshots.

## Example
```ignore
let index = TempIndex::new();

// basic usages
db_snap!(index, word_docids);

// named snapshot to avoid conflicts
db_snap!(index, word_docids, "some_identifier");

// write the snapshot inline
db_snap!(index, word_docids, @""); // will be autocompleted by running `cargo insta review`

// give a name to the inline snapshot
db_snap!(index, word_docids, "some_identifier", @"");
```
*/
#[macro_export]
macro_rules! db_snap {
    ($index:ident, $db_name:ident, $name:expr) => {
        let (settings, _) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(Some(
            &format!("{}", $name),
        ));
        settings.bind(|| {
            let snap = $crate::full_snap_of_db!($index, $db_name);
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(stringify!($db_name), &snap, false);
            for (name, snap) in snaps {
                insta::assert_snapshot!(name, snap);
            }
        });
    };
    ($index:ident, $db_name:ident) => {
        let (settings, _) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(None);
        settings.bind(|| {
            let snap = $crate::full_snap_of_db!($index, $db_name);
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(stringify!($db_name), &snap, false);
            for (name, snap) in snaps {
                insta::assert_snapshot!(name, snap);
            }
        });
    };
    ($index:ident, $db_name:ident, @$inline:literal) => {
        let (settings, _) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(None);
        settings.bind(|| {
            let snap = $crate::full_snap_of_db!($index, $db_name);
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(stringify!($db_name), &snap, true);
            for (name, snap) in snaps {
                if !name.ends_with(".full") {
                    insta::assert_snapshot!(snap, @$inline);
                } else {
                    insta::assert_snapshot!(name, snap);
                }
            }
        });
    };
    ($index:ident, $db_name:ident, $name:expr, @$inline:literal) => {
        let (settings, _) = $crate::snapshot_tests::default_db_snapshot_settings_for_test(Some(&format!("{}", $name)));
        settings.bind(|| {
            let snap = $crate::full_snap_of_db!($index, $db_name);
            let snaps = $crate::snapshot_tests::convert_snap_to_hash_if_needed(stringify!($db_name), &snap, true);
            for (name, snap) in snaps {
                if !name.ends_with(".full") {
                    insta::assert_snapshot!(snap, @$inline);
                } else {
                    insta::assert_snapshot!(name, snap);
                }
            }
        });
    };
}

pub fn snap_word_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, word_docids, |(s, b)| {
        &format!("{s:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_exact_word_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, exact_word_docids, |(s, b)| {
        &format!("{s:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_word_prefix_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, word_prefix_docids, |(s, b)| {
        &format!("{s:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_exact_word_prefix_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, exact_word_prefix_docids, |(s, b)| {
        &format!("{s:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_docid_word_positions(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, docid_word_positions, |((idx, s), b)| {
        &format!("{idx:<6} {s:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_word_pair_proximity_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, word_pair_proximity_docids, |(
        (proximity, word1, word2),
        b,
    )| {
        &format!("{proximity:<2} {word1:<16} {word2:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_word_prefix_pair_proximity_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, word_prefix_pair_proximity_docids, |(
        (proximity, word1, prefix),
        b,
    )| {
        &format!("{proximity:<2} {word1:<16} {prefix:<4} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_prefix_word_pair_proximity_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, prefix_word_pair_proximity_docids, |(
        (proximity, prefix, word2),
        b,
    )| {
        &format!("{proximity:<2} {prefix:<4} {word2:<16} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_word_position_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, word_position_docids, |((word, position), b)| {
        &format!("{word:<16} {position:<6} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_field_id_word_count_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, field_id_word_count_docids, |(
        (field_id, word_count),
        b,
    )| {
        &format!("{field_id:<3} {word_count:<6} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_word_prefix_position_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, word_prefix_position_docids, |(
        (word_prefix, position),
        b,
    )| {
        &format!("{word_prefix:<4} {position:<6} {}", display_bitmap(&b))
    });
    snap
}
pub fn snap_facet_id_f64_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, facet_id_f64_docids, |(
        FacetGroupKey { field_id, level, left_bound },
        FacetGroupValue { size, bitmap },
    )| {
        &format!("{field_id:<3} {level:<2} {left_bound:<6} {size:<2} {}", display_bitmap(&bitmap))
    });
    snap
}
pub fn snap_facet_id_exists_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, facet_id_exists_docids, |(facet_id, docids)| {
        &format!("{facet_id:<3} {}", display_bitmap(&docids))
    });
    snap
}
pub fn snap_facet_id_string_docids(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, facet_id_string_docids, |(
        FacetGroupKey { field_id, level, left_bound },
        FacetGroupValue { size, bitmap },
    )| {
        &format!("{field_id:<3} {level:<2} {left_bound:<12} {size:<2} {}", display_bitmap(&bitmap))
    });
    snap
}
pub fn snap_field_id_docid_facet_strings(index: &Index) -> String {
    let snap = make_db_snap_from_iter!(index, field_id_docid_facet_strings, |(
        (field_id, doc_id, string),
        other_string,
    )| {
        &format!("{field_id:<3} {doc_id:<4} {string:<12} {other_string}")
    });
    snap
}
pub fn snap_documents_ids(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let documents_ids = index.documents_ids(&rtxn).unwrap();

    display_bitmap(&documents_ids)
}
pub fn snap_stop_words(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let stop_words = index.stop_words(&rtxn).unwrap();
    let snap = format!("{stop_words:?}");
    snap
}
pub fn snap_soft_deleted_documents_ids(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let soft_deleted_documents_ids = index.soft_deleted_documents_ids(&rtxn).unwrap();

    display_bitmap(&soft_deleted_documents_ids)
}
pub fn snap_field_distributions(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let mut snap = String::new();
    for (field, count) in index.field_distribution(&rtxn).unwrap() {
        writeln!(&mut snap, "{field:<16} {count:<6}").unwrap();
    }
    snap
}
pub fn snap_fields_ids_map(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let mut snap = String::new();
    for field_id in fields_ids_map.ids() {
        let name = fields_ids_map.name(field_id).unwrap();
        writeln!(&mut snap, "{field_id:<3} {name:<16}").unwrap();
    }
    snap
}
pub fn snap_geo_faceted_documents_ids(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let geo_faceted_documents_ids = index.geo_faceted_documents_ids(&rtxn).unwrap();

    display_bitmap(&geo_faceted_documents_ids)
}
pub fn snap_external_documents_ids(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let ExternalDocumentsIds { soft, hard, .. } = index.external_documents_ids(&rtxn).unwrap();
    let mut snap = String::new();
    let soft_bytes = soft.into_fst().as_bytes().to_owned();
    let mut hex_soft = String::new();
    for byte in soft_bytes {
        write!(&mut hex_soft, "{:x}", byte).unwrap();
    }
    writeln!(&mut snap, "soft: {hex_soft}").unwrap();
    let hard_bytes = hard.into_fst().as_bytes().to_owned();
    let mut hex_hard = String::new();
    for byte in hard_bytes {
        write!(&mut hex_hard, "{:x}", byte).unwrap();
    }
    writeln!(&mut snap, "hard: {hex_hard}").unwrap();
    snap
}
pub fn snap_number_faceted_documents_ids(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let mut snap = String::new();
    for field_id in fields_ids_map.ids() {
        let number_faceted_documents_ids =
            index.faceted_documents_ids(&rtxn, field_id, FacetType::Number).unwrap();
        writeln!(&mut snap, "{field_id:<3} {}", display_bitmap(&number_faceted_documents_ids))
            .unwrap();
    }
    snap
}
pub fn snap_string_faceted_documents_ids(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();

    let mut snap = String::new();
    for field_id in fields_ids_map.ids() {
        let string_faceted_documents_ids =
            index.faceted_documents_ids(&rtxn, field_id, FacetType::String).unwrap();
        writeln!(&mut snap, "{field_id:<3} {}", display_bitmap(&string_faceted_documents_ids))
            .unwrap();
    }
    snap
}
pub fn snap_words_fst(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let words_fst = index.words_fst(&rtxn).unwrap();
    let bytes = words_fst.into_fst().as_bytes().to_owned();
    let mut snap = String::new();
    for byte in bytes {
        write!(&mut snap, "{:x}", byte).unwrap();
    }
    snap
}
pub fn snap_words_prefixes_fst(index: &Index) -> String {
    let rtxn = index.read_txn().unwrap();
    let words_prefixes_fst = index.words_prefixes_fst(&rtxn).unwrap();
    let bytes = words_prefixes_fst.into_fst().as_bytes().to_owned();
    let mut snap = String::new();
    for byte in bytes {
        write!(&mut snap, "{:x}", byte).unwrap();
    }
    snap
}

pub fn snap_settings(index: &Index) -> String {
    let mut snap = String::new();
    let rtxn = index.read_txn().unwrap();

    macro_rules! write_setting_to_snap {
        ($name:ident) => {
            let $name = index.$name(&rtxn).unwrap();
            writeln!(&mut snap, "{}: {:?}", stringify!($name), $name).unwrap();
        };
    }

    write_setting_to_snap!(primary_key);
    write_setting_to_snap!(criteria);
    write_setting_to_snap!(displayed_fields);
    write_setting_to_snap!(distinct_field);
    write_setting_to_snap!(filterable_fields);
    write_setting_to_snap!(sortable_fields);
    write_setting_to_snap!(synonyms);
    write_setting_to_snap!(authorize_typos);
    write_setting_to_snap!(min_word_len_one_typo);
    write_setting_to_snap!(min_word_len_two_typos);
    write_setting_to_snap!(exact_words);
    write_setting_to_snap!(exact_attributes);
    write_setting_to_snap!(max_values_per_facet);
    write_setting_to_snap!(pagination_max_total_hits);
    write_setting_to_snap!(searchable_fields);
    write_setting_to_snap!(user_defined_searchable_fields);

    snap
}

#[macro_export]
macro_rules! full_snap_of_db {
    ($index:ident, settings) => {{
        $crate::snapshot_tests::snap_settings(&$index)
    }};
    ($index:ident, word_docids) => {{
        $crate::snapshot_tests::snap_word_docids(&$index)
    }};
    ($index:ident, exact_word_docids) => {{
        $crate::snapshot_tests::snap_exact_word_docids(&$index)
    }};
    ($index:ident, word_prefix_docids) => {{
        $crate::snapshot_tests::snap_word_prefix_docids(&$index)
    }};
    ($index:ident, exact_word_prefix_docids) => {{
        $crate::snapshot_tests::snap_exact_word_prefix_docids(&$index)
    }};
    ($index:ident, docid_word_positions) => {{
        $crate::snapshot_tests::snap_docid_word_positions(&$index)
    }};
    ($index:ident, word_pair_proximity_docids) => {{
        $crate::snapshot_tests::snap_word_pair_proximity_docids(&$index)
    }};
    ($index:ident, word_prefix_pair_proximity_docids) => {{
        $crate::snapshot_tests::snap_word_prefix_pair_proximity_docids(&$index)
    }};
    ($index:ident, prefix_word_pair_proximity_docids) => {{
        $crate::snapshot_tests::snap_prefix_word_pair_proximity_docids(&$index)
    }};
    ($index:ident, word_position_docids) => {{
        $crate::snapshot_tests::snap_word_position_docids(&$index)
    }};
    ($index:ident, field_id_word_count_docids) => {{
        $crate::snapshot_tests::snap_field_id_word_count_docids(&$index)
    }};
    ($index:ident, word_prefix_position_docids) => {{
        $crate::snapshot_tests::snap_word_prefix_position_docids(&$index)
    }};
    ($index:ident, facet_id_f64_docids) => {{
        $crate::snapshot_tests::snap_facet_id_f64_docids(&$index)
    }};
    ($index:ident, facet_id_string_docids) => {{
        $crate::snapshot_tests::snap_facet_id_string_docids(&$index)
    }};
    ($index:ident, field_id_docid_facet_strings) => {{
        $crate::snapshot_tests::snap_field_id_docid_facet_strings(&$index)
    }};
    ($index:ident, facet_id_exists_docids) => {{
        $crate::snapshot_tests::snap_facet_id_exists_docids(&$index)
    }};
    ($index:ident, documents_ids) => {{
        $crate::snapshot_tests::snap_documents_ids(&$index)
    }};
    ($index:ident, stop_words) => {{
        $crate::snapshot_tests::snap_stop_words(&$index)
    }};
    ($index:ident, soft_deleted_documents_ids) => {{
        $crate::snapshot_tests::snap_soft_deleted_documents_ids(&$index)
    }};
    ($index:ident, field_distribution) => {{
        $crate::snapshot_tests::snap_field_distributions(&$index)
    }};
    ($index:ident, fields_ids_map) => {{
        $crate::snapshot_tests::snap_fields_ids_map(&$index)
    }};
    ($index:ident, geo_faceted_documents_ids) => {{
        $crate::snapshot_tests::snap_geo_faceted_documents_ids(&$index)
    }};
    ($index:ident, external_documents_ids) => {{
        $crate::snapshot_tests::snap_external_documents_ids(&$index)
    }};
    ($index:ident, number_faceted_documents_ids) => {{
        $crate::snapshot_tests::snap_number_faceted_documents_ids(&$index)
    }};
    ($index:ident, string_faceted_documents_ids) => {{
        $crate::snapshot_tests::snap_string_faceted_documents_ids(&$index)
    }};
    ($index:ident, words_fst) => {{
        $crate::snapshot_tests::snap_words_fst(&$index)
    }};
    ($index:ident, words_prefixes_fst) => {{
        $crate::snapshot_tests::snap_words_prefixes_fst(&$index)
    }};
}

pub fn convert_snap_to_hash_if_needed<'snap>(
    name: &str,
    snap: &'snap str,
    inline: bool,
) -> Vec<(String, Cow<'snap, str>)> {
    let store_whole_snapshot = std::env::var("MILLI_TEST_FULL_SNAPS").unwrap_or("false".to_owned());
    let store_whole_snapshot: bool = store_whole_snapshot.parse().unwrap();

    let max_len = if inline { 256 } else { 2048 };

    if snap.len() < max_len {
        vec![(name.to_owned(), Cow::Borrowed(snap))]
    } else {
        let mut r = vec![];
        if store_whole_snapshot {
            r.push((format!("{name}.full"), Cow::Borrowed(snap)));
        }
        let hash = md5::compute(snap.as_bytes());
        let hash_str = format!("{hash:x}");
        r.push((format!("{name}.hash"), Cow::Owned(hash_str)));
        r
    }
}

#[macro_export]
macro_rules! make_db_snap_from_iter {
    ($index:ident, $name:ident, |$vars:pat| $push:block) => {{
        let rtxn = $index.read_txn().unwrap();
        let iter = $index.$name.iter(&rtxn).unwrap();
        let mut snap = String::new();
        for x in iter {
            let $vars = x.unwrap();
            snap.push_str($push);
            snap.push('\n');
        }
        snap
    }};
}

pub fn display_bitmap(b: &RoaringBitmap) -> String {
    let mut s = String::new();
    s.push('[');
    for x in b.into_iter() {
        write!(&mut s, "{x}, ").unwrap();
    }
    s.push(']');
    s
}
