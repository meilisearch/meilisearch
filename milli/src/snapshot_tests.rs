use heed::BytesDecode;
use roaring::RoaringBitmap;
use std::path::Path;

use crate::{
    heed_codec::facet::{
        FacetLevelValueU32Codec, FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec,
        FacetStringZeroBoundsValueCodec,
    },
    CboRoaringBitmapCodec, ExternalDocumentsIds, Index,
};

macro_rules! snapshot_index {
    ($index:expr, $name:expr) => {
        $crate::index::tests::snapshot_index($index, $name, None, None)
    };
    ($index:expr, $name:expr, include: $regex:literal) => {
        $crate::index::tests::snapshot_index(
            $index,
            $name,
            Some(regex::Regex::new($regex).unwrap()),
            None,
        )
    };
    ($index:expr, $name:expr, exclude: $regex:literal) => {
        $crate::index::tests::snapshot_index(
            $index,
            $name,
            None,
            Some(regex::Regex::new($regex).unwrap()),
        )
    };
}

#[track_caller]
pub fn snapshot_index(
    index: &Index,
    name: &str,
    include: Option<regex::Regex>,
    exclude: Option<regex::Regex>,
) {
    use std::fmt::Write;

    let should_snapshot = |name: &str| -> bool {
        include.as_ref().map(|f| f.is_match(name)).unwrap_or(true)
            && !exclude.as_ref().map(|f| f.is_match(name)).unwrap_or(false)
    };

    let mut settings = insta::Settings::clone_current();
    settings.set_prepend_module_to_snapshot(false);
    let path = Path::new(std::panic::Location::caller().file());
    let path = path.strip_prefix("milli/src").unwrap();
    settings.set_omit_expression(true);
    settings.set_snapshot_path(Path::new("snapshots").join(path).join(name));
    let rtxn = index.read_txn().unwrap();

    let store_whole_snapshot = std::env::var("MILLI_TEST_FULL_SNAPS").unwrap_or("false".to_owned());
    let store_whole_snapshot: bool = store_whole_snapshot.parse().unwrap();

    macro_rules! snapshot_db {
        ($name:ident, |$vars:pat| $push:block) => {
            let name_str = stringify!($name);
            if should_snapshot(name_str) {
                let iter = index.$name.iter(&rtxn).unwrap();
                let mut snap = String::new();
                for x in iter {
                    let $vars = x.unwrap();
                    snap.push_str($push);
                    snap.push('\n');
                }
                if snap.len() < 512 {
                    insta::assert_snapshot!(name_str, snap);
                } else {
                    if store_whole_snapshot {
                        insta::assert_snapshot!(format!("{name_str}.full"), snap);
                    }
                    let hash = md5::compute(snap.as_bytes());
                    let hash_str = format!("{hash:x}");
                    insta::assert_snapshot!(format!("{name_str}.hash"), hash_str);
                }
            }
        };
    }

    fn display_bitmap(b: &RoaringBitmap) -> String {
        let mut s = String::new();
        s.push('[');
        for x in b.into_iter() {
            write!(&mut s, "{x}, ").unwrap();
        }
        s.push(']');
        s
    }

    settings.bind(|| {
        snapshot_db!(word_docids, |(s, b)| { &format!("{s:<16} {}", display_bitmap(&b)) });
        snapshot_db!(exact_word_docids, |(s, b)| { &format!("{s:<16} {}", display_bitmap(&b)) });
        snapshot_db!(word_prefix_docids, |(s, b)| { &format!("{s:<16} {}", display_bitmap(&b)) });
        snapshot_db!(exact_word_prefix_docids, |(s, b)| {
            &format!("{s:<16} {}", display_bitmap(&b))
        });

        snapshot_db!(docid_word_positions, |((idx, s), b)| {
            &format!("{idx:<6} {s:<16} {}", display_bitmap(&b))
        });

        snapshot_db!(word_pair_proximity_docids, |((word1, word2, proximity), b)| {
            &format!("{word1:<16} {word2:<16} {proximity:<2} {}", display_bitmap(&b))
        });

        snapshot_db!(word_prefix_pair_proximity_docids, |((word1, prefix, proximity), b)| {
            &format!("{word1:<16} {prefix:<4} {proximity:<2} {}", display_bitmap(&b))
        });

        snapshot_db!(word_position_docids, |((word, position), b)| {
            &format!("{word:<16} {position:<6} {}", display_bitmap(&b))
        });

        snapshot_db!(field_id_word_count_docids, |((field_id, word_count), b)| {
            &format!("{field_id:<3} {word_count:<6} {}", display_bitmap(&b))
        });

        snapshot_db!(word_prefix_position_docids, |((word_prefix, position), b)| {
            &format!("{word_prefix:<4} {position:<6} {}", display_bitmap(&b))
        });

        snapshot_db!(facet_id_f64_docids, |((facet_id, level, left, right), b)| {
            &format!("{facet_id:<3} {level:<2} {left:<6} {right:<6} {}", display_bitmap(&b))
        });
        {
            let name_str = stringify!(facet_id_string_docids);
            if should_snapshot(name_str) {
                let bytes_db = index.facet_id_string_docids.remap_types::<ByteSlice, ByteSlice>();
                let iter = bytes_db.iter(&rtxn).unwrap();
                let mut snap = String::new();

                for x in iter {
                    let (key, value) = x.unwrap();
                    if let Some((field_id, normalized_str)) =
                        FacetStringLevelZeroCodec::bytes_decode(key)
                    {
                        let (orig_string, docids) =
                            FacetStringLevelZeroValueCodec::bytes_decode(value).unwrap();
                        snap.push_str(&format!(
                            "{field_id:<3} {normalized_str:<8} {orig_string:<8} {}\n",
                            display_bitmap(&docids)
                        ));
                    } else if let Some((field_id, level, left, right)) =
                        FacetLevelValueU32Codec::bytes_decode(key)
                    {
                        snap.push_str(&format!("{field_id:<3} {level:<2} {left:<6} {right:<6} "));
                        let (bounds, docids) = FacetStringZeroBoundsValueCodec::<
                            CboRoaringBitmapCodec,
                        >::bytes_decode(value)
                        .unwrap();
                        if let Some((left, right)) = bounds {
                            snap.push_str(&format!("{left:<8} {right:<8} "));
                        }
                        snap.push_str(&display_bitmap(&docids));
                        snap.push('\n');
                    } else {
                        panic!();
                    }
                }
                insta::assert_snapshot!(name_str, snap);
            }
        }

        // Main - computed settings
        {
            let mut snap = String::new();

            macro_rules! write_setting_to_snap {
                ($name:ident) => {
                    if should_snapshot(&format!("settings.{}", stringify!($name))) {
                        let $name = index.$name(&rtxn).unwrap();
                        writeln!(&mut snap, "{}: {:?}", stringify!($name), $name).unwrap();
                    }
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

            if !snap.is_empty() {
                insta::assert_snapshot!("settings", snap);
            }
        }
        // Main - others
        {
            macro_rules! snapshot_string {
                ($name:ident) => {
                    if should_snapshot(&format!("{}", stringify!($name))) {
                        insta::assert_snapshot!(stringify!($name), $name);
                    }
                };
            }
            {
                let documents_ids = index.documents_ids(&rtxn).unwrap();
                let documents_ids = display_bitmap(&documents_ids);
                snapshot_string!(documents_ids);
            }
            {
                let stop_words = index.stop_words(&rtxn).unwrap();
                let stop_words = format!("{stop_words:?}");
                snapshot_string!(stop_words);
            }
            {
                let soft_deleted_documents_ids = index.soft_deleted_documents_ids(&rtxn).unwrap();
                let soft_deleted_documents_ids = display_bitmap(&soft_deleted_documents_ids);
                snapshot_string!(soft_deleted_documents_ids);
            }

            {
                let mut field_distribution = String::new();
                for (field, count) in index.field_distribution(&rtxn).unwrap() {
                    writeln!(&mut field_distribution, "{field:<16} {count:<6}").unwrap();
                }
                snapshot_string!(field_distribution);
            }
            let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
            {
                let mut snap = String::new();
                for field_id in fields_ids_map.ids() {
                    let name = fields_ids_map.name(field_id).unwrap();
                    writeln!(&mut snap, "{field_id:<3} {name:<16}").unwrap();
                }
                let fields_ids_map = snap;
                snapshot_string!(fields_ids_map);
            }

            {
                let geo_faceted_documents_ids = index.geo_faceted_documents_ids(&rtxn).unwrap();
                let geo_faceted_documents_ids = display_bitmap(&geo_faceted_documents_ids);
                snapshot_string!(geo_faceted_documents_ids);
            }
            // let geo_rtree = index.geo_rtree(&rtxn).unwrap();
            {
                let ExternalDocumentsIds { soft, hard, .. } =
                    index.external_documents_ids(&rtxn).unwrap();
                let mut external_documents_ids = String::new();
                let soft_bytes = soft.into_fst().as_bytes().to_owned();
                let mut hex_soft = String::new();
                for byte in soft_bytes {
                    write!(&mut hex_soft, "{:x}", byte).unwrap();
                }
                writeln!(&mut external_documents_ids, "soft: {hex_soft}").unwrap();
                let hard_bytes = hard.into_fst().as_bytes().to_owned();
                let mut hex_hard = String::new();
                for byte in hard_bytes {
                    write!(&mut hex_hard, "{:x}", byte).unwrap();
                }
                writeln!(&mut external_documents_ids, "hard: {hex_hard}").unwrap();

                snapshot_string!(external_documents_ids);
            }
            {
                let mut snap = String::new();
                for field_id in fields_ids_map.ids() {
                    let number_faceted_documents_ids =
                        index.number_faceted_documents_ids(&rtxn, field_id).unwrap();
                    writeln!(
                        &mut snap,
                        "{field_id:<3} {}",
                        display_bitmap(&number_faceted_documents_ids)
                    )
                    .unwrap();
                }
                let number_faceted_documents_ids = snap;
                snapshot_string!(number_faceted_documents_ids);
            }
            {
                let mut snap = String::new();
                for field_id in fields_ids_map.ids() {
                    let string_faceted_documents_ids =
                        index.string_faceted_documents_ids(&rtxn, field_id).unwrap();
                    writeln!(
                        &mut snap,
                        "{field_id:<3} {}",
                        display_bitmap(&string_faceted_documents_ids)
                    )
                    .unwrap();
                }
                let string_faceted_documents_ids = snap;
                snapshot_string!(string_faceted_documents_ids);
            }
            {
                let words_fst = index.words_fst(&rtxn).unwrap();
                let bytes = words_fst.into_fst().as_bytes().to_owned();
                let mut words_fst = String::new();
                for byte in bytes {
                    write!(&mut words_fst, "{:x}", byte).unwrap();
                }
                snapshot_string!(words_fst);
            }
            {
                let words_prefixes_fst = index.words_prefixes_fst(&rtxn).unwrap();
                let bytes = words_prefixes_fst.into_fst().as_bytes().to_owned();
                let mut words_prefixes_fst = String::new();
                for byte in bytes {
                    write!(&mut words_prefixes_fst, "{:x}", byte).unwrap();
                }
                snapshot_string!(words_prefixes_fst);
            }
        }
    });
}
