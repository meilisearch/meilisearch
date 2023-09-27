use std::fmt;
use std::time::Instant;

use anyhow::bail;
use roaring::RoaringBitmap;

use crate::Index;

pub fn compare(lhs_database: Index, rhs_database: Index) -> anyhow::Result<()> {
    let Index {
        env,
        main,
        word_docids,
        exact_word_docids,
        word_prefix_docids,
        exact_word_prefix_docids,
        word_pair_proximity_docids,
        word_prefix_pair_proximity_docids,
        prefix_word_pair_proximity_docids,
        word_position_docids,
        word_fid_docids,
        field_id_word_count_docids,
        word_prefix_position_docids,
        word_prefix_fid_docids,
        script_language_docids,
        facet_id_exists_docids,
        facet_id_is_null_docids,
        facet_id_is_empty_docids,
        facet_id_f64_docids,
        facet_id_string_docids,
        facet_id_normalized_string_strings,
        facet_id_string_fst,
        field_id_docid_facet_f64s,
        field_id_docid_facet_strings,
        vector_id_docid,
        documents,
    } = lhs_database;

    let lhs_rtxn = env.read_txn()?;
    let rhs_rtxn = rhs_database.env.read_txn()?;

    compare_iter_roaring_bitmaps(
        "word_docids",
        word_docids.iter(&lhs_rtxn)?,
        rhs_database.word_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "exact_word_docids",
        exact_word_docids.iter(&lhs_rtxn)?,
        rhs_database.exact_word_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_prefix_docids",
        word_prefix_docids.iter(&lhs_rtxn)?,
        rhs_database.word_prefix_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "exact_word_prefix_docids",
        exact_word_prefix_docids.iter(&lhs_rtxn)?,
        rhs_database.exact_word_prefix_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_pair_proximity_docids",
        word_pair_proximity_docids.iter(&lhs_rtxn)?,
        rhs_database.word_pair_proximity_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_prefix_pair_proximity_docids",
        word_prefix_pair_proximity_docids.iter(&lhs_rtxn)?,
        rhs_database.word_prefix_pair_proximity_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "prefix_word_pair_proximity_docids",
        prefix_word_pair_proximity_docids.iter(&lhs_rtxn)?,
        rhs_database.prefix_word_pair_proximity_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_position_docids",
        word_position_docids.iter(&lhs_rtxn)?,
        rhs_database.word_position_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_fid_docids",
        word_fid_docids.iter(&lhs_rtxn)?,
        rhs_database.word_fid_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "field_id_word_count_docids",
        field_id_word_count_docids.iter(&lhs_rtxn)?,
        rhs_database.field_id_word_count_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_prefix_position_docids",
        word_prefix_position_docids.iter(&lhs_rtxn)?,
        rhs_database.word_prefix_position_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "word_prefix_fid_docids",
        word_prefix_fid_docids.iter(&lhs_rtxn)?,
        rhs_database.word_prefix_fid_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "script_language_docids",
        script_language_docids.iter(&lhs_rtxn)?,
        rhs_database.script_language_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "facet_id_exists_docids",
        facet_id_exists_docids.iter(&lhs_rtxn)?,
        rhs_database.facet_id_exists_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "facet_id_is_null_docids",
        facet_id_is_null_docids.iter(&lhs_rtxn)?,
        rhs_database.facet_id_is_null_docids.iter(&rhs_rtxn)?,
    )?;

    compare_iter_roaring_bitmaps(
        "facet_id_is_empty_docids",
        facet_id_is_empty_docids.iter(&lhs_rtxn)?,
        rhs_database.facet_id_is_empty_docids.iter(&rhs_rtxn)?,
    )?;

    Ok(())
}

fn compare_iter_roaring_bitmaps<'txn, KC, DC>(
    name: &str,
    mut lhs: heed::RoIter<'txn, KC, DC>,
    mut rhs: heed::RoIter<'txn, KC, DC>,
) -> anyhow::Result<()>
where
    KC: heed::BytesDecode<'txn>,
    KC::DItem: fmt::Debug + PartialEq,
    DC: heed::BytesDecode<'txn, DItem = RoaringBitmap>,
{
    let before = Instant::now();
    eprintln!("Comparing `{name}`...");

    loop {
        let lhs = lhs.next().transpose()?;
        let rhs = rhs.next().transpose()?;

        match (lhs, rhs) {
            (Some((lkey, ref lvalue)), Some((rkey, ref rvalue))) => {
                if lkey != rkey {
                    bail!("The lhs key `{lkey:?}` is different from the rhs key `{rkey:?}` in the `{name}` database");
                } else if lvalue != rvalue {
                    let missing_from_lhs: Vec<_> = (rvalue - lvalue).into_iter().collect();
                    let missing_from_rhs: Vec<_> = (lvalue - rvalue).into_iter().collect();
                    return Err(anyhow::Error::msg(
                        format!("The keys `{lkey:?}` are present in the `{name}` database but both values are different")
                    ).context(format!("You must modify rhs this way to make it equal to lhs: \n
                        Insert: `{missing_from_rhs:?}`\
                        Remove: `{missing_from_lhs:?}`")));
                }
            }
            (Some((key, _)), None) => bail!("Missing `{key:?}` in the rhs `{name}` database"),
            (None, Some((key, _))) => bail!("Missing `{key:?}` in the lhs `{name}` database"),
            (None, None) => break,
        }
    }

    eprintln!("Both `{name}` are equal (took {:.02?})", before.elapsed());
    Ok(())
}
