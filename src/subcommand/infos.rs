use std::fmt::Write as _;
use std::path::PathBuf;
use std::{str, io, fmt};

use anyhow::Context;
use byte_unit::Byte;
use crate::Index;
use heed::EnvOpenOptions;
use structopt::StructOpt;

use Command::*;

const MAIN_DB_NAME: &str = "main";
const WORD_DOCIDS_DB_NAME: &str = "word-docids";
const DOCID_WORD_POSITIONS_DB_NAME: &str = "docid-word-positions";
const WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME: &str = "word-pair-proximity-docids";
const DOCUMENTS_DB_NAME: &str = "documents";
const USERS_IDS_DOCUMENTS_IDS: &[u8] = b"users-ids-documents-ids";

const ALL_DATABASE_NAMES: &[&str] = &[
    MAIN_DB_NAME,
    WORD_DOCIDS_DB_NAME,
    DOCID_WORD_POSITIONS_DB_NAME,
    WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME,
    DOCUMENTS_DB_NAME,
];

const POSTINGS_DATABASE_NAMES: &[&str] = &[
    WORD_DOCIDS_DB_NAME,
    DOCID_WORD_POSITIONS_DB_NAME,
    WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME,
];

#[derive(Debug, StructOpt)]
/// A stats fetcher for milli.
pub struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "100 GiB")]
    database_size: Byte,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Outputs a CSV of the most frequent words of this index.
    ///
    /// `word` are displayed and ordered by frequency.
    /// `document_frequency` defines the number of documents which contains the word.
    MostCommonWords {
        /// The maximum number of frequencies to return.
        #[structopt(default_value = "10")]
        limit: usize,
    },

    /// Outputs a CSV with the biggest entries of the database.
    BiggestValues {
        /// The maximum number of sizes to return.
        #[structopt(default_value = "10")]
        limit: usize,
    },

    /// Outputs a CSV with the documents ids where the given words appears.
    WordsDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// The words to display the documents ids of.
        words: Vec<String>,
    },

    /// Outputs a CSV with the documents ids along with the facet values where it appears.
    FacetValuesDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// The field name in the document.
        field_name: String,
    },

    /// Outputs some facets statistics for the given facet name.
    FacetStats {
        /// The field name in the document.
        field_name: String,
    },

    /// Outputs the total size of all the docid-word-positions keys and values.
    TotalDocidWordPositionsSize,

    /// Outputs the average number of *different* words by document.
    AverageNumberOfWordsByDoc,

    /// Outputs the average number of positions for each document words.
    AverageNumberOfPositionsByWord,

    /// Outputs some statistics about the given database (e.g. median, quartiles,
    /// percentiles, minimum, maximum, averge, key size, value size).
    DatabaseStats {
        #[structopt(possible_values = POSTINGS_DATABASE_NAMES)]
        database: String,
    },

    /// Outputs the size in bytes of the specified database.
    SizeOfDatabase {
        #[structopt(possible_values = ALL_DATABASE_NAMES)]
        database: String,
    },

    /// Outputs a CSV with the proximities for the two specidied words and
    /// the documents ids where these relations appears.
    ///
    /// `word1`, `word2` defines the word pair specified *in this specific order*.
    /// `proximity` defines the proximity between the two specified words.
    /// `documents_ids` defines the documents ids where the relation appears.
    WordPairProximitiesDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// First word of the word pair.
        word1: String,

        /// Second word of the word pair.
        word2: String,
    },

    /// Outputs the words FST to standard output.
    ///
    /// One can use the FST binary helper to dissect and analyze it,
    /// you can install it using `cargo install fst-bin`.
    ExportWordsFst,

    /// Outputs the documents as JSON lines to the standard output.
    ///
    /// All of the fields are extracted, not just the displayed ones.
    ExportDocuments,

    /// A command that patches the old external ids
    /// into the new external ids format.
    PatchToNewExternalIds,
}

pub fn run(opt: Opt) -> anyhow::Result<()> {
    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    let mut options = EnvOpenOptions::new();
    options.map_size(opt.database_size.get_bytes() as usize);

    // Open the LMDB database.
    let index = Index::new(options, opt.database)?;
    let rtxn = index.read_txn()?;

    match opt.command {
        MostCommonWords { limit } => most_common_words(&index, &rtxn, limit),
        BiggestValues { limit } => biggest_value_sizes(&index, &rtxn, limit),
        WordsDocids { full_display, words } => words_docids(&index, &rtxn, !full_display, words),
        FacetValuesDocids { full_display, field_name } => {
            facet_values_docids(&index, &rtxn, !full_display, field_name)
        },
        FacetStats { field_name } => facet_stats(&index, &rtxn, field_name),
        TotalDocidWordPositionsSize => total_docid_word_positions_size(&index, &rtxn),
        AverageNumberOfWordsByDoc => average_number_of_words_by_doc(&index, &rtxn),
        AverageNumberOfPositionsByWord => {
            average_number_of_positions_by_word(&index, &rtxn)
        },
        SizeOfDatabase { database } => size_of_database(&index, &rtxn, &database),
        DatabaseStats { database } => database_stats(&index, &rtxn, &database),
        WordPairProximitiesDocids { full_display, word1, word2 } => {
            word_pair_proximities_docids(&index, &rtxn, !full_display, word1, word2)
        },
        ExportWordsFst => export_words_fst(&index, &rtxn),
        ExportDocuments => export_documents(&index, &rtxn),
        PatchToNewExternalIds => {
            drop(rtxn);
            let mut wtxn = index.write_txn()?;
            let result = patch_to_new_external_ids(&index, &mut wtxn);
            wtxn.commit()?;
            result
        },
    }
}

fn patch_to_new_external_ids(index: &Index, wtxn: &mut heed::RwTxn) -> anyhow::Result<()> {
    use heed::types::ByteSlice;

    if let Some(documents_ids) = index.main.get::<_, ByteSlice, ByteSlice>(wtxn, USERS_IDS_DOCUMENTS_IDS)? {
        let documents_ids = documents_ids.to_owned();
        index.main.put::<_, ByteSlice, ByteSlice>(
            wtxn,
            crate::index::HARD_EXTERNAL_DOCUMENTS_IDS_KEY.as_bytes(),
            &documents_ids,
        )?;
        index.main.delete::<_, ByteSlice>(wtxn, USERS_IDS_DOCUMENTS_IDS)?;
    }

    Ok(())
}

fn most_common_words(index: &Index, rtxn: &heed::RoTxn, limit: usize) -> anyhow::Result<()> {
    use std::collections::BinaryHeap;
    use std::cmp::Reverse;

    let mut heap = BinaryHeap::with_capacity(limit + 1);
    for result in index.word_docids.iter(rtxn)? {
        if limit == 0 { break }
        let (word, docids) = result?;
        heap.push((Reverse(docids.len()), word));
        if heap.len() > limit { heap.pop(); }
    }

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "document_frequency"])?;

    for (Reverse(document_frequency), word) in heap.into_sorted_vec() {
        wtr.write_record(&[word, &document_frequency.to_string()])?;
    }

    Ok(wtr.flush()?)
}

/// Helper function that converts the facet value key to a unique type
/// that can be used to log or display purposes.
fn facet_values_iter<'txn, DC: 'txn, T>(
    rtxn: &'txn heed::RoTxn,
    db: heed::Database<heed::types::ByteSlice, DC>,
    field_id: u8,
    facet_type: crate::facet::FacetType,
    string_fn: impl Fn(&str) -> T + 'txn,
    float_fn: impl Fn(u8, f64, f64) -> T + 'txn,
    integer_fn: impl Fn(u8, i64, i64) -> T + 'txn,
) -> heed::Result<Box<dyn Iterator<Item=heed::Result<(T, DC::DItem)>> + 'txn>>
where
    DC: heed::BytesDecode<'txn>,
{
    use crate::facet::FacetType;
    use crate::heed_codec::facet::{
        FacetValueStringCodec, FacetLevelValueF64Codec, FacetLevelValueI64Codec,
    };

    let iter = db.prefix_iter(&rtxn, &[field_id])?;
    match facet_type {
        FacetType::String => {
            let iter = iter.remap_key_type::<FacetValueStringCodec>()
                .map(move |r| r.map(|((_, key), value)| (string_fn(key), value)));
            Ok(Box::new(iter) as Box<dyn Iterator<Item=_>>)
        },
        FacetType::Float => {
            let iter = iter.remap_key_type::<FacetLevelValueF64Codec>()
                .map(move |r| r.map(|((_, level, left, right), value)| {
                    (float_fn(level, left, right), value)
                }));
            Ok(Box::new(iter))
        },
        FacetType::Integer => {
            let iter = iter.remap_key_type::<FacetLevelValueI64Codec>()
                .map(move |r| r.map(|((_, level, left, right), value)| {
                    (integer_fn(level, left, right), value)
                }));
            Ok(Box::new(iter))
        },
    }
}

fn facet_number_value_to_string<T: fmt::Debug>(level: u8, left: T, right: T) -> (u8, String) {
    if level == 0 {
        (level, format!("{:?}", left))
    } else {
        (level, format!("{:?} to {:?}", left, right))
    }
}

fn biggest_value_sizes(index: &Index, rtxn: &heed::RoTxn, limit: usize) -> anyhow::Result<()> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use heed::types::{Str, ByteSlice};

    let Index {
        env: _env,
        main,
        word_docids,
        docid_word_positions,
        word_pair_proximity_docids,
        facet_field_id_value_docids,
        field_id_docid_facet_values: _,
        documents,
    } = index;

    let main_name = "main";
    let word_docids_name = "word_docids";
    let docid_word_positions_name = "docid_word_positions";
    let word_pair_proximity_docids_name = "word_pair_proximity_docids";
    let facet_field_id_value_docids_name = "facet_field_id_value_docids";
    let documents_name = "documents";

    let mut heap = BinaryHeap::with_capacity(limit + 1);

    if limit > 0 {
        let words_fst = index.words_fst(rtxn)?;
        heap.push(Reverse((words_fst.as_fst().as_bytes().len(), format!("words-fst"), main_name)));
        if heap.len() > limit { heap.pop(); }

        if let Some(documents_ids) = main.get::<_, Str, ByteSlice>(rtxn, "documents-ids")? {
            heap.push(Reverse((documents_ids.len(), format!("documents-ids"), main_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in word_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_docids_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in docid_word_positions.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((docid, word), value) = result?;
            let key = format!("{} {}", docid, word);
            heap.push(Reverse((value.len(), key, docid_word_positions_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in word_pair_proximity_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((word1, word2, prox), value) = result?;
            let key = format!("{} {} {}", word1, word2, prox);
            heap.push(Reverse((value.len(), key, word_pair_proximity_docids_name)));
            if heap.len() > limit { heap.pop(); }
        }

        let faceted_fields = index.faceted_fields_ids(rtxn)?;
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        for (field_id, field_type) in faceted_fields {
            let facet_name = fields_ids_map.name(field_id).unwrap();

            let db = facet_field_id_value_docids.remap_data_type::<ByteSlice>();
            let iter = facet_values_iter(
                rtxn,
                db,
                field_id,
                field_type,
                |key| key.to_owned(),
                |level, left, right| {
                    let mut output = facet_number_value_to_string(level, left, right).1;
                    let _ = write!(&mut output, " (level {})", level);
                    output
                },
                |level, left, right| {
                    let mut output = facet_number_value_to_string(level, left, right).1;
                    let _ = write!(&mut output, " (level {})", level);
                    output
                },
            )?;

            for result in iter {
                let (fvalue, value) = result?;
                let key = format!("{} {}", facet_name, fvalue);
                heap.push(Reverse((value.len(), key, facet_field_id_value_docids_name)));
                if heap.len() > limit { heap.pop(); }
            }
        }

        for result in documents.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let (id, value) = result?;
            heap.push(Reverse((value.len(), id.to_string(), documents_name)));
            if heap.len() > limit { heap.pop(); }
        }
    }

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["database_name", "key_name", "size"])?;

    for Reverse((size, key_name, database_name)) in heap.into_sorted_vec() {
        wtr.write_record(&[database_name.to_string(), key_name, size.to_string()])?;
    }

    Ok(wtr.flush()?)
}

fn words_docids(index: &Index, rtxn: &heed::RoTxn, debug: bool, words: Vec<String>) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "documents_ids"])?;

    for word in words {
        if let Some(docids) = index.word_docids.get(rtxn, &word)? {
            let docids = if debug {
                format!("{:?}", docids)
            } else {
                format!("{:?}", docids.iter().collect::<Vec<_>>())
            };
            wtr.write_record(&[word, docids])?;
        }
    }

    Ok(wtr.flush()?)
}

fn facet_values_docids(index: &Index, rtxn: &heed::RoTxn, debug: bool, field_name: String) -> anyhow::Result<()> {
    let fields_ids_map = index.fields_ids_map(&rtxn)?;
    let faceted_fields = index.faceted_fields_ids(&rtxn)?;

    let field_id = fields_ids_map.id(&field_name)
        .with_context(|| format!("field {} not found", field_name))?;
    let field_type = faceted_fields.get(&field_id)
        .with_context(|| format!("field {} is not faceted", field_name))?;

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["facet_value", "facet_level", "documents_count", "documents_ids"])?;

    let db = index.facet_field_id_value_docids;
    let iter = facet_values_iter(
        rtxn,
        db,
        field_id,
        *field_type,
        |key| (0, key.to_owned()),
        facet_number_value_to_string,
        facet_number_value_to_string,
    )?;

    for result in iter {
        let ((level, value), docids) = result?;
        let count = docids.len();
        let docids = if debug {
            format!("{:?}", docids)
        } else {
            format!("{:?}", docids.iter().collect::<Vec<_>>())
        };
        wtr.write_record(&[value, level.to_string(), count.to_string(), docids])?;
    }

    Ok(wtr.flush()?)
}

fn facet_stats(index: &Index, rtxn: &heed::RoTxn, field_name: String) -> anyhow::Result<()> {
    let fields_ids_map = index.fields_ids_map(&rtxn)?;
    let faceted_fields = index.faceted_fields_ids(&rtxn)?;

    let field_id = fields_ids_map.id(&field_name)
        .with_context(|| format!("field {} not found", field_name))?;
    let field_type = faceted_fields.get(&field_id)
        .with_context(|| format!("field {} is not faceted", field_name))?;

    let db = index.facet_field_id_value_docids;
    let iter = facet_values_iter(
        rtxn,
        db,
        field_id,
        *field_type,
        |_key| 0u8,
        |level, _left, _right| level,
        |level, _left, _right| level,
    )?;

    println!("The database {:?} facet stats", field_name);

    let mut level_size = 0;
    let mut current_level = None;
    for result in iter {
        let (level, _) = result?;
        if let Some(current) = current_level {
            if current != level {
                println!("\tnumber of groups at level {}: {}", current, level_size);
                level_size = 0;
            }
        }
        current_level = Some(level);
        level_size += 1;
    }

    if let Some(current) = current_level {
        println!("\tnumber of groups at level {}: {}", current, level_size);
    }

    Ok(())
}

fn export_words_fst(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut stdout = io::stdout();
    let words_fst = index.words_fst(rtxn)?;
    stdout.write_all(words_fst.as_fst().as_bytes())?;

    Ok(())
}

fn export_documents(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use std::io::{BufWriter, Write as _};
    use crate::obkv_to_json;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout);

    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let displayed_fields: Vec<_> = fields_ids_map.iter().map(|(id, _name)| id).collect();

    for result in index.documents.iter(rtxn)? {
        let (_id, obkv) = result?;
        let document = obkv_to_json(&displayed_fields, &fields_ids_map, obkv)?;
        serde_json::to_writer(&mut out, &document)?;
        writeln!(&mut out)?;
    }

    out.into_inner()?;

    Ok(())
}

fn total_docid_word_positions_size(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use heed::types::ByteSlice;

    let mut total_key_size = 0;
    let mut total_val_size = 0;
    let mut count = 0;

    let iter = index.docid_word_positions.as_polymorph().iter::<_, ByteSlice, ByteSlice>(rtxn)?;
    for result in iter {
        let (key, val) = result?;
        total_key_size += key.len();
        total_val_size += val.len();
        count += 1;
    }

    println!("number of keys: {}", count);
    println!("total key size: {}", total_key_size);
    println!("total value size: {}", total_val_size);

    Ok(())
}

fn average_number_of_words_by_doc(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use heed::types::DecodeIgnore;
    use crate::{DocumentId, BEU32StrCodec};

    let mut words_counts = Vec::new();
    let mut count = 0;
    let mut prev = None as Option<(DocumentId, u32)>;

    let iter = index.docid_word_positions.as_polymorph().iter::<_, BEU32StrCodec, DecodeIgnore>(rtxn)?;
    for result in iter {
        let ((docid, _word), ()) = result?;

        match prev.as_mut() {
            Some((prev_docid, prev_count)) if docid == *prev_docid => {
                *prev_count += 1;
            },
            Some((prev_docid, prev_count)) => {
                words_counts.push(*prev_count);
                *prev_docid = docid;
                *prev_count = 0;
                count += 1;
            },
            None => prev = Some((docid, 1)),
        }
    }

    if let Some((_, prev_count)) = prev.take() {
        words_counts.push(prev_count);
        count += 1;
    }

    let words_count = words_counts.into_iter().map(|c| c as usize).sum::<usize>() as f64;
    let count = count as f64;

    println!("average number of different words by document: {}", words_count / count);

    Ok(())
}

fn average_number_of_positions_by_word(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use heed::types::DecodeIgnore;
    use crate::BoRoaringBitmapCodec;

    let mut values_length = Vec::new();
    let mut count = 0;

    let db = index.docid_word_positions.as_polymorph();
    for result in db.iter::<_, DecodeIgnore, BoRoaringBitmapCodec>(rtxn)? {
        let ((), val) = result?;
        values_length.push(val.len() as u32);
        count += 1;
    }

    let values_length_sum = values_length.into_iter().map(|c| c as usize).sum::<usize>() as f64;
    let count = count as f64;

    println!("average number of positions by word: {}", values_length_sum / count);

    Ok(())
}

fn size_of_database(index: &Index, rtxn: &heed::RoTxn, name: &str) -> anyhow::Result<()> {
    use heed::types::ByteSlice;

    let database = match name {
        MAIN_DB_NAME => &index.main,
        WORD_DOCIDS_DB_NAME => index.word_docids.as_polymorph(),
        DOCID_WORD_POSITIONS_DB_NAME => index.docid_word_positions.as_polymorph(),
        WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME => index.word_pair_proximity_docids.as_polymorph(),
        DOCUMENTS_DB_NAME => index.documents.as_polymorph(),
        unknown => anyhow::bail!("unknown database {:?}", unknown),
    };

    let mut key_size: u64 = 0;
    let mut val_size: u64 = 0;
    for result in database.iter::<_, ByteSlice, ByteSlice>(rtxn)? {
        let (k, v) = result?;
        key_size += k.len() as u64;
        val_size += v.len() as u64;
    }

    println!("The {} database weigh:", name);
    println!("\ttotal key size: {} bytes", key_size);
    println!("\ttotal val size: {} bytes", val_size);
    println!("\ttotal size: {} bytes", key_size + val_size);

    Ok(())
}

fn database_stats(index: &Index, rtxn: &heed::RoTxn, name: &str) -> anyhow::Result<()> {
    use heed::types::ByteSlice;
    use heed::{Error, BytesDecode};
    use roaring::RoaringBitmap;
    use crate::{BoRoaringBitmapCodec, CboRoaringBitmapCodec, RoaringBitmapCodec};

    fn compute_stats<'a, DC: BytesDecode<'a, DItem = RoaringBitmap>>(
        db: heed::PolyDatabase,
        rtxn: &'a heed::RoTxn,
        name: &str,
    ) -> anyhow::Result<()>
    {
        let mut key_size = 0u64;
        let mut val_size = 0u64;
        let mut values_length = Vec::new();

        for result in db.iter::<_, ByteSlice, ByteSlice>(rtxn)? {
            let (key, val) = result?;
            key_size += key.len() as u64;
            val_size += val.len() as u64;
            let val = DC::bytes_decode(val).ok_or(Error::Decoding)?;
            values_length.push(val.len() as u32);
        }

        values_length.sort_unstable();

        let median = values_length.len() / 2;
        let quartile = values_length.len() / 4;
        let percentile = values_length.len() / 100;

        let twenty_five_percentile = values_length.get(quartile).unwrap_or(&0);
        let fifty_percentile = values_length.get(median).unwrap_or(&0);
        let seventy_five_percentile = values_length.get(quartile * 3).unwrap_or(&0);
        let ninety_percentile = values_length.get(percentile * 90).unwrap_or(&0);
        let ninety_five_percentile = values_length.get(percentile * 95).unwrap_or(&0);
        let ninety_nine_percentile = values_length.get(percentile * 99).unwrap_or(&0);
        let minimum = values_length.first().unwrap_or(&0);
        let maximum = values_length.last().unwrap_or(&0);
        let count = values_length.len();
        let sum = values_length.iter().map(|l| *l as u64).sum::<u64>();

        println!("The {} database stats on the lengths", name);
        println!("\tnumber of proximity pairs: {}", count);
        println!("\t25th percentile (first quartile): {}", twenty_five_percentile);
        println!("\t50th percentile (median): {}", fifty_percentile);
        println!("\t75th percentile (third quartile): {}", seventy_five_percentile);
        println!("\t90th percentile: {}", ninety_percentile);
        println!("\t95th percentile: {}", ninety_five_percentile);
        println!("\t99th percentile: {}", ninety_nine_percentile);
        println!("\tminimum: {}", minimum);
        println!("\tmaximum: {}", maximum);
        println!("\taverage: {}", sum as f64 / count as f64);
        println!("\ttotal key size: {} bytes", key_size);
        println!("\ttotal val size: {} bytes", val_size);
        println!("\ttotal size: {} bytes", key_size + val_size);

        Ok(())
    }

    match name {
        WORD_DOCIDS_DB_NAME => {
            let db = index.word_docids.as_polymorph();
            compute_stats::<RoaringBitmapCodec>(*db, rtxn, name)
        },
        DOCID_WORD_POSITIONS_DB_NAME => {
            let db = index.docid_word_positions.as_polymorph();
            compute_stats::<BoRoaringBitmapCodec>(*db, rtxn, name)
        },
        WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME => {
            let db = index.word_pair_proximity_docids.as_polymorph();
            compute_stats::<CboRoaringBitmapCodec>(*db, rtxn, name)
        },
        unknown => anyhow::bail!("unknown database {:?}", unknown),
    }
}

fn word_pair_proximities_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    word1: String,
    word2: String,
) -> anyhow::Result<()>
{
    use heed::types::ByteSlice;
    use crate::RoaringBitmapCodec;

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word1", "word2", "proximity", "documents_ids"])?;

    // Create the prefix key with only the pair of words.
    let mut prefix = Vec::with_capacity(word1.len() + word2.len() + 1);
    prefix.extend_from_slice(word1.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(word2.as_bytes());

    let db = index.word_pair_proximity_docids.as_polymorph();
    let iter = db.prefix_iter::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &prefix)?;
    for result in iter {
        let (key, docids) = result?;

        // Skip keys that are longer than the requested one,
        // a longer key means that the second word is a prefix of the request word.
        if key.len() != prefix.len() + 1 { continue; }

        let proximity = key.last().unwrap();
        let docids = if debug {
            format!("{:?}", docids)
        } else {
            format!("{:?}", docids.iter().collect::<Vec<_>>())
        };
        wtr.write_record(&[&word1, &word2, &proximity.to_string(), &docids])?;
    }

    Ok(wtr.flush()?)
}
