use std::fmt::Write as _;
use std::path::PathBuf;
use std::{fmt, io, str};

use anyhow::Context;
use byte_unit::Byte;
use heed::EnvOpenOptions;
use milli::facet::FacetType;
use milli::index::db_name::*;
use milli::{FieldId, Index};
use structopt::StructOpt;
use Command::*;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const ALL_DATABASE_NAMES: &[&str] = &[
    MAIN,
    WORD_DOCIDS,
    WORD_PREFIX_DOCIDS,
    DOCID_WORD_POSITIONS,
    WORD_PAIR_PROXIMITY_DOCIDS,
    WORD_PREFIX_PAIR_PROXIMITY_DOCIDS,
    WORD_POSITION_DOCIDS,
    WORD_PREFIX_POSITION_DOCIDS,
    FIELD_ID_WORD_COUNT_DOCIDS,
    FACET_ID_F64_DOCIDS,
    FACET_ID_STRING_DOCIDS,
    FIELD_ID_DOCID_FACET_F64S,
    FIELD_ID_DOCID_FACET_STRINGS,
    EXACT_WORD_DOCIDS,
    EXACT_WORD_PREFIX_DOCIDS,
    DOCUMENTS,
];

const POSTINGS_DATABASE_NAMES: &[&str] = &[
    WORD_DOCIDS,
    WORD_PREFIX_DOCIDS,
    DOCID_WORD_POSITIONS,
    WORD_PAIR_PROXIMITY_DOCIDS,
    WORD_PREFIX_PAIR_PROXIMITY_DOCIDS,
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

    /// Outputs a CSV with the documents ids where the given words prefixes appears.
    WordsPrefixesDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// The prefixes to display the documents ids of.
        prefixes: Vec<String>,
    },

    /// Outputs a CSV with the documents ids along with the facet numbers where it appears.
    FacetNumbersDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// The field name in the document.
        field_name: String,
    },

    /// Outputs a CSV with the documents ids along with the facet strings where it appears.
    FacetStringsDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// The field name in the document.
        field_name: String,
    },

    /// Outputs a CSV with the documents ids along with the word level positions where it appears.
    WordsLevelPositionsDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// Words appearing in the documents.
        words: Vec<String>,
    },

    /// Outputs a CSV with the documents ids along with
    /// the word prefix level positions where it appears.
    WordPrefixesLevelPositionsDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// Prefixes of words appearing in the documents.
        prefixes: Vec<String>,
    },

    /// Outputs a CSV with the documents ids along with
    /// the field id and the word count where it appears.
    FieldIdWordCountDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// The field name in the document.
        field_name: String,
    },

    /// Outputs a CSV with the documents ids, words and the positions where this word appears.
    DocidsWordsPositions {
        /// Display the whole positions in detail.
        #[structopt(long)]
        full_display: bool,

        /// If defined, only retrieve the documents that corresponds to these internal ids.
        internal_documents_ids: Vec<u32>,
    },

    /// Outputs some facets numbers statistics for the given facet name.
    FacetNumberStats {
        /// The field name in the document.
        field_name: String,
    },

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

    /// Outputs the size in bytes of the specified databases names.
    SizeOfDatabase {
        /// The name of the database to measure the size of, if not specified it's equivalent
        /// to specifying all the databases names.
        #[structopt(possible_values = ALL_DATABASE_NAMES)]
        databases: Vec<String>,
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

    /// Outputs a CSV with the proximities for the two specified words and
    /// the documents ids where these relations appears.
    ///
    /// `word1`, `prefix` defines the word pair specified *in this specific order*.
    /// `proximity` defines the proximity between the two specified words.
    /// `documents_ids` defines the documents ids where the relation appears.
    WordPrefixPairProximitiesDocids {
        /// Display the whole documents ids in details.
        #[structopt(long)]
        full_display: bool,

        /// First word of the word pair.
        word1: String,

        /// Second word of the word pair.
        prefix: String,
    },

    /// Outputs the words FST to standard output.
    ///
    /// One can use the FST binary helper to dissect and analyze it,
    /// you can install it using `cargo install fst-bin`.
    ExportWordsFst,

    /// Outputs the words prefix FST to standard output.
    ///
    /// One can use the FST binary helper to dissect and analyze it,
    /// you can install it using `cargo install fst-bin`.
    ExportWordsPrefixFst,

    /// Outputs the documents as JSON lines to the standard output.
    ///
    /// All of the fields are extracted, not just the displayed ones.
    ExportDocuments {
        /// If defined, only retrieve the documents that corresponds to these internal ids.
        internal_documents_ids: Vec<u32>,
    },
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    let mut options = EnvOpenOptions::new();
    options.map_size(opt.database_size.get_bytes() as usize);

    // Return an error if the database does not exist.
    if !opt.database.exists() {
        anyhow::bail!("The database ({}) does not exist.", opt.database.display());
    }

    // Open the LMDB database.
    let index = Index::new(options, opt.database)?;
    let rtxn = index.read_txn()?;

    match opt.command {
        MostCommonWords { limit } => most_common_words(&index, &rtxn, limit),
        BiggestValues { limit } => biggest_value_sizes(&index, &rtxn, limit),
        WordsDocids { full_display, words } => words_docids(&index, &rtxn, !full_display, words),
        WordsPrefixesDocids { full_display, prefixes } => {
            words_prefixes_docids(&index, &rtxn, !full_display, prefixes)
        }
        FacetNumbersDocids { full_display, field_name } => {
            facet_values_docids(&index, &rtxn, !full_display, FacetType::Number, field_name)
        }
        FacetStringsDocids { full_display, field_name } => {
            facet_values_docids(&index, &rtxn, !full_display, FacetType::String, field_name)
        }
        WordsLevelPositionsDocids { full_display, words } => {
            words_positions_docids(&index, &rtxn, !full_display, words)
        }
        WordPrefixesLevelPositionsDocids { full_display, prefixes } => {
            word_prefixes_positions_docids(&index, &rtxn, !full_display, prefixes)
        }
        FieldIdWordCountDocids { full_display, field_name } => {
            field_id_word_count_docids(&index, &rtxn, !full_display, field_name)
        }
        DocidsWordsPositions { full_display, internal_documents_ids } => {
            docids_words_positions(&index, &rtxn, !full_display, internal_documents_ids)
        }
        FacetNumberStats { field_name } => facet_number_stats(&index, &rtxn, field_name),
        AverageNumberOfWordsByDoc => average_number_of_words_by_doc(&index, &rtxn),
        AverageNumberOfPositionsByWord => average_number_of_positions_by_word(&index, &rtxn),
        SizeOfDatabase { databases } => size_of_databases(&index, &rtxn, databases),
        DatabaseStats { database } => database_stats(&index, &rtxn, &database),
        WordPairProximitiesDocids { full_display, word1, word2 } => {
            word_pair_proximities_docids(&index, &rtxn, !full_display, word1, word2)
        }
        WordPrefixPairProximitiesDocids { full_display, word1, prefix } => {
            word_prefix_pair_proximities_docids(&index, &rtxn, !full_display, word1, prefix)
        }
        ExportWordsFst => export_words_fst(&index, &rtxn),
        ExportWordsPrefixFst => export_words_prefix_fst(&index, &rtxn),
        ExportDocuments { internal_documents_ids } => {
            export_documents(&index, &rtxn, internal_documents_ids)
        }
    }
}

fn most_common_words(index: &Index, rtxn: &heed::RoTxn, limit: usize) -> anyhow::Result<()> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut heap = BinaryHeap::with_capacity(limit + 1);
    for result in index.word_docids.iter(rtxn)? {
        if limit == 0 {
            break;
        }
        let (word, docids) = result?;
        heap.push((Reverse(docids.len()), word));
        if heap.len() > limit {
            heap.pop();
        }
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
/// that can be used for log or display purposes.
fn facet_values_iter<'txn, KC: 'txn, DC: 'txn>(
    rtxn: &'txn heed::RoTxn,
    db: heed::Database<KC, DC>,
    field_id: FieldId,
) -> heed::Result<Box<dyn Iterator<Item = heed::Result<(KC::DItem, DC::DItem)>> + 'txn>>
where
    KC: heed::BytesDecode<'txn>,
    DC: heed::BytesDecode<'txn>,
{
    let iter = db
        .remap_key_type::<heed::types::ByteSlice>()
        .prefix_iter(&rtxn, &field_id.to_be_bytes())?
        .remap_key_type::<KC>();

    Ok(Box::new(iter))
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

    use heed::types::ByteSlice;

    let Index {
        word_docids,
        word_prefix_docids,
        docid_word_positions,
        word_pair_proximity_docids,
        word_prefix_pair_proximity_docids,
        word_position_docids,
        word_prefix_position_docids,
        field_id_word_count_docids,
        facet_id_f64_docids,
        facet_id_string_docids,
        facet_id_exists_docids,
        exact_word_docids,
        exact_word_prefix_docids,
        field_id_docid_facet_f64s: _,
        field_id_docid_facet_strings: _,
        ..
    } = index;

    let main_name = "main";
    let word_docids_name = "word_docids";
    let word_prefix_docids_name = "word_prefix_docids";
    let docid_word_positions_name = "docid_word_positions";
    let word_prefix_pair_proximity_docids_name = "word_prefix_pair_proximity_docids";
    let word_pair_proximity_docids_name = "word_pair_proximity_docids";
    let word_position_docids_name = "word_position_docids";
    let word_prefix_position_docids_name = "word_prefix_position_docids";
    let field_id_word_count_docids_name = "field_id_word_count_docids";
    let facet_id_f64_docids_name = "facet_id_f64_docids";
    let facet_id_string_docids_name = "facet_id_string_docids";
    let facet_id_exists_docids_name = "facet_id_exists_docids";
    let documents_name = "documents";

    let mut heap = BinaryHeap::with_capacity(limit + 1);

    if limit > 0 {
        // Fetch the words FST
        let words_fst = index.words_fst(rtxn)?;
        let length = words_fst.as_fst().as_bytes().len();
        heap.push(Reverse((length, "words-fst".to_string(), main_name)));
        if heap.len() > limit {
            heap.pop();
        }

        // Fetch the word prefix FST
        let words_prefixes_fst = index.words_prefixes_fst(rtxn)?;
        let length = words_prefixes_fst.as_fst().as_bytes().len();
        heap.push(Reverse((length, "words-prefixes-fst".to_string(), main_name)));
        if heap.len() > limit {
            heap.pop();
        }

        let documents_ids = index.documents_ids(rtxn)?;
        heap.push(Reverse((documents_ids.len() as usize, "documents-ids".to_string(), main_name)));
        if heap.len() > limit {
            heap.pop();
        }

        for result in word_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in exact_word_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in word_prefix_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_prefix_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in exact_word_prefix_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_prefix_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in docid_word_positions.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((docid, word), value) = result?;
            let key = format!("{} {}", docid, word);
            heap.push(Reverse((value.len(), key, docid_word_positions_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in word_pair_proximity_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((word1, word2, prox), value) = result?;
            let key = format!("{} {} {}", word1, word2, prox);
            heap.push(Reverse((value.len(), key, word_pair_proximity_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in word_prefix_pair_proximity_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((word, prefix, prox), value) = result?;
            let key = format!("{} {} {}", word, prefix, prox);
            heap.push(Reverse((value.len(), key, word_prefix_pair_proximity_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in word_position_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((word, pos), value) = result?;
            let key = format!("{} {}", word, pos);
            heap.push(Reverse((value.len(), key, word_position_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in word_prefix_position_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((word, pos), value) = result?;
            let key = format!("{} {}", word, pos);
            heap.push(Reverse((value.len(), key, word_prefix_position_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        for result in field_id_word_count_docids.remap_data_type::<ByteSlice>().iter(rtxn)? {
            let ((field_id, word_count), docids) = result?;
            let key = format!("{} {}", field_id, word_count);
            heap.push(Reverse((docids.len(), key, field_id_word_count_docids_name)));
            if heap.len() > limit {
                heap.pop();
            }
        }

        let faceted_fields = index.faceted_fields_ids(rtxn)?;
        let fields_ids_map = index.fields_ids_map(rtxn)?;

        for facet_id in faceted_fields {
            let facet_name = fields_ids_map.name(facet_id).unwrap();

            // List the facet numbers of this facet id.
            let db = facet_id_f64_docids.remap_data_type::<ByteSlice>();
            for result in facet_values_iter(rtxn, db, facet_id)? {
                let ((_fid, level, left, right), value) = result?;
                let mut output = facet_number_value_to_string(level, left, right).1;
                write!(&mut output, " (level {})", level)?;
                let key = format!("{} {}", facet_name, output);
                heap.push(Reverse((value.len(), key, facet_id_f64_docids_name)));
                if heap.len() > limit {
                    heap.pop();
                }
            }

            // List the facet strings of this facet id.
            let db = facet_id_string_docids.remap_data_type::<ByteSlice>();
            for result in facet_values_iter(rtxn, db, facet_id)? {
                let ((_fid, fvalue), value) = result?;
                let key = format!("{} {}", facet_name, fvalue);
                heap.push(Reverse((value.len(), key, facet_id_string_docids_name)));
                if heap.len() > limit {
                    heap.pop();
                }
            }

            // List the docids where the facet exists
            let db = facet_id_exists_docids.remap_data_type::<ByteSlice>();
            for result in facet_values_iter(rtxn, db, facet_id)? {
                let (_fid, value) = result?;
                let key = format!("{}", facet_name);
                heap.push(Reverse((value.len(), key, facet_id_exists_docids_name)));
                if heap.len() > limit {
                    heap.pop();
                }
            }
        }

        for result in index.all_documents(rtxn)? {
            let (id, value) = result?;
            let size = value.iter().map(|(k, v)| k.to_ne_bytes().len() + v.len()).sum();
            heap.push(Reverse((size, id.to_string(), documents_name)));
            if heap.len() > limit {
                heap.pop();
            }
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

fn words_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    words: Vec<String>,
) -> anyhow::Result<()> {
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

fn words_prefixes_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    prefixes: Vec<String>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["prefix", "documents_ids"])?;

    if prefixes.is_empty() {
        for result in index.word_prefix_docids.iter(rtxn)? {
            let (prefix, docids) = result?;
            let docids = if debug {
                format!("{:?}", docids)
            } else {
                format!("{:?}", docids.iter().collect::<Vec<_>>())
            };
            wtr.write_record(&[prefix, &docids])?;
        }
    } else {
        for prefix in prefixes {
            if let Some(docids) = index.word_prefix_docids.get(rtxn, &prefix)? {
                let docids = if debug {
                    format!("{:?}", docids)
                } else {
                    format!("{:?}", docids.iter().collect::<Vec<_>>())
                };
                wtr.write_record(&[prefix, docids])?;
            }
        }
    }

    Ok(wtr.flush()?)
}

fn facet_values_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    facet_type: FacetType,
    field_name: String,
) -> anyhow::Result<()> {
    let fields_ids_map = index.fields_ids_map(&rtxn)?;
    let faceted_fields = index.faceted_fields_ids(&rtxn)?;

    let field_id = fields_ids_map
        .id(&field_name)
        .with_context(|| format!("field {} not found", field_name))?;

    if !faceted_fields.contains(&field_id) {
        anyhow::bail!("field {} is not faceted", field_name);
    }

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());

    match facet_type {
        FacetType::Number => {
            wtr.write_record(&["facet_number", "facet_level", "documents_count", "documents_ids"])?;
            for result in facet_values_iter(rtxn, index.facet_id_f64_docids, field_id)? {
                let ((_fid, level, left, right), docids) = result?;
                let value = facet_number_value_to_string(level, left, right).1;
                let count = docids.len();
                let docids = if debug {
                    format!("{:?}", docids)
                } else {
                    format!("{:?}", docids.iter().collect::<Vec<_>>())
                };
                wtr.write_record(&[value, level.to_string(), count.to_string(), docids])?;
            }
        }
        FacetType::String => {
            wtr.write_record(&["facet_string", "documents_count", "documents_ids"])?;
            for result in facet_values_iter(rtxn, index.facet_id_string_docids, field_id)? {
                let ((_fid, normalized), (_original, docids)) = result?;
                let count = docids.len();
                let docids = if debug {
                    format!("{:?}", docids)
                } else {
                    format!("{:?}", docids.iter().collect::<Vec<_>>())
                };
                wtr.write_record(&[normalized.to_string(), count.to_string(), docids])?;
            }
        }
    }

    Ok(wtr.flush()?)
}

fn words_positions_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    words: Vec<String>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "position", "documents_count", "documents_ids"])?;

    for word in words.iter().map(AsRef::as_ref) {
        let range = {
            let left = (word, u32::min_value());
            let right = (word, u32::max_value());
            left..=right
        };
        for result in index.word_position_docids.range(rtxn, &range)? {
            let ((w, pos), docids) = result?;

            let count = docids.len().to_string();
            let docids = if debug {
                format!("{:?}", docids)
            } else {
                format!("{:?}", docids.iter().collect::<Vec<_>>())
            };
            let position = format!("{:?}", pos);
            wtr.write_record(&[w, &position, &count, &docids])?;
        }
    }

    Ok(wtr.flush()?)
}

fn word_prefixes_positions_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    prefixes: Vec<String>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["prefix", "position", "documents_count", "documents_ids"])?;

    for word in prefixes.iter().map(AsRef::as_ref) {
        let range = {
            let left = (word, u32::min_value());
            let right = (word, u32::max_value());
            left..=right
        };
        for result in index.word_prefix_position_docids.range(rtxn, &range)? {
            let ((w, pos), docids) = result?;

            let count = docids.len().to_string();
            let docids = if debug {
                format!("{:?}", docids)
            } else {
                format!("{:?}", docids.iter().collect::<Vec<_>>())
            };
            let position = format!("{:?}", pos);
            wtr.write_record(&[w, &position, &count, &docids])?;
        }
    }

    Ok(wtr.flush()?)
}

fn field_id_word_count_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    field_name: String,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["field_name", "word_count", "docids"])?;

    let field_id = index
        .fields_ids_map(rtxn)?
        .id(&field_name)
        .with_context(|| format!("unknown field name: {}", &field_name))?;

    let left = (field_id, 0);
    let right = (field_id, u8::max_value());
    let iter = index.field_id_word_count_docids.range(rtxn, &(left..=right))?;

    for result in iter {
        let ((_, word_count), docids) = result?;
        let docids = if debug {
            format!("{:?}", docids)
        } else {
            format!("{:?}", docids.iter().collect::<Vec<_>>())
        };
        wtr.write_record(&[&field_name, &format!("{}", word_count), &docids])?;
    }

    Ok(wtr.flush()?)
}

fn docids_words_positions(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    internal_ids: Vec<u32>,
) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["document_id", "word", "positions"])?;

    let iter: Box<dyn Iterator<Item = _>> = if internal_ids.is_empty() {
        Box::new(index.docid_word_positions.iter(rtxn)?)
    } else {
        let vec: heed::Result<Vec<_>> = internal_ids
            .into_iter()
            .map(|id| index.docid_word_positions.prefix_iter(rtxn, &(id, "")))
            .collect();
        Box::new(vec?.into_iter().flatten())
    };

    for result in iter {
        let ((id, word), positions) = result?;
        let positions = if debug {
            format!("{:?}", positions)
        } else {
            format!("{:?}", positions.iter().collect::<Vec<_>>())
        };
        wtr.write_record(&[&id.to_string(), word, &positions])?;
    }

    Ok(wtr.flush()?)
}

fn facet_number_stats(index: &Index, rtxn: &heed::RoTxn, field_name: String) -> anyhow::Result<()> {
    let fields_ids_map = index.fields_ids_map(&rtxn)?;
    let faceted_fields = index.faceted_fields_ids(&rtxn)?;

    let field_id = fields_ids_map
        .id(&field_name)
        .with_context(|| format!("field {} not found", field_name))?;

    if !faceted_fields.contains(&field_id) {
        anyhow::bail!("field {} is not faceted", field_name);
    }

    let iter = facet_values_iter(rtxn, index.facet_id_f64_docids, field_id)?;
    println!("The database {:?} facet stats", field_name);

    let mut level_size = 0;
    let mut current_level = None;
    for result in iter {
        let ((_fid, level, _left, _right), _) = result?;
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

fn export_words_prefix_fst(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut stdout = io::stdout();
    let words_prefixes_fst = index.words_prefixes_fst(rtxn)?;
    stdout.write_all(words_prefixes_fst.as_fst().as_bytes())?;

    Ok(())
}

fn export_documents(
    index: &Index,
    rtxn: &heed::RoTxn,
    internal_ids: Vec<u32>,
) -> anyhow::Result<()> {
    use std::io::{BufWriter, Write as _};

    use milli::obkv_to_json;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout);

    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let displayed_fields: Vec<_> = fields_ids_map.iter().map(|(id, _name)| id).collect();

    let iter: Box<dyn Iterator<Item = _>> = if internal_ids.is_empty() {
        Box::new(index.all_documents(rtxn)?.map(|result| result.map(|(_id, obkv)| obkv)))
    } else {
        Box::new(
            index
                .documents(rtxn, internal_ids.into_iter())?
                .into_iter()
                .map(|(_id, obkv)| Ok(obkv)),
        )
    };

    for result in iter {
        let obkv = result?;
        let document = obkv_to_json(&displayed_fields, &fields_ids_map, obkv)?;
        serde_json::to_writer(&mut out, &document)?;
        writeln!(&mut out)?;
    }

    out.into_inner()?;

    Ok(())
}

fn average_number_of_words_by_doc(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use heed::types::DecodeIgnore;
    use milli::{BEU32StrCodec, DocumentId};

    let mut words_counts = Vec::new();
    let mut count = 0;
    let mut prev = None as Option<(DocumentId, u32)>;

    let iter =
        index.docid_word_positions.as_polymorph().iter::<_, BEU32StrCodec, DecodeIgnore>(rtxn)?;
    for result in iter {
        let ((docid, _word), ()) = result?;

        match prev.as_mut() {
            Some((prev_docid, prev_count)) if docid == *prev_docid => {
                *prev_count += 1;
            }
            Some((prev_docid, prev_count)) => {
                words_counts.push(*prev_count);
                *prev_docid = docid;
                *prev_count = 0;
                count += 1;
            }
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
    use milli::BoRoaringBitmapCodec;

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

fn size_of_databases(index: &Index, rtxn: &heed::RoTxn, names: Vec<String>) -> anyhow::Result<()> {
    use heed::types::ByteSlice;

    let Index {
        word_docids,
        word_prefix_docids,
        docid_word_positions,
        word_pair_proximity_docids,
        word_prefix_pair_proximity_docids,
        word_position_docids,
        word_prefix_position_docids,
        field_id_word_count_docids,
        facet_id_f64_docids,
        facet_id_string_docids,
        field_id_docid_facet_f64s,
        field_id_docid_facet_strings,
        facet_id_exists_docids,
        exact_word_prefix_docids,
        exact_word_docids,
        ..
    } = index;

    let names = if names.is_empty() {
        ALL_DATABASE_NAMES.iter().map(|s| s.to_string()).collect()
    } else {
        names
    };

    for name in names {
        let database = match name.as_str() {
            WORD_PREFIX_DOCIDS => word_prefix_docids.as_polymorph(),
            WORD_DOCIDS => word_docids.as_polymorph(),
            DOCID_WORD_POSITIONS => docid_word_positions.as_polymorph(),
            WORD_PAIR_PROXIMITY_DOCIDS => word_pair_proximity_docids.as_polymorph(),
            WORD_PREFIX_PAIR_PROXIMITY_DOCIDS => word_prefix_pair_proximity_docids.as_polymorph(),
            WORD_POSITION_DOCIDS => word_position_docids.as_polymorph(),
            WORD_PREFIX_POSITION_DOCIDS => word_prefix_position_docids.as_polymorph(),
            FIELD_ID_WORD_COUNT_DOCIDS => field_id_word_count_docids.as_polymorph(),
            FACET_ID_F64_DOCIDS => facet_id_f64_docids.as_polymorph(),
            FACET_ID_STRING_DOCIDS => facet_id_string_docids.as_polymorph(),
            FACET_ID_EXISTS_DOCIDS => facet_id_exists_docids.as_polymorph(),
            FIELD_ID_DOCID_FACET_F64S => field_id_docid_facet_f64s.as_polymorph(),
            FIELD_ID_DOCID_FACET_STRINGS => field_id_docid_facet_strings.as_polymorph(),
            EXACT_WORD_DOCIDS => exact_word_docids.as_polymorph(),
            EXACT_WORD_PREFIX_DOCIDS => exact_word_prefix_docids.as_polymorph(),

            unknown => anyhow::bail!("unknown database {:?}", unknown),
        };

        let mut key_size: u64 = 0;
        let mut val_size: u64 = 0;
        let mut number_entries: u64 = 0;
        for result in database.iter::<_, ByteSlice, ByteSlice>(rtxn)? {
            let (k, v) = result?;
            key_size += k.len() as u64;
            val_size += v.len() as u64;
            number_entries += 1;
        }

        println!("The {} database weigh:", name);
        println!("\ttotal key size: {}", Byte::from(key_size).get_appropriate_unit(true));
        println!("\ttotal val size: {}", Byte::from(val_size).get_appropriate_unit(true));
        println!("\ttotal size: {}", Byte::from(key_size + val_size).get_appropriate_unit(true));
        println!("\tnumber of entries: {}", number_entries);
    }

    Ok(())
}

fn database_stats(index: &Index, rtxn: &heed::RoTxn, name: &str) -> anyhow::Result<()> {
    use heed::types::ByteSlice;
    use heed::{BytesDecode, Error};
    use milli::{BoRoaringBitmapCodec, CboRoaringBitmapCodec, RoaringBitmapCodec};
    use roaring::RoaringBitmap;

    fn compute_stats<'a, DC: BytesDecode<'a, DItem = RoaringBitmap>>(
        db: heed::PolyDatabase,
        rtxn: &'a heed::RoTxn,
        name: &str,
    ) -> anyhow::Result<()> {
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
        let len = values_length.len();

        let twenty_five_percentile = values_length.get(len / 4).unwrap_or(&0);
        let fifty_percentile = values_length.get(len / 2).unwrap_or(&0);
        let seventy_five_percentile = values_length.get(len * 3 / 4).unwrap_or(&0);
        let ninety_percentile = values_length.get(len * 90 / 100).unwrap_or(&0);
        let ninety_five_percentile = values_length.get(len * 95 / 100).unwrap_or(&0);
        let ninety_nine_percentile = values_length.get(len * 99 / 100).unwrap_or(&0);
        let minimum = values_length.first().unwrap_or(&0);
        let maximum = values_length.last().unwrap_or(&0);
        let count = values_length.len();
        let sum = values_length.iter().map(|l| *l as u64).sum::<u64>();

        println!("The {} database stats on the lengths", name);
        println!("\tnumber of entries: {}", count);
        println!("\t25th percentile (first quartile): {}", twenty_five_percentile);
        println!("\t50th percentile (median): {}", fifty_percentile);
        println!("\t75th percentile (third quartile): {}", seventy_five_percentile);
        println!("\t90th percentile: {}", ninety_percentile);
        println!("\t95th percentile: {}", ninety_five_percentile);
        println!("\t99th percentile: {}", ninety_nine_percentile);
        println!("\tminimum: {}", minimum);
        println!("\tmaximum: {}", maximum);
        println!("\taverage: {}", sum as f64 / count as f64);
        println!("\ttotal key size: {}", Byte::from(key_size).get_appropriate_unit(true));
        println!("\ttotal val size: {}", Byte::from(val_size).get_appropriate_unit(true));
        println!("\ttotal size: {}", Byte::from(key_size + val_size).get_appropriate_unit(true));

        Ok(())
    }

    match name {
        WORD_DOCIDS => {
            let db = index.word_docids.as_polymorph();
            compute_stats::<RoaringBitmapCodec>(*db, rtxn, name)
        }
        WORD_PREFIX_DOCIDS => {
            let db = index.word_prefix_docids.as_polymorph();
            compute_stats::<RoaringBitmapCodec>(*db, rtxn, name)
        }
        DOCID_WORD_POSITIONS => {
            let db = index.docid_word_positions.as_polymorph();
            compute_stats::<BoRoaringBitmapCodec>(*db, rtxn, name)
        }
        WORD_PAIR_PROXIMITY_DOCIDS => {
            let db = index.word_pair_proximity_docids.as_polymorph();
            compute_stats::<CboRoaringBitmapCodec>(*db, rtxn, name)
        }
        WORD_PREFIX_PAIR_PROXIMITY_DOCIDS => {
            let db = index.word_prefix_pair_proximity_docids.as_polymorph();
            compute_stats::<CboRoaringBitmapCodec>(*db, rtxn, name)
        }
        FIELD_ID_WORD_COUNT_DOCIDS => {
            let db = index.field_id_word_count_docids.as_polymorph();
            compute_stats::<CboRoaringBitmapCodec>(*db, rtxn, name)
        }
        unknown => anyhow::bail!("unknown database {:?}", unknown),
    }
}

fn word_pair_proximities_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    word1: String,
    word2: String,
) -> anyhow::Result<()> {
    use heed::types::ByteSlice;
    use milli::RoaringBitmapCodec;

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
        if key.len() != prefix.len() + 1 {
            continue;
        }

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

fn word_prefix_pair_proximities_docids(
    index: &Index,
    rtxn: &heed::RoTxn,
    debug: bool,
    word1: String,
    word_prefix: String,
) -> anyhow::Result<()> {
    use heed::types::ByteSlice;
    use milli::RoaringBitmapCodec;

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word1", "word_prefix", "proximity", "documents_ids"])?;

    // Create the prefix key with only the pair of words.
    let mut prefix = Vec::with_capacity(word1.len() + word_prefix.len() + 1);
    prefix.extend_from_slice(word1.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(word_prefix.as_bytes());

    let db = index.word_prefix_pair_proximity_docids.as_polymorph();
    let iter = db.prefix_iter::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &prefix)?;
    for result in iter {
        let (key, docids) = result?;

        // Skip keys that are longer than the requested one,
        // a longer key means that the second word is a prefix of the request word.
        if key.len() != prefix.len() + 1 {
            continue;
        }

        let proximity = key.last().unwrap();
        let docids = if debug {
            format!("{:?}", docids)
        } else {
            format!("{:?}", docids.iter().collect::<Vec<_>>())
        };
        wtr.write_record(&[&word1, &word_prefix, &proximity.to_string(), &docids])?;
    }

    Ok(wtr.flush()?)
}
