use std::path::PathBuf;
use std::{str, io};

use anyhow::Context;
use heed::EnvOpenOptions;
use milli::Index;
use structopt::StructOpt;

use Command::*;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const MAIN_DB_NAME: &str = "main";
const WORD_DOCIDS_DB_NAME: &str = "word-docids";
const DOCID_WORD_POSITIONS_DB_NAME: &str = "docid-word-positions";
const WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME: &str = "word-pair-proximity-docids";
const DOCUMENTS_DB_NAME: &str = "documents";

const DATABASE_NAMES: &[&str] = &[
    MAIN_DB_NAME,
    WORD_DOCIDS_DB_NAME,
    DOCID_WORD_POSITIONS_DB_NAME,
    WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME,
    DOCUMENTS_DB_NAME,
];

#[derive(Debug, StructOpt)]
#[structopt(name = "milli-info", about = "A stats crawler for milli.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "107374182400")] // 100 GB
    database_size: usize,

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

    /// Outputs the total size of all the docid-word-positions keys and values.
    TotalDocidWordPositionsSize,

    /// Outputs the average number of *different* words by document.
    AverageNumberOfWordsByDoc,

    /// Outputs the average number of positions for each document words.
    AverageNumberOfPositionsByWord,

    /// Outputs some statistics about the words pairs proximities
    /// (median, quartiles, percentiles, minimum, maximum, averge).
    WordPairProximityStats,

    /// Outputs the size in bytes of the specified database.
    SizeOfDatabase {
        #[structopt(possible_values = DATABASE_NAMES)]
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

    /// Outputs the words FST to disk.
    ///
    /// One can use the FST binary helper to dissect and analyze it,
    /// you can install it using `cargo install fst-bin`.
    ExportWordsFst {
        /// The path where the FST will be written.
        #[structopt(short, long, default_value = "words.fst")]
        output: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    let env = EnvOpenOptions::new()
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    // Open the LMDB database.
    let index = Index::new(&env)?;
    let rtxn = env.read_txn()?;

    match opt.command {
        MostCommonWords { limit } => most_common_words(&index, &rtxn, limit),
        BiggestValues { limit } => biggest_value_sizes(&index, &rtxn, limit),
        WordsDocids { full_display, words } => words_docids(&index, &rtxn, !full_display, words),
        TotalDocidWordPositionsSize => total_docid_word_positions_size(&index, &rtxn),
        AverageNumberOfWordsByDoc => average_number_of_words_by_doc(&index, &rtxn),
        AverageNumberOfPositionsByWord => {
            average_number_of_positions_by_word(&index, &rtxn)
        },
        SizeOfDatabase { database } => size_of_database(&index, &rtxn, &database),
        WordPairProximityStats => word_pair_proximity_stats(&index, &rtxn),
        WordPairProximitiesDocids { full_display, word1, word2 } => {
            word_pair_proximities_docids(&index, &rtxn, !full_display, word1, word2)
        },
        ExportWordsFst { output } => export_words_fst(&index, &rtxn, output),
    }
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

fn biggest_value_sizes(index: &Index, rtxn: &heed::RoTxn, limit: usize) -> anyhow::Result<()> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use heed::types::{Str, ByteSlice};
    use milli::heed_codec::BEU32StrCodec;

    let main_name = "main";
    let word_docids_name = "word_docids";
    let docid_word_positions_name = "docid_word_positions";

    let mut heap = BinaryHeap::with_capacity(limit + 1);

    if limit > 0 {
        if let Some(fst) = index.fst(rtxn)? {
            heap.push(Reverse((fst.as_fst().as_bytes().len(), format!("words-fst"), main_name)));
            if heap.len() > limit { heap.pop(); }
        }

        if let Some(documents) = index.main.get::<_, Str, ByteSlice>(rtxn, "documents")? {
            heap.push(Reverse((documents.len(), format!("documents"), main_name)));
            if heap.len() > limit { heap.pop(); }
        }

        if let Some(documents_ids) = index.main.get::<_, Str, ByteSlice>(rtxn, "documents-ids")? {
            heap.push(Reverse((documents_ids.len(), format!("documents-ids"), main_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in index.word_docids.as_polymorph().iter::<_, Str, ByteSlice>(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_docids_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in index.docid_word_positions.as_polymorph().iter::<_, BEU32StrCodec, ByteSlice>(rtxn)? {
            let ((docid, word), value) = result?;
            let key = format!("{} {}", docid, word);
            heap.push(Reverse((value.len(), key, docid_word_positions_name)));
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

fn export_words_fst(index: &Index, rtxn: &heed::RoTxn, output: PathBuf) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::Write as _;

    let mut output = File::create(&output)
        .with_context(|| format!("failed to create {} file", output.display()))?;

    match index.fst(rtxn)? {
        Some(fst) =>  output.write_all(fst.as_fst().as_bytes())?,
        None => {
            let fst = fst::Set::default();
            output.write_all(fst.as_fst().as_bytes())?;
        },
    }

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
    use milli::{DocumentId, BEU32StrCodec};

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

fn size_of_database(index: &Index, rtxn: &heed::RoTxn, name: &str) -> anyhow::Result<()> {
    use heed::types::ByteSlice;

    let database = match name {
        MAIN_DB_NAME => &index.main,
        WORD_DOCIDS_DB_NAME => index.word_docids.as_polymorph(),
        DOCID_WORD_POSITIONS_DB_NAME => index.docid_word_positions.as_polymorph(),
        WORD_PAIR_PROXIMITY_DOCIDS_DB_NAME => index.word_pair_proximity_docids.as_polymorph(),
        DOCUMENTS_DB_NAME => index.documents.as_polymorph(),
        otherwise => anyhow::bail!("unknown database {:?}", otherwise),
    };

    let mut key_size: u64 = 0;
    let mut val_size: u64 = 0;
    for result in database.iter::<_, ByteSlice, ByteSlice>(rtxn)? {
        let (k, v) = result?;
        key_size += k.len() as u64;
        val_size += v.len() as u64;
    }

    eprintln!("The {} database weigh {} bytes in terms of keys and {} bytes in terms of values.",
        name, key_size, val_size,
    );

    Ok(())
}

fn word_pair_proximity_stats(index: &Index, rtxn: &heed::RoTxn) -> anyhow::Result<()> {
    use heed::types::DecodeIgnore;
    use milli::RoaringBitmapCodec;

    let mut values_length = Vec::new();

    let db = index.word_pair_proximity_docids.as_polymorph();
    for result in db.iter::<_, DecodeIgnore, RoaringBitmapCodec>(rtxn)? {
        let ((), val) = result?;
        values_length.push(val.len() as u32);
    }

    values_length.sort_unstable();

    let median = values_length.get(values_length.len() / 2).unwrap_or(&0);
    let first_quartile = values_length.get(values_length.len() / 4).unwrap_or(&0);
    let third_quartile = values_length.get(values_length.len() / 4 * 3).unwrap_or(&0);
    let ninety_percentile = values_length.get(values_length.len() / 100 * 90).unwrap_or(&0);
    let ninety_five_percentile = values_length.get(values_length.len() / 100 * 95).unwrap_or(&0);
    let ninety_nine_percentile = values_length.get(values_length.len() / 100 * 99).unwrap_or(&0);
    let minimum = values_length.first().unwrap_or(&0);
    let maximum = values_length.last().unwrap_or(&0);
    let count = values_length.len();
    let sum = values_length.iter().map(|l| *l as u64).sum::<u64>();

    println!("words pairs proximities stats on the lengths");
    println!("\tnumber of proximity pairs: {}", count);
    println!("\tfirst quartile: {}", first_quartile);
    println!("\tmedian: {}", median);
    println!("\tthird quartile: {}", third_quartile);
    println!("\t90th percentile: {}", ninety_percentile);
    println!("\t95th percentile: {}", ninety_five_percentile);
    println!("\t99th percentile: {}", ninety_nine_percentile);
    println!("\tminimum: {}", minimum);
    println!("\tmaximum: {}", maximum);
    println!("\taverage: {}", sum as f64 / count as f64);

    Ok(())
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
