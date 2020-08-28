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
    /// `frequency` defines the number times the word appears in all the documents.
    MostCommonWords {
        /// The maximum number of frequencies to return.
        #[structopt(default_value = "10")]
        limit: usize,
    },

    /// Outputs a CSV with the frequencies of the specified words.
    ///
    /// Read the documentation of the `most-common-words` command
    /// for more information about the CSV headers.
    WordsFrequencies {
        /// The words you want to retrieve frequencies of.
        words: Vec<String>,
    },

    /// Outputs a CSV with the biggest entries of the database.
    BiggestValueSizes {
        /// The maximum number of sizes to return.
        #[structopt(default_value = "10")]
        limit: usize,
    },

    /// Outputs a CSV with the document ids for all the positions of the given words.
    WordPositionDocIds {
        /// Show the value entirely, not just the debug version.
        #[structopt(long)]
        full_display: bool,
        /// The words you want to display the values of.
        words: Vec<String>,
    },

    /// Outputs a CSV with all the positions of the given words.
    WordPositions {
        /// Show the value entirely, not just the debug version.
        #[structopt(long)]
        full_display: bool,
        /// The words you want to display the values of.
        words: Vec<String>,
    }
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
        WordsFrequencies { words } => words_frequencies(&index, &rtxn, words),
        BiggestValueSizes { limit } => biggest_value_sizes(&index, &rtxn, limit),
        WordPositionDocIds { full_display, words } => word_position_doc_ids(&index, &rtxn, !full_display, words),
        WordPositions { full_display, words } => word_positions(&index, &rtxn, !full_display, words),
    }
}

fn most_common_words(index: &Index, rtxn: &heed::RoTxn, limit: usize) -> anyhow::Result<()> {
    use std::collections::BinaryHeap;
    use std::cmp::Reverse;
    use roaring::RoaringBitmap;

    let mut heap = BinaryHeap::with_capacity(limit + 1);
    let mut prev = None as Option<(String, u64, RoaringBitmap)>;
    for result in index.word_position_docids.iter(rtxn)? {
        if limit == 0 { break }

        let ((word, _position), postings) = result?;
        match prev.as_mut() {
            Some((prev_word, freq, docids)) if prev_word == word => {
                *freq += postings.len();
                docids.union_with(&postings);
            },
            Some((prev_word, freq, docids)) => {
                heap.push(Reverse((docids.len(), *freq, prev_word.to_string())));
                if heap.len() > limit { heap.pop(); }
                prev = Some((word.to_string(), postings.len(), postings))
            },
            None => prev = Some((word.to_string(), postings.len(), postings)),
        }
    }

    if let Some((prev_word, freq, docids)) = prev {
        heap.push(Reverse((docids.len(), freq, prev_word.to_string())));
        if heap.len() > limit { heap.pop(); }
    }

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "document_frequency", "frequency"])?;

    for Reverse((document_frequency, frequency, word)) in heap.into_sorted_vec() {
        wtr.write_record(&[word, document_frequency.to_string(), frequency.to_string()])?;
    }

    Ok(wtr.flush()?)
}

fn words_frequencies(index: &Index, rtxn: &heed::RoTxn, words: Vec<String>) -> anyhow::Result<()> {
    use heed::BytesDecode;
    use heed::types::ByteSlice;
    use milli::heed_codec::{RoaringBitmapCodec, StrBEU32Codec};
    use roaring::RoaringBitmap;

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "document_frequency", "frequency"])?;

    for word in words {
        let mut document_frequency = RoaringBitmap::new();
        let mut frequency = 0;
        let db = index.word_position_docids.as_polymorph();
        for result in db.prefix_iter::<_, ByteSlice, RoaringBitmapCodec>(rtxn, word.as_bytes())? {
            let (bytes, postings) = result?;
            let (w, _position) = StrBEU32Codec::bytes_decode(bytes).unwrap();

            // if the word is not exactly the word we requested then it means
            // we found a word that *starts with* the requested word and we must stop.
            if word != w { break }

            document_frequency.union_with(&postings);
            frequency += postings.len();
        }
        wtr.write_record(&[word, document_frequency.len().to_string(), frequency.to_string()])?;
    }

    Ok(wtr.flush()?)
}

fn biggest_value_sizes(index: &Index, rtxn: &heed::RoTxn, limit: usize) -> anyhow::Result<()> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use heed::BytesDecode;
    use heed::types::{Str, ByteSlice};
    use milli::heed_codec::StrBEU32Codec;

    let main_name = "main";
    let word_positions_name = "word_positions";
    let word_position_docids_name = "word_position_docids";
    let word_attribute_docids_name = "word_attribute_docids";

    let mut heap = BinaryHeap::with_capacity(limit + 1);

    if limit > 0 {
        if let Some(fst) = index.fst(rtxn)? {
            heap.push(Reverse((fst.as_fst().as_bytes().len(), format!("words-fst"), main_name)));
            if heap.len() > limit { heap.pop(); }
        }

        if let Some(documents) = index.main.get::<_, ByteSlice, ByteSlice>(rtxn, b"documents")? {
            heap.push(Reverse((documents.len(), format!("documents"), main_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in index.word_positions.as_polymorph().iter::<_, Str, ByteSlice>(rtxn)? {
            let (word, value) = result?;
            heap.push(Reverse((value.len(), word.to_string(), word_positions_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in index.word_position_docids.as_polymorph().iter::<_, ByteSlice, ByteSlice>(rtxn)? {
            let (key_bytes, value) = result?;
            let (word, position) = StrBEU32Codec::bytes_decode(key_bytes).unwrap();
            let key = format!("{} {}", word, position);
            heap.push(Reverse((value.len(), key, word_position_docids_name)));
            if heap.len() > limit { heap.pop(); }
        }

        for result in index.word_attribute_docids.as_polymorph().iter::<_, ByteSlice, ByteSlice>(rtxn)? {
            let (key_bytes, value) = result?;
            let (word, attribute) = StrBEU32Codec::bytes_decode(key_bytes).unwrap();
            let key = format!("{} {}", word, attribute);
            heap.push(Reverse((value.len(), key, word_attribute_docids_name)));
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

fn word_position_doc_ids(index: &Index, rtxn: &heed::RoTxn, debug: bool, words: Vec<String>) -> anyhow::Result<()> {
    use heed::BytesDecode;
    use heed::types::ByteSlice;
    use milli::heed_codec::{RoaringBitmapCodec, StrBEU32Codec};

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "position", "document_ids"])?;

    let mut non_debug = Vec::new();
    for word in words {
        let db = index.word_position_docids.as_polymorph();
        for result in db.prefix_iter::<_, ByteSlice, RoaringBitmapCodec>(rtxn, word.as_bytes())? {
            let (bytes, postings) = result?;
            let (w, position) = StrBEU32Codec::bytes_decode(bytes).unwrap();

            // if the word is not exactly the word we requested then it means
            // we found a word that *starts with* the requested word and we must stop.
            if word != w { break }

            let postings_string = if debug {
                format!("{:?}", postings)
            } else {
                non_debug.clear();
                non_debug.extend(postings);
                format!("{:?}", non_debug)
            };

            wtr.write_record(&[&word, &position.to_string(), &postings_string])?;
        }
    }

    Ok(wtr.flush()?)
}

fn word_positions(index: &Index, rtxn: &heed::RoTxn, debug: bool, words: Vec<String>) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "positions"])?;

    let mut non_debug = Vec::new();
    for word in words {
        let postings = index.word_positions.get(rtxn, &word)?
            .with_context(|| format!("could not find word {:?}", &word))?;

        let postings_string = if debug {
            format!("{:?}", postings)
        } else {
            non_debug.clear();
            non_debug.extend(postings);
            format!("{:?}", non_debug)
        };

        wtr.write_record(&[word, postings_string])?;
    }

    Ok(wtr.flush()?)
}
