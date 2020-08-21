use std::path::PathBuf;
use std::{str, io};

use heed::EnvOpenOptions;
use milli::Index;
use structopt::StructOpt;

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
    let index = Index::new(&env, opt.database)?;
    let rtxn = env.read_txn()?;

    match opt.command {
        Command::MostCommonWords { limit } => most_common_words(&index, &rtxn, limit),
        Command::WordsFrequencies { words } => words_frequencies(&index, &rtxn, words),
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

        let (bytes, postings) = result?;
        let (word, _position) = bytes.split_at(bytes.len() - 4);
        let word = str::from_utf8(word)?;

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
    use roaring::RoaringBitmap;

    let stdout = io::stdout();
    let mut wtr = csv::Writer::from_writer(stdout.lock());
    wtr.write_record(&["word", "document_frequency", "frequency"])?;

    for word in words {
        let mut document_frequency = RoaringBitmap::new();
        let mut frequency = 0;
        for result in index.word_position_docids.prefix_iter(rtxn, word.as_bytes())? {
            let (bytes, postings) = result?;
            let (w, _position) = bytes.split_at(bytes.len() - 4);

            // if the word is not exactly the word we requested then it means
            // we found a word that *starts with* the requested word and we must stop.
            if word.as_bytes() != w { break }

            document_frequency.union_with(&postings);
            frequency += postings.len();
        }
        wtr.write_record(&[word, document_frequency.len().to_string(), frequency.to_string()])?;
    }

    Ok(wtr.flush()?)
}
