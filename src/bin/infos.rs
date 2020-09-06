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
