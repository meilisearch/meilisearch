use std::fs::File;
use std::path::PathBuf;

use anyhow::bail;
use heed::EnvOpenOptions;
use structopt::StructOpt;

use crate::indexing::{self, IndexerOpt};
use crate::Index;

#[derive(Debug, StructOpt)]
#[structopt(name = "milli-indexer")]
/// The indexer binary of the milli project.
pub struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "107374182400")] // 100 GB
    database_size: usize,

    #[structopt(flatten)]
    indexer: IndexerOpt,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// CSV file to index, if unspecified the CSV is read from standard input.
    ///
    /// You can also provide a ".gz" or ".gzip" CSV file, the indexer will figure out
    /// how to decode and read it.
    ///
    /// Note that it is much faster to index from a file as when the indexer reads from stdin
    /// it will dedicate a thread for that and context switches could slow down the indexing jobs.
    csv_file: Option<PathBuf>,
}

pub fn run(opt: Opt) -> anyhow::Result<()> {
    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    if opt.database.exists() {
        bail!("Database ({}) already exists, delete it to continue.", opt.database.display());
    }

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    let index = Index::new(&env)?;

    let file_path = opt.csv_file.unwrap();
    let gzipped = file_path.extension().map_or(false, |e| e == "gz" || e == "gzip");
    let file = File::open(file_path)?;
    let content = unsafe { memmap::Mmap::map(&file)? };

    indexing::run(&env, &index, &opt.indexer, &content, gzipped)
}
