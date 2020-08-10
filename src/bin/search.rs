use std::io::{self, Write, BufRead};
use std::iter::once;
use std::path::PathBuf;
use std::time::Instant;

use heed::EnvOpenOptions;
use log::debug;
use milli::Index;
use structopt::StructOpt;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "milli-search", about = "A simple search binary for milli project.")]
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

    /// The query string to search for (doesn't support prefix search yet).
    query: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    // Open the LMDB database.
    let index = Index::new(&env, opt.database)?;
    let rtxn = env.read_txn()?;

    let stdin = io::stdin();
    let lines = match opt.query {
        Some(query) => Box::new(once(Ok(query.to_string()))),
        None => Box::new(stdin.lock().lines()) as Box<dyn Iterator<Item = _>>,
    };

    for result in lines {
        let before = Instant::now();

        let query = result?;
        let (_, documents_ids) = index.search(&rtxn, &query)?;
        let headers = match index.headers(&rtxn)? {
            Some(headers) => headers,
            None => return Ok(()),
        };
        let documents = index.documents(documents_ids.iter().cloned())?;

        let mut stdout = io::stdout();
        stdout.write_all(&headers)?;

        for (_id, content) in documents {
            stdout.write_all(&content)?;
        }

        debug!("Took {:.02?} to find {} documents", before.elapsed(), documents_ids.len());
    }

    Ok(())
}
