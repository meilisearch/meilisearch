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
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(10)
        .open(opt.database)?;

    let index = Index::new(&env)?;

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

        let mut stdout = io::stdout();
        stdout.write_all(&headers)?;

        let documents = index.documents(&rtxn)?.unwrap();
        for id in &documents_ids {
            let id_bytes = id.to_be_bytes();
            if let Some(content) = documents.clone().get(&id_bytes)? {
                stdout.write_all(content.as_ref())?;
            }
        }

        debug!("Took {:.02?} to find {} documents", before.elapsed(), documents_ids.len());
    }

    Ok(())
}
