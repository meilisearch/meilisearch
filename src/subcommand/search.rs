use std::collections::HashMap;
use std::io::{self, BufRead};
use std::iter::once;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use heed::EnvOpenOptions;
use log::debug;
use structopt::StructOpt;

use crate::Index;

#[derive(Debug, StructOpt)]
/// A simple search helper binary for the milli project.
pub struct Opt {
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

pub fn run(opt: Opt) -> anyhow::Result<()> {
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
        let result = index.search(&rtxn).query(query).execute().unwrap();

        let mut stdout = io::stdout();
        let fields_ids_map = index.fields_ids_map(&rtxn)?;
        let documents = index.documents(&rtxn, result.documents_ids.iter().cloned())?;

        for (_id, record) in documents {
            let document: anyhow::Result<HashMap<_, _>> = record.iter()
                .map(|(k, v)| {
                    let key = fields_ids_map.name(k).context("field id not found")?;
                    let val = std::str::from_utf8(v)?;
                    Ok((key, val))
                })
                .collect();

            let document = document?;
            serde_json::to_writer(&mut stdout, &document)?;
        }

        debug!("Took {:.02?} to find {} documents", before.elapsed(), result.documents_ids.len());
    }

    Ok(())
}
