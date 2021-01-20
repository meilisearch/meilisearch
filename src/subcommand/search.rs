use std::io::{self, BufRead, Write};
use std::iter::once;
use std::path::PathBuf;
use std::time::Instant;

use byte_unit::Byte;
use heed::EnvOpenOptions;
use log::debug;
use structopt::StructOpt;

use crate::{Index, obkv_to_json};

#[derive(Debug, StructOpt)]
/// A simple search helper binary for the milli project.
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

    /// The query string to search for (doesn't support prefix search yet).
    query: Option<String>,

    /// Compute and print the facet distribution of all the faceted fields.
    #[structopt(long)]
    print_facet_distribution: bool,
}

pub fn run(opt: Opt) -> anyhow::Result<()> {
    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    std::fs::create_dir_all(&opt.database)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(opt.database_size.get_bytes() as usize);

    // Open the LMDB database.
    let index = Index::new(options, &opt.database)?;
    let rtxn = index.read_txn()?;
    let fields_ids_map = index.fields_ids_map(&rtxn)?;
    let displayed_fields = match index.displayed_fields_ids(&rtxn)? {
        Some(fields) => fields,
        None => fields_ids_map.iter().map(|(id, _)| id).collect(),
    };

    let stdin = io::stdin();
    let lines = match opt.query {
        Some(query) => Box::new(once(Ok(query))),
        None => Box::new(stdin.lock().lines()) as Box<dyn Iterator<Item = _>>,
    };

    let mut stdout = io::stdout();
    for result in lines {
        let before = Instant::now();

        let query = result?;
        let result = index.search(&rtxn).query(query).execute()?;
        let documents = index.documents(&rtxn, result.documents_ids.iter().cloned())?;

        for (_id, record) in documents {
            let val = obkv_to_json(&displayed_fields, &fields_ids_map, record)?;
            serde_json::to_writer(&mut stdout, &val)?;
            let _ = writeln!(&mut stdout);
        }

        if opt.print_facet_distribution {
            let facets = index.facets(&rtxn).candidates(result.candidates).execute()?;
            serde_json::to_writer(&mut stdout, &facets)?;
            let _ = writeln!(&mut stdout);
        }

        debug!("Took {:.02?} to find {} documents", before.elapsed(), result.documents_ids.len());
    }

    Ok(())
}
