use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

use cow_utils::CowUtils;
use heed::types::*;
use heed::{EnvOpenOptions, Database};
use roaring::RoaringBitmap;
use structopt::StructOpt;

use mega_mini_indexer::alphanumeric_tokens;
use mega_mini_indexer::BEU32;

#[derive(Debug, StructOpt)]
#[structopt(name = "mm-indexer", about = "The server side of the daugt project.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The query string to search for (doesn't support prefix search yet).
    query: String,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(5)
        .open(opt.database)?;

    let main = env.create_poly_database(None)?;
    let postings_ids: Database<Str, ByteSlice> = env.create_database(Some("postings-ids"))?;
    let documents: Database<OwnedType<BEU32>, ByteSlice> = env.create_database(Some("documents"))?;

    let rtxn = env.read_txn()?;

    let before = Instant::now();
    let mut result: Option<RoaringBitmap> = None;
    for word in alphanumeric_tokens(&opt.query) {
        let word = word.cow_to_lowercase();
        match postings_ids.get(&rtxn, &word)? {
            Some(ids) => {
                let before = Instant::now();
                let right = RoaringBitmap::deserialize_from(ids)?;
                eprintln!("deserialized bitmap for {:?} took {:.02?}", word, before.elapsed());
                result = match result.take() {
                    Some(mut left) => {
                        let before = Instant::now();
                        let left_len = left.len();
                        left.intersect_with(&right);
                        eprintln!("intersect between {:?} and {:?} took {:.02?}",
                            left_len, right.len(), before.elapsed());
                        Some(left)
                    },
                    None => Some(right),
                };
            },
            None => result = Some(RoaringBitmap::default()),
        }
    }

    let headers = match main.get::<_, Str, ByteSlice>(&rtxn, "headers")? {
        Some(headers) => headers,
        None => return Ok(()),
    };

    let mut stdout = io::stdout();
    stdout.write_all(&headers)?;

    let total_length = result.as_ref().map_or(0, |x| x.len());
    for id in result.unwrap_or_default().iter().take(20) {
        if let Some(content) = documents.get(&rtxn, &BEU32::new(id))? {
            stdout.write_all(&content)?;
        }
    }

    eprintln!("Took {:.02?} to find {} documents", before.elapsed(), total_length);

    Ok(())
}
