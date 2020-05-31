use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

use cow_utils::CowUtils;
use fst::{Streamer, IntoStreamer};
use heed::types::*;
use heed::{EnvOpenOptions, Database};
use levenshtein_automata::LevenshteinAutomatonBuilder;
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
    let headers = match main.get::<_, Str, ByteSlice>(&rtxn, "headers")? {
        Some(headers) => headers,
        None => return Ok(()),
    };

    let fst = match main.get::<_, Str, ByteSlice>(&rtxn, "words-fst")? {
        Some(bytes) => fst::Set::new(bytes)?,
        None => return Ok(()),
    };

    // Building these factories is not free.
    let lev0 = LevenshteinAutomatonBuilder::new(0, true);
    let lev1 = LevenshteinAutomatonBuilder::new(1, true);
    let lev2 = LevenshteinAutomatonBuilder::new(2, true);

    let words: Vec<_> = alphanumeric_tokens(&opt.query).collect();
    let number_of_words = words.len();
    let dfas = words.into_iter().enumerate().map(|(i, word)| {
        let word = word.cow_to_lowercase();
        let is_last = i + 1 == number_of_words;
        let dfa = match word.len() {
            0..=4 => if is_last { lev0.build_prefix_dfa(&word) } else { lev0.build_dfa(&word) },
            5..=8 => if is_last { lev1.build_prefix_dfa(&word) } else { lev1.build_dfa(&word) },
            _     => if is_last { lev2.build_prefix_dfa(&word) } else { lev2.build_dfa(&word) },
        };
        (word, dfa)
    });

    let before = Instant::now();
    let mut intersect_result: Option<RoaringBitmap> = None;
    for (word, dfa) in dfas {
        let before = Instant::now();
        let mut union_result = RoaringBitmap::default();
        let mut stream = fst.search(dfa).into_stream();
        while let Some(word) = stream.next() {
            let word = std::str::from_utf8(word)?;
            if let Some(ids) = postings_ids.get(&rtxn, word)? {
                let right = RoaringBitmap::deserialize_from(ids)?;
                union_result.union_with(&right);
            }
        }
        eprintln!("union for {:?} took {:.02?}", word, before.elapsed());

        intersect_result = match intersect_result.take() {
            Some(mut left) => {
                let before = Instant::now();
                let left_len = left.len();
                left.intersect_with(&union_result);
                eprintln!("intersect between {:?} and {:?} took {:.02?}",
                    left_len, union_result.len(), before.elapsed());
                Some(left)
            },
            None => Some(union_result),
        };
    }

    let mut stdout = io::stdout();
    stdout.write_all(&headers)?;

    let total_length = intersect_result.as_ref().map_or(0, |x| x.len());
    for id in intersect_result.unwrap_or_default().iter().take(20) {
        if let Some(content) = documents.get(&rtxn, &BEU32::new(id))? {
            stdout.write_all(&content)?;
        }
    }

    eprintln!("Took {:.02?} to find {} documents", before.elapsed(), total_length);

    Ok(())
}
