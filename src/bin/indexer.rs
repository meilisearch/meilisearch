use std::collections::hash_map::Entry;
use std::collections::{HashMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, BuildHasher};
use std::{cmp, io};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{ensure, Context};
use fst::{Streamer, set::OpBuilder};
use heed::types::*;
use heed::{Env, EnvOpenOptions};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;
use tempfile::TempDir;

use mega_mini_indexer::cache::ArcCache;
use mega_mini_indexer::{BEU32, Index, DocumentId, FastMap4};

const ONE_MILLION: u32 = 1_000_000;
const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn simple_alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

#[derive(Debug, StructOpt)]
#[structopt(name = "mm-indexer", about = "The indexer side of the MMI project.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// CSV file to index.
    csv_file: Option<PathBuf>,
}

fn put_evicted_into_heed<I>(wtxn: &mut heed::RwTxn, index: &Index, iter: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = (String, (RoaringBitmap, FastMap4<u32, RoaringBitmap>))>
{
    for (word, (positions, positions_docids)) in iter {
        index.word_positions.put(wtxn, &word, &positions)?;

        for (position, docids) in positions_docids {
            let mut key = word.as_bytes().to_vec();
            key.extend_from_slice(&position.to_be_bytes());
            index.word_position_docids.put(wtxn, &key, &docids)?;
        }
    }
    Ok(())
}

fn merge_hashmaps<K, V, S, F>(mut a: HashMap<K, V, S>, mut b: HashMap<K, V, S>, mut merge: F) -> HashMap<K, V, S>
where
    K: Hash + Eq,
    S: BuildHasher,
    F: FnMut(&K, &mut V, V)
{
    for (k, v) in a.iter_mut() {
        if let Some(vb) = b.remove(k) {
            (merge)(k, v, vb)
        }
    }

    a.extend(b);

    a
}

fn index_csv<R: io::Read>(
    wtxn: &mut heed::RwTxn,
    mut rdr: csv::Reader<R>,
    index: &Index,
    num_threads: usize,
    thread_index: usize,
) -> anyhow::Result<()>
{
    eprintln!("Indexing into LMDB...");

    let mut words_cache = ArcCache::<_, (RoaringBitmap, FastMap4<_, RoaringBitmap>)>::new(100_000);

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    let mut document_id = 0usize;
    let mut before = Instant::now();
    let mut document = csv::StringRecord::new();

    while rdr.read_record(&mut document)? {
        document_id = document_id + 1;
        let document_id = DocumentId::try_from(document_id).context("Generated id is too big")?;

        if thread_index == 0 && document_id % ONE_MILLION == 0 {
            eprintln!("Document {}m just processed ({:.02?} elapsed).", document_id / ONE_MILLION, before.elapsed());
            before = Instant::now();
        }

        for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (pos, word) in simple_alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < 500 { // LMDB limits
                    let word = word.to_lowercase(); // TODO cow_to_lowercase
                    let position = (attr * 1000 + pos) as u32;

                    // If this indexing process is not concerned by this word, then ignore it.
                    if fxhash::hash32(&word) as usize % num_threads != thread_index { continue; }

                    match words_cache.get_mut(&word) {
                        (Some(entry), evicted) => {
                            let (ids, positions) = entry;
                            ids.insert(position);
                            positions.entry(position).or_default().insert(document_id);
                            put_evicted_into_heed(wtxn, index, evicted)?;
                        },
                        (None, _evicted) => {
                            let mut key = word.as_bytes().to_vec();
                            key.extend_from_slice(&position.to_be_bytes());

                            let mut words_positions = index.word_positions.get(wtxn, &word)?.unwrap_or_default();
                            let mut words_position_docids = index.word_position_docids.get(wtxn, &key)?.unwrap_or_default();

                            words_positions.insert(position);
                            words_position_docids.insert(document_id);

                            let mut map = FastMap4::default();
                            map.insert(position, words_position_docids);
                            let value = (words_positions, map);

                            let evicted = words_cache.insert(word.clone(), value, |(pa, pda), (pb, pdb)| {
                                (pa | pb, merge_hashmaps(pda, pdb, |_, a, b| RoaringBitmap::union_with(a, &b)))
                            });

                            put_evicted_into_heed(wtxn, index, evicted)?;
                        }
                    }
                }
            }
        }

        if thread_index == 0 {
            // We write the document in the database.
            let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
            writer.write_byte_record(document.as_byte_record())?;
            let document = writer.into_inner()?;
            index.documents.put(wtxn, &BEU32::new(document_id), &document)?;
        }
    }

    put_evicted_into_heed(wtxn, index, words_cache)?;

    // We store the words from the postings.
    let mut new_words = BTreeSet::default();
    let iter = index.word_positions.as_polymorph().iter::<_, Str, DecodeIgnore>(wtxn)?;
    for result in iter {
        let (word, ()) = result?;
        new_words.insert(word);
    }

    let new_words_fst = fst::Set::from_iter(new_words)?;

    index.put_fst(wtxn, &new_words_fst)?;
    index.put_headers(wtxn, &headers)?;

    Ok(())
}

fn compute_words_attributes_docids(wtxn: &mut heed::RwTxn, index: &Index) -> anyhow::Result<()> {
    eprintln!("Computing the attributes documents ids...");

    let fst = match index.fst(&wtxn)? {
        Some(fst) => fst.map_data(|s| s.to_vec())?,
        None => return Ok(()),
    };

    let mut word_attributes = HashMap::new();
    let mut stream = fst.stream();
    while let Some(word) = stream.next() {
        word_attributes.clear();

        // Loop on the word attributes and unions all the documents ids by attribute.
        for result in index.word_position_docids.prefix_iter(wtxn, word)? {
            let (key, docids) = result?;
            let (_key_word, key_pos) = key.split_at(key.len() - 4);
            let key_pos = key_pos.try_into().map(u32::from_be_bytes)?;
            // If the key corresponds to the word (minus the attribute)
            if key.len() == word.len() + 4 {
                let attribute = key_pos / 1000;
                match word_attributes.entry(attribute) {
                    Entry::Vacant(entry) => { entry.insert(docids); },
                    Entry::Occupied(mut entry) => entry.get_mut().union_with(&docids),
                }
            }
        }

        // Write this word attributes unions into LMDB.
        let mut key = word.to_vec();
        for (attribute, docids) in word_attributes.drain() {
            key.truncate(word.len());
            key.extend_from_slice(&attribute.to_be_bytes());
            index.word_attribute_docids.put(wtxn, &key, &docids)?;
        }
    }

    Ok(())
}

fn merge_databases(
    others: Vec<(usize, Option<TempDir>, Env, Index)>,
    wtxn: &mut heed::RwTxn,
    index: &Index,
) -> anyhow::Result<()>
{
    eprintln!("Merging the temporary databases...");

    let mut fsts = Vec::new();
    for (_i, _dir, env, oindex) in others {
        let rtxn = env.read_txn()?;

        // merge and check the headers are equal
        let headers = oindex.headers(&rtxn)?.context("A database is missing the headers")?;
        match index.headers(wtxn)? {
            Some(h) => ensure!(h == headers, "headers are not equal"),
            None => index.put_headers(wtxn, &headers)?,
        };

        // retrieve the FSTs to merge them together in one run.
        let fst = oindex.fst(&rtxn)?.context("A database is missing its FST")?;
        let fst = fst.map_data(|s| s.to_vec())?;
        fsts.push(fst);

        // merge the words positions
        for result in oindex.word_positions.iter(&rtxn)? {
            let (word, pos) = result?;
            index.word_positions.put(wtxn, word, &pos)?;
        }

        // merge the documents ids by word and position
        for result in oindex.word_position_docids.iter(&rtxn)? {
            let (key, docids) = result?;
            index.word_position_docids.put(wtxn, key, &docids)?;
        }

        // merge the documents ids by word and attribute
        for result in oindex.word_attribute_docids.iter(&rtxn)? {
            let (key, docids) = result?;
            index.word_attribute_docids.put(wtxn, key, &docids)?;
        }

        for result in oindex.documents.iter(&rtxn)? {
            let (id, content) = result?;
            index.documents.put(wtxn, &id, &content)?;
        }
    }

    // Merge all the FSTs to create a final one and write it in the final database.
    if let Some(fst) = index.fst(wtxn)? {
        let fst = fst.map_data(|s| s.to_vec())?;
        fsts.push(fst);
    }

    let builder = OpBuilder::from_iter(&fsts);
    let op = builder.r#union();
    let mut builder = fst::set::SetBuilder::memory();
    builder.extend_stream(op)?;
    let fst = builder.into_set();

    index.put_fst(wtxn, &fst)?;

    Ok(())
}

fn open_env_index(path: impl AsRef<Path>) -> anyhow::Result<(Env, Index)> {
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(10)
        .open(path)?;

    let index = Index::new(&env)?;

    Ok((env, index))
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    std::fs::create_dir_all(&opt.database)?;

    match &opt.csv_file {
        Some(path) => {
            let num_threads = rayon::current_num_threads();

            let result: Result<Vec<_>, anyhow::Error> =
                (0..num_threads).into_par_iter().map(|i| {
                    let (dir, env, index) = if i == 0 {
                        let (env, index) = open_env_index(&opt.database)?;
                        (None, env, index)
                    } else {
                        let dir = tempfile::tempdir()?;
                        let (env, index) = open_env_index(&dir)?;
                        (Some(dir), env, index)
                    };

                    let mut wtxn = env.write_txn()?;
                    let rdr = csv::Reader::from_path(path)?;
                    index_csv(&mut wtxn, rdr, &index, num_threads, i)?;

                    wtxn.commit()?;

                    Ok((i, dir, env, index))
                })
                .collect();

            let mut parts = result?;
            parts.sort_unstable_by_key(|&(i, ..)| cmp::Reverse(i));

            let (_, _, env, index) = parts.pop().context("missing base database")?;

            // TODO we can merge databases that are ready to be merged
            //      into the final one, without having to wait for all of them.
            // TODO we can reuse an already existing database instead of creating a new one
            //      it would be even better to use the first one as it contains the documents.
            let mut wtxn = env.write_txn()?;
            merge_databases(parts, &mut wtxn, &index)?;

            compute_words_attributes_docids(&mut wtxn, &index)?;
            let count = index.documents.len(&wtxn)?;

            wtxn.commit()?;

            eprintln!("Wrote {} documents into LMDB", count);
        },
        None => todo!("support for stdin CSV while indexing in parallel"),
    };


    Ok(())
}
