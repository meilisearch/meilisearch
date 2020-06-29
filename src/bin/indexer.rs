use std::collections::hash_map::Entry;
use std::collections::{HashMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::io;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use cow_utils::CowUtils;
use fst::Streamer;
use heed::EnvOpenOptions;
use heed::types::*;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;

use mega_mini_indexer::{BEU32, Index, DocumentId};

const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0); // AtomicU32 ?

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

fn index_csv<R: io::Read>(wtxn: &mut heed::RwTxn, mut rdr: csv::Reader<R>, index: &Index) -> anyhow::Result<()> {
    eprintln!("Indexing into LMDB...");

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    let mut document = csv::StringRecord::new();

    while rdr.read_record(&mut document)? {
        let document_id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        let document_id = DocumentId::try_from(document_id).context("Generated id is too big")?;

        for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (pos, word) in simple_alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < 500 { // LMDB limits
                    let word = word.cow_to_lowercase();
                    let position = (attr * 1000 + pos) as u32;

                    // ------ merge word positions --------

                    let ids = match index.word_positions.get(wtxn, &word)? {
                        Some(mut ids) => { ids.insert(position); ids },
                        None => RoaringBitmap::from_iter(Some(position)),
                    };

                    index.word_positions.put(wtxn, &word, &ids)?;

                    // ------ merge word position documents ids --------

                    let mut key = word.as_bytes().to_vec();
                    key.extend_from_slice(&position.to_be_bytes());

                    let ids = match index.word_position_docids.get(wtxn, &key)? {
                        Some(mut ids) => { ids.insert(document_id); ids },
                        None => RoaringBitmap::from_iter(Some(document_id)),
                    };

                    index.word_position_docids.put(wtxn, &key, &ids)?;
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        index.documents.put(wtxn, &BEU32::new(document_id), &document)?;
    }

    // We store the words from the postings.
    let mut new_words = BTreeSet::default();
    let iter = index.word_positions.as_polymorph().iter::<_, Str, DecodeIgnore>(wtxn)?;
    for result in iter {
        let (word, ()) = result?;
        new_words.insert(word.clone());
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

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(10)
        .open(opt.database)?;

    let index = Index::new(&env)?;

    let mut wtxn = env.write_txn()?;

    match opt.csv_file {
        Some(path) => {
            let rdr = csv::Reader::from_path(path)?;
            index_csv(&mut wtxn, rdr, &index)?;
        },
        None => {
            let rdr = csv::Reader::from_reader(io::stdin());
            index_csv(&mut wtxn, rdr, &index)?;
        }
    };

    compute_words_attributes_docids(&mut wtxn, &index)?;
    let count = index.documents.len(&wtxn)?;

    wtxn.commit()?;

    eprintln!("Wrote {} documents into LMDB", count);

    Ok(())
}
