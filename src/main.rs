#[cfg(test)]
#[macro_use] extern crate quickcheck;

mod codec;
mod bp_vec;

use std::collections::{HashMap, BTreeSet};
use std::convert::TryFrom;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::path::PathBuf;

use anyhow::{ensure, Context};
use fst::IntoStreamer;
use fxhash::FxHasher32;
use rayon::prelude::*;
use sdset::{SetOperation, SetBuf};
use slice_group_by::StrGroupBy;
use structopt::StructOpt;

use self::codec::CodecBitPacker4xSorted;
use self::bp_vec::BpVec;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "mm-indexer", about = "The server side of the daugt project.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// Files to index in parallel.
    files_to_index: Vec<PathBuf>,
}

fn union_bitpacked_postings_ids(_key: &[u8], old_value: Option<&[u8]>, new_value: &[u8]) -> Option<Vec<u8>> {
    if old_value.is_none() {
        return Some(new_value.to_vec())
    }

    let old_value = old_value.unwrap_or_default();
    let old_value = CodecBitPacker4xSorted::bytes_decode(&old_value).unwrap();
    let new_value = CodecBitPacker4xSorted::bytes_decode(&new_value).unwrap();

    let old_set = SetBuf::new(old_value).unwrap();
    let new_set = SetBuf::new(new_value).unwrap();

    let result = sdset::duo::Union::new(&old_set, &new_set).into_set_buf();
    let compressed = CodecBitPacker4xSorted::bytes_encode(&result).unwrap();

    Some(compressed)
}

fn union_words_fst(key: &[u8], old_value: Option<&[u8]>, new_value: &[u8]) -> Option<Vec<u8>> {
    if key != b"words-fst" { unimplemented!() }

    let old_value = match old_value {
        Some(old_value) => old_value,
        None => return Some(new_value.to_vec()),
    };

    eprintln!("old_words size: {}", old_value.len());
    eprintln!("new_words size: {}", new_value.len());

    let old_words = fst::Set::new(old_value).unwrap();
    let new_words = fst::Set::new(new_value).unwrap();

    // Do an union of the old and the new set of words.
    let op = old_words.op().add(new_words.into_stream()).r#union();
    let mut build = fst::SetBuilder::memory();
    build.extend_stream(op.into_stream()).unwrap();

    Some(build.into_inner().unwrap())
}

fn alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

fn index_csv(tid: usize, db: sled::Db, mut rdr: csv::Reader<File>) -> anyhow::Result<usize> {
    const MAX_POSITION: usize = 1000;
    const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

    let main = &*db;
    let postings_ids = db.open_tree("postings-ids")?;
    let documents = db.open_tree("documents")?;

    let mut document = csv::StringRecord::new();
    let mut new_postings_ids = FastMap4::default();
    let mut new_words = BTreeSet::default();
    let mut number_of_documents = 0;

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    if let Some(old_headers) = main.insert("headers", headers.as_slice())? {
        ensure!(old_headers == headers, "headers differs from the previous ones");
    }

    while rdr.read_record(&mut document)? {
        let document_id = db.generate_id()?;
        let document_id = u32::try_from(document_id).context("Generated id is too big")?;

        for (_attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (_pos, word) in alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                new_postings_ids.entry(SmallString32::from(word)).or_insert_with(BpVec::new).push(document_id);
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        documents.insert(document_id.to_be_bytes(), document)?;

        number_of_documents += 1;
        if number_of_documents % 100000 == 0 {
            let postings_ids_size = new_postings_ids.iter().map(|(_, v)| v.capacity() * 4).sum::<usize>();
            eprintln!("{}, documents seen {}, postings size {}",
                tid, number_of_documents, postings_ids_size);
        }
    }

    eprintln!("Start collecting the postings lists and words");

    // We compute and store the postings list into the DB.
    for (word, new_ids) in new_postings_ids {
        let new_ids = SetBuf::from_dirty(new_ids.to_vec());
        let compressed = CodecBitPacker4xSorted::bytes_encode(&new_ids)
            .context("error while compressing using CodecBitPacker4xSorted")?;

        postings_ids.merge(word.as_bytes(), compressed)?;

        new_words.insert(word);
    }

    eprintln!("Finished collecting the postings lists and words");

    eprintln!("Start merging the words-fst");

    let new_words_fst = fst::Set::from_iter(new_words.iter().map(|s| s.as_str()))?;
    drop(new_words);
    main.merge("words-fst", new_words_fst.as_fst().as_bytes())?;

    eprintln!("Finished merging the words-fst");

    Ok(number_of_documents)
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let db = sled::open(opt.database)?;
    let main = &*db;

    // Setup the merge operators
    main.set_merge_operator(union_words_fst);
    let postings_ids = db.open_tree("postings-ids")?;
    postings_ids.set_merge_operator(union_bitpacked_postings_ids);
    // ...
    let _documents = db.open_tree("documents")?;

    let res = opt.files_to_index
        .into_par_iter()
        .enumerate()
        .map(|(tid, path)| {
            let rdr = csv::Reader::from_path(path)?;
            index_csv(tid, db.clone(), rdr)
        })
        .try_reduce(|| 0, |a, b| Ok(a + b));

    println!("{:?}", res);

    Ok(())
}
