#[cfg(test)]
#[macro_use] extern crate quickcheck;

mod codec;
mod bp_vec;

use std::collections::{HashMap, BTreeSet};
use std::convert::TryFrom;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{ensure, Context};
use fst::IntoStreamer;
use fxhash::FxHasher32;
use rayon::prelude::*;
use sdset::{SetOperation, SetBuf};
use slice_group_by::StrGroupBy;
use structopt::StructOpt;
use zerocopy::{LayoutVerified, AsBytes};

// use self::codec::CodecBitPacker4xSorted;
use self::bp_vec::BpVec;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

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

fn bytes_to_u32s(bytes: &[u8]) -> Vec<u32> {
    fn aligned_to(bytes: &[u8], align: usize) -> bool {
        (bytes as *const _ as *const () as usize) % align == 0
    }

    match LayoutVerified::new_slice(bytes) {
        Some(slice) => slice.to_vec(),
        None => {
            let len = bytes.len();

            // ensure that it is the alignment that is wrong and the length is valid
            assert!(len % 4 == 0, "length is {} and is not modulo 4", len);
            assert!(!aligned_to(bytes, std::mem::align_of::<u32>()), "bytes are already aligned");

            let elems = len / 4;
            let mut vec = Vec::<u32>::with_capacity(elems);

            unsafe {
                let dst = vec.as_mut_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                vec.set_len(elems);
            }

            vec
        },
    }
}

fn union_postings_ids(
    _key: &[u8],
    old_value: Option<&[u8]>,
    operands: &mut rocksdb::MergeOperands,
) -> Option<Vec<u8>>
{
    let mut sets_bufs = Vec::new();

    if let Some(old_value) = old_value {
        let old_value = bytes_to_u32s(old_value);
        sets_bufs.push(SetBuf::new_unchecked(old_value.to_vec()));
    }

    for operand in operands {
        let new_value = bytes_to_u32s(operand);
        sets_bufs.push(SetBuf::new_unchecked(new_value.to_vec()));
    }

    let sets = sets_bufs.iter().map(|s| s.as_set()).collect();
    let result: SetBuf<u32> = sdset::multi::Union::new(sets).into_set_buf();

    assert!(result.as_bytes().len() % 4 == 0);

    Some(result.as_bytes().to_vec())
}

fn union_words_fst(
    key: &[u8],
    old_value: Option<&[u8]>,
    operands: &mut rocksdb::MergeOperands,
) -> Option<Vec<u8>>
{
    if key != b"words-fst" { unimplemented!() }

    let mut fst_operands = Vec::new();
    for operand in operands {
        fst_operands.push(fst::Set::new(operand).unwrap());
    }

    // Do an union of the old and the new set of words.
    let mut builder = fst::set::OpBuilder::new();

    let old_words = old_value.map(|v| fst::Set::new(v).unwrap());
    let old_words = old_words.as_ref().map(|v| v.into_stream());
    if let Some(old_words) = old_words {
        builder.push(old_words);
    }

    for new_words in &fst_operands {
        builder.push(new_words.into_stream());
    }

    let op = builder.r#union();
    let mut build = fst::SetBuilder::memory();
    build.extend_stream(op.into_stream()).unwrap();

    Some(build.into_inner().unwrap())
}

fn alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

fn index_csv(
    tid: usize,
    db: Arc<rocksdb::DB>,
    mut rdr: csv::Reader<File>,
) -> anyhow::Result<usize>
{
    const MAX_POSITION: usize = 1000;
    const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

    let main = db.cf_handle("main").context("cf \"main\" not found")?;
    let postings_ids = db.cf_handle("postings-ids").context("cf \"postings-ids\" not found")?;
    let documents = db.cf_handle("documents").context("cf \"documents\" not found")?;

    let mut document = csv::StringRecord::new();
    let mut new_postings_ids = FastMap4::default();
    let mut new_words = BTreeSet::default();
    let mut number_of_documents = 0;

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    if let Some(old_headers) = db.get_cf(&main, "headers")? {
        ensure!(old_headers == headers, "headers differs from the previous ones");
    }
    db.put_cf(&main, "headers", headers.as_slice())?;

    while rdr.read_record(&mut document)? {
        let document_id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
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
        db.put_cf(&documents, document_id.to_be_bytes(), document)?;

        number_of_documents += 1;
        if number_of_documents % 100000 == 0 {
            let postings_ids_size = new_postings_ids.iter().map(|(_, v)| {
                v.compressed_capacity() + v.uncompressed_capacity() * 4
            }).sum::<usize>();
            eprintln!("{}, documents seen {}, postings size {}",
                tid, number_of_documents, postings_ids_size);
        }
    }

    eprintln!("Start collecting the postings lists and words");

    // We compute and store the postings list into the DB.
    for (word, new_ids) in new_postings_ids {
        let new_ids = SetBuf::from_dirty(new_ids.to_vec());
        db.merge_cf(&postings_ids, word.as_bytes(), new_ids.as_bytes())?;
        new_words.insert(word);
    }

    eprintln!("Finished collecting the postings lists and words");

    eprintln!("Start merging the words-fst");

    let new_words_fst = fst::Set::from_iter(new_words.iter().map(|s| s.as_str()))?;
    drop(new_words);
    db.merge_cf(&main, "words-fst", new_words_fst.as_fst().as_bytes())?;

    eprintln!("Finished merging the words-fst");
    eprintln!("Total number of documents seen is {}", ID_GENERATOR.load(Ordering::Relaxed));

    Ok(number_of_documents)
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    // Setup the merge operators
    opts.set_merge_operator("main", union_words_fst, None); // Some(union_words_fst));
    opts.set_merge_operator("postings-ids", union_postings_ids, None); // Some(union_postings_ids));

    let mut db = rocksdb::DB::open(&opts, &opt.database)?;

    let cfs = &["main", "postings-ids", "documents"];
    for cf in cfs.into_iter() {
        db.create_cf(cf, &opts).unwrap();
    }

    let db = Arc::new(db);
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
