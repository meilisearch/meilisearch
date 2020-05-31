use std::collections::{HashMap, BTreeSet};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use fst::{Streamer, IntoStreamer};
use fxhash::FxHasher32;
use heed::types::*;
use heed::{EnvOpenOptions, PolyDatabase, Database};
use oxidized_mtbl::{Reader, ReaderOptions, Writer, Merger, MergerOptions};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32 = smallvec::SmallVec<[u8; 32]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0); // AtomicU32 ?

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

fn alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

struct Indexed {
    fst: fst::Set<Vec<u8>>,
    postings_ids: FastMap4<SmallVec32, RoaringBitmap>,
    headers: Vec<u8>,
    documents: Vec<(DocumentId, Vec<u8>)>,
}

#[derive(Default)]
struct MtblKvStore(Option<File>);

impl MtblKvStore {
    fn from_indexed(mut indexed: Indexed) -> anyhow::Result<MtblKvStore> {
        let outfile = tempfile::tempfile()?;
        let mut out = Writer::new(outfile, None)?;

        out.add(b"\0headers", indexed.headers)?;
        out.add(b"\0words-fst", indexed.fst.as_fst().as_bytes())?;

        // postings ids keys are all prefixed by a '1'
        let mut key = vec![1];
        let mut buffer = Vec::new();
        // We must write the postings ids in order for mtbl therefore
        // we iterate over the fst to read the words in order
        let mut stream = indexed.fst.stream();
        while let Some(word) = stream.next() {
            key.truncate(1);
            key.extend_from_slice(word);
            if let Some(ids) = indexed.postings_ids.remove(word) {
                buffer.clear();
                ids.serialize_into(&mut buffer)?;
                out.add(&key, &buffer).unwrap();
            }
        }

        // postings ids keys are all prefixed by a '2'
        key[0] = 2;
        indexed.documents.sort_unstable();
        for (id, content) in indexed.documents {
            key.truncate(1);
            key.extend_from_slice(&id.to_be_bytes());
            out.add(&key, content).unwrap();
        }

        let out = out.into_inner()?;
        Ok(MtblKvStore(Some(out)))
    }

    fn merge_with(self, other: MtblKvStore) -> anyhow::Result<MtblKvStore> {
        let (left, right) = match (self.0, other.0) {
            (Some(left), Some(right)) => (left, right),
            (Some(left), None) => return Ok(MtblKvStore(Some(left))),
            (None, Some(right)) => return Ok(MtblKvStore(Some(right))),
            (None, None) => return Ok(MtblKvStore(None)),
        };

        let left = unsafe { memmap::Mmap::map(&left)? };
        let right = unsafe { memmap::Mmap::map(&right)? };

        let left = Reader::new(&left, ReaderOptions::default()).unwrap();
        let right = Reader::new(&right, ReaderOptions::default()).unwrap();

        fn merge(key: &[u8], left: &[u8], right: &[u8]) -> Option<Vec<u8>> {
            if key == b"\0words-fst" {
                let left_fst = fst::Set::new(left).unwrap();
                let right_fst = fst::Set::new(right).unwrap();

                // Union of the two FSTs
                let op = fst::set::OpBuilder::new()
                    .add(left_fst.into_stream())
                    .add(right_fst.into_stream())
                    .r#union();

                let mut build = fst::SetBuilder::memory();
                build.extend_stream(op.into_stream()).unwrap();
                Some(build.into_inner().unwrap())
            }
            else if key == b"\0headers" {
                assert_eq!(left, right);
                Some(left.to_vec())
            }
            else if key.starts_with(&[1]) {
                let mut left = RoaringBitmap::deserialize_from(left).unwrap();
                let right = RoaringBitmap::deserialize_from(right).unwrap();
                left.union_with(&right);
                let mut vec = Vec::new();
                left.serialize_into(&mut vec).unwrap();
                Some(vec)
            }
            else if key.starts_with(&[2]) {
                assert_eq!(left, right);
                Some(left.to_vec())
            }
            else {
                panic!("wut? {:?}", key)
            }
        }

        let outfile = tempfile::tempfile()?;
        let mut out = Writer::new(outfile, None)?;

        let sources = vec![left, right];
        let opt = MergerOptions { merge };
        let mut merger = Merger::new(sources, opt);

        let mut iter = merger.iter();
        while let Some((k, v)) = iter.next() {
            out.add(k, v).unwrap();
        }

        let out = out.into_inner()?;
        Ok(MtblKvStore(Some(out)))
    }
}

fn index_csv(mut rdr: csv::Reader<File>) -> anyhow::Result<MtblKvStore> {
    const MAX_POSITION: usize = 1000;
    const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

    let mut document = csv::StringRecord::new();
    let mut postings_ids = FastMap4::default();
    let mut documents = Vec::new();

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    while rdr.read_record(&mut document)? {
        let document_id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        let document_id = DocumentId::try_from(document_id).context("Generated id is too big")?;

        for (_attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (_pos, word) in alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < 500 { // LMDB limits
                    postings_ids.entry(SmallVec32::from(word.as_bytes()))
                        .or_insert_with(RoaringBitmap::new)
                        .insert(document_id);
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        documents.push((document_id, document));
    }

    // We compute and store the postings list into the DB.
    let mut new_words = BTreeSet::default();
    for (word, _new_ids) in &postings_ids {
        new_words.insert(word.clone());
    }

    let new_words_fst = fst::Set::from_iter(new_words.iter().map(SmallVec32::as_ref))?;

    let indexed = Indexed { fst: new_words_fst, headers, postings_ids, documents };

    MtblKvStore::from_indexed(indexed)
}

// TODO merge with the previous values
fn writer(
    wtxn: &mut heed::RwTxn,
    main: PolyDatabase,
    postings_ids: Database<Str, ByteSlice>,
    documents: Database<OwnedType<BEU32>, ByteSlice>,
    mtbl_store: MtblKvStore,
) -> anyhow::Result<usize>
{
    let mtbl_store = match mtbl_store.0 {
        Some(store) => unsafe { memmap::Mmap::map(&store)? },
        None => return Ok(0),
    };
    let mtbl_store = Reader::new(&mtbl_store, ReaderOptions::default()).unwrap();

    // Write the words fst
    let fst = mtbl_store.get(b"\0words-fst").unwrap();
    let fst = fst::Set::new(fst)?;
    main.put::<_, Str, ByteSlice>(wtxn, "words-fst", &fst.as_fst().as_bytes())?;

    // Write and merge the headers
    let headers = mtbl_store.get(b"\0headers").unwrap();
    main.put::<_, Str, ByteSlice>(wtxn, "headers", headers.as_ref())?;

    // Write and merge the postings lists
    let mut iter = mtbl_store.iter_prefix(&[1]).unwrap();
    while let Some((word, postings)) = iter.next() {
        let word = std::str::from_utf8(&word[1..]).unwrap();
        postings_ids.put(wtxn, &word, &postings)?;
    }

    // Write the documents
    let mut count = 0;
    let mut iter = mtbl_store.iter_prefix(&[2]).unwrap();
    while let Some((id_bytes, content)) = iter.next() {
        let id = id_bytes[1..].try_into().map(u32::from_be_bytes).unwrap();
        documents.put(wtxn, &BEU32::new(id), &content)?;
        count += 1;
    }

    Ok(count)
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

    let res = opt.files_to_index
        .into_par_iter()
        .try_fold(MtblKvStore::default, |acc, path| {
            let rdr = csv::Reader::from_path(path)?;
            let mtbl_store = index_csv(rdr)?;
            acc.merge_with(mtbl_store)
        })
        .inspect(|_| {
            eprintln!("Total number of documents seen so far is {}", ID_GENERATOR.load(Ordering::Relaxed))
        })
        .try_reduce(MtblKvStore::default, MtblKvStore::merge_with);

    let mtbl_store = res?;

    eprintln!("We are writing into LMDB...");
    let mut wtxn = env.write_txn()?;
    let count = writer(&mut wtxn, main, postings_ids, documents, mtbl_store)?;
    wtxn.commit()?;
    eprintln!("Wrote {} documents into LMDB", count);

    Ok(())
}
