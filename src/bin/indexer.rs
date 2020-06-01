use std::collections::BTreeSet;
use std::convert::{TryInto, TryFrom};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Context;
use cow_utils::CowUtils;
use fst::{Streamer, IntoStreamer};
use heed::EnvOpenOptions;
use heed::types::*;
use oxidized_mtbl::{Reader, ReaderOptions, Writer, Merger, MergerOptions};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use structopt::StructOpt;

use mega_mini_indexer::alphanumeric_tokens;
use mega_mini_indexer::{FastMap4, SmallVec32, BEU32, Index, DocumentId};

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

struct Indexed {
    fst: fst::Set<Vec<u8>>,
    postings_ids: FastMap4<SmallVec32, RoaringBitmap>,
    prefix_postings_ids: FastMap4<SmallVec32, RoaringBitmap>,
    headers: Vec<u8>,
    documents: Vec<(DocumentId, Vec<u8>)>,
}

#[derive(Default)]
struct MtblKvStore(Option<File>);

impl MtblKvStore {
    fn from_indexed(mut indexed: Indexed) -> anyhow::Result<MtblKvStore> {
        eprintln!("{:?}: Creating an MTBL store from an Indexed...", rayon::current_thread_index());

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

        // We must write the prefix postings ids
        key[0] = 2;
        let mut stream = indexed.fst.stream();
        while let Some(prefix) = stream.next() {
            key.truncate(1);
            key.extend_from_slice(prefix);
            if let Some(ids) = indexed.prefix_postings_ids.remove(prefix) {
                buffer.clear();
                ids.serialize_into(&mut buffer)?;
                out.add(&key, &buffer).unwrap();
            }
        }

        // postings ids keys are all prefixed by a '2'
        key[0] = 3;
        indexed.documents.sort_unstable();
        for (id, content) in indexed.documents {
            key.truncate(1);
            key.extend_from_slice(&id.to_be_bytes());
            out.add(&key, content).unwrap();
        }

        let out = out.into_inner()?;

        eprintln!("{:?}: MTBL store created!", rayon::current_thread_index());
        Ok(MtblKvStore(Some(out)))
    }

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
        else if key.starts_with(&[1]) || key.starts_with(&[2]) {
            let mut left = RoaringBitmap::deserialize_from(left).unwrap();
            let right = RoaringBitmap::deserialize_from(right).unwrap();
            left.union_with(&right);
            let mut vec = Vec::new();
            left.serialize_into(&mut vec).unwrap();
            Some(vec)
        }
        else if key.starts_with(&[3]) {
            assert_eq!(left, right);
            Some(left.to_vec())
        }
        else {
            panic!("wut? {:?}", key)
        }
    }

    fn from_many(stores: Vec<MtblKvStore>) -> anyhow::Result<MtblKvStore> {
        eprintln!("{:?}: Merging {} MTBL stores...", rayon::current_thread_index(), stores.len());

        let mmaps: Vec<_> = stores.iter().flat_map(|m| {
            m.0.as_ref().map(|f| unsafe { memmap::Mmap::map(f).unwrap() })
        }).collect();

        let sources = mmaps.iter().map(|mmap| {
            Reader::new(&mmap, ReaderOptions::default()).unwrap()
        }).collect();

        let outfile = tempfile::tempfile()?;
        let mut out = Writer::new(outfile, None)?;

        let opt = MergerOptions { merge: MtblKvStore::merge };
        let mut merger = Merger::new(sources, opt);

        let mut iter = merger.iter();
        while let Some((k, v)) = iter.next() {
            out.add(k, v).unwrap();
        }

        let out = out.into_inner()?;

        eprintln!("{:?}: MTBL stores merged!", rayon::current_thread_index());
        Ok(MtblKvStore(Some(out)))
    }
}

fn index_csv(mut rdr: csv::Reader<File>) -> anyhow::Result<MtblKvStore> {
    eprintln!("{:?}: Indexing into an Indexed...", rayon::current_thread_index());

    const MAX_POSITION: usize = 1000;
    const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

    let mut document = csv::StringRecord::new();
    let mut postings_ids = FastMap4::default();
    let mut prefix_postings_ids = FastMap4::default();
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
                    let word = word.cow_to_lowercase();
                    postings_ids.entry(SmallVec32::from(word.as_bytes()))
                        .or_insert_with(RoaringBitmap::new)
                        .insert(document_id);
                    if let Some(prefix) = word.as_bytes().get(0..word.len().min(5)) {
                        for i in 0..=prefix.len() {
                            prefix_postings_ids.entry(SmallVec32::from(&prefix[..i]))
                                .or_insert_with(RoaringBitmap::new)
                                .insert(document_id);
                        }
                    }
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        documents.push((document_id, document));
    }

    // We store the words from the postings.
    let mut new_words = BTreeSet::default();
    for (word, _new_ids) in &postings_ids {
        new_words.insert(word.clone());
    }

    let new_words_fst = fst::Set::from_iter(new_words.iter().map(SmallVec32::as_ref))?;

    let indexed = Indexed { fst: new_words_fst, headers, postings_ids, prefix_postings_ids, documents };
    eprintln!("{:?}: Indexed created!", rayon::current_thread_index());

    MtblKvStore::from_indexed(indexed)
}

// TODO merge with the previous values
fn writer(wtxn: &mut heed::RwTxn, index: Index, mtbl_store: MtblKvStore) -> anyhow::Result<usize> {
    let mtbl_store = match mtbl_store.0 {
        Some(store) => unsafe { memmap::Mmap::map(&store)? },
        None => return Ok(0),
    };
    let mtbl_store = Reader::new(&mtbl_store, ReaderOptions::default()).unwrap();

    // Write the words fst
    let fst = mtbl_store.get(b"\0words-fst").unwrap();
    let fst = fst::Set::new(fst)?;
    index.main.put::<_, Str, ByteSlice>(wtxn, "words-fst", &fst.as_fst().as_bytes())?;

    // Write and merge the headers
    let headers = mtbl_store.get(b"\0headers").unwrap();
    index.main.put::<_, Str, ByteSlice>(wtxn, "headers", headers.as_ref())?;

    // Write and merge the postings lists
    let mut iter = mtbl_store.iter_prefix(&[1]).unwrap();
    while let Some((word, postings)) = iter.next() {
        let word = std::str::from_utf8(&word[1..]).unwrap();
        index.postings_ids.put(wtxn, &word, &postings)?;
    }

    // Write and merge the prefix postings lists
    let mut iter = mtbl_store.iter_prefix(&[2]).unwrap();
    while let Some((word, postings)) = iter.next() {
        let word = std::str::from_utf8(&word[1..]).unwrap();
        index.prefix_postings_ids.put(wtxn, &word, &postings)?;
    }

    // Write the documents
    let mut count = 0;
    let mut iter = mtbl_store.iter_prefix(&[3]).unwrap();
    while let Some((id_bytes, content)) = iter.next() {
        let id = id_bytes[1..].try_into().map(u32::from_be_bytes).unwrap();
        index.documents.put(wtxn, &BEU32::new(id), &content)?;
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

    let index = Index::new(&env)?;

    let stores: Vec<_> = opt.files_to_index
        .into_par_iter()
        .map(|path| {
            let rdr = csv::Reader::from_path(path)?;
            index_csv(rdr)
        })
        .inspect(|_| {
            eprintln!("Total number of documents seen so far is {}", ID_GENERATOR.load(Ordering::Relaxed))
        })
        .collect::<Result<_, _>>()?;

    let mtbl_store = MtblKvStore::from_many(stores)?;

    eprintln!("We are writing into LMDB...");
    let mut wtxn = env.write_txn()?;
    let count = writer(&mut wtxn, index, mtbl_store)?;
    wtxn.commit()?;
    eprintln!("Wrote {} documents into LMDB", count);

    Ok(())
}
