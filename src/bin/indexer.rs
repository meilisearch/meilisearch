use std::collections::BTreeSet;
use std::convert::TryFrom;
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
use mega_mini_indexer::{FastMap4, SmallVec32, Index, DocumentId};

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

    fn merge(key: &[u8], values: &[Vec<u8>]) -> Option<Vec<u8>> {
        if key == b"\0words-fst" {
            let fsts: Vec<_> = values.iter().map(|v| fst::Set::new(v).unwrap()).collect();

            // Union of the two FSTs
            let mut op = fst::set::OpBuilder::new();
            fsts.iter().for_each(|fst| op.push(fst.into_stream()));
            let op = op.r#union();

            let mut build = fst::SetBuilder::memory();
            build.extend_stream(op.into_stream()).unwrap();
            Some(build.into_inner().unwrap())
        }
        else if key == b"\0headers" {
            assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
            Some(values[0].to_vec())
        }
        else if key.starts_with(&[1]) || key.starts_with(&[2]) {
            let mut first = RoaringBitmap::deserialize_from(values[0].as_slice()).unwrap();

            for value in &values[1..] {
                let bitmap = RoaringBitmap::deserialize_from(value.as_slice()).unwrap();
                first.union_with(&bitmap);
            }

            let mut vec = Vec::new();
            first.serialize_into(&mut vec).unwrap();
            Some(vec)
        }
        else if key.starts_with(&[3]) {
            assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
            Some(values[0].to_vec())
        }
        else {
            panic!("wut? {:?}", key)
        }
    }

    fn from_many<F>(stores: Vec<MtblKvStore>, mut f: F) -> anyhow::Result<()>
    where F: FnMut(&[u8], &[u8]) -> anyhow::Result<()>
    {
        eprintln!("{:?}: Merging {} MTBL stores...", rayon::current_thread_index(), stores.len());

        let mmaps: Vec<_> = stores.iter().flat_map(|m| {
            m.0.as_ref().map(|f| unsafe { memmap::Mmap::map(f).unwrap() })
        }).collect();

        let sources = mmaps.iter().map(|mmap| {
            Reader::new(&mmap, ReaderOptions::default()).unwrap()
        }).collect();

        let opt = MergerOptions { merge: MtblKvStore::merge };
        let mut merger = Merger::new(sources, opt);

        let mut iter = merger.iter();
        while let Some((k, v)) = iter.next() {
            (f)(k, v)?;
        }

        eprintln!("{:?}: MTBL stores merged!", rayon::current_thread_index());
        Ok(())
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
fn writer(wtxn: &mut heed::RwTxn, index: &Index, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
    if key == b"\0words-fst" {
        // Write the words fst
        index.main.put::<_, Str, ByteSlice>(wtxn, "words-fst", val)?;
    }
    else if key == b"\0headers" {
        // Write the headers
        index.main.put::<_, Str, ByteSlice>(wtxn, "headers", val)?;
    }
    else if key.starts_with(&[1]) {
        // Write the postings lists
        index.postings_ids.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[2]) {
        // Write the prefix postings lists
        index.prefix_postings_ids.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[3]) {
        // Write the documents
        index.documents.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }

    Ok(())
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

    let mut stores: Vec<_> = opt.files_to_index
        .into_par_iter()
        .map(|path| {
            let rdr = csv::Reader::from_path(path)?;
            index_csv(rdr)
        })
        .inspect(|_| {
            eprintln!("Total number of documents seen so far is {}", ID_GENERATOR.load(Ordering::Relaxed))
        })
        .collect::<Result<_, _>>()?;

    while stores.len() > 3 {
        let chunk_size = (stores.len() / rayon::current_num_threads()).max(2);
        let s = std::mem::take(&mut stores);
        stores = s.into_par_iter().chunks(chunk_size)
            .map(|v| {
                let outfile = tempfile::tempfile()?;
                let mut out = Writer::new(outfile, None)?;
                MtblKvStore::from_many(v, |k, v| Ok(out.add(k, v).unwrap()))?;
                let out = out.into_inner()?;
                Ok(MtblKvStore(Some(out))) as anyhow::Result<_>
            })
            .collect::<Result<_, _>>()?;
    }

    eprintln!("We are writing into LMDB...");
    let mut wtxn = env.write_txn()?;
    MtblKvStore::from_many(stores, |k, v| writer(&mut wtxn, &index, k, v))?;
    let count = index.documents.len(&wtxn)?;
    wtxn.commit()?;
    eprintln!("Wrote {} documents into LMDB", count);

    Ok(())
}
