use std::collections::hash_map::Entry;
use std::collections::{HashMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::mem;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use cow_utils::CowUtils;
use fst::{Streamer, IntoStreamer};
use heed::EnvOpenOptions;
use heed::types::*;
use log::debug;
use oxidized_mtbl::{Reader, ReaderOptions, Writer, Merger, MergerOptions};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;

use milli::{FastMap4, SmallVec32, Index, DocumentId, Position};

const LMDB_MAX_KEY_LENGTH: usize = 512;
const ONE_MILLION: usize = 1_000_000;

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

    /// Number of parallel jobs, defaults to # of CPUs.
    #[structopt(short, long)]
    jobs: Option<usize>,

    /// Maximum number of bytes to allocate, will be divided by the number of
    /// cores used. It is recommended to set a maximum of half of the available memory
    /// as the current measurement method is really bad.
    ///
    /// The minumum amount of memory used will be 50MB anyway.
    #[structopt(long, default_value = "4294967296")]
    max_memory_usage: usize,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// CSV file to index, if unspecified the CSV is read from standard input.
    csv_file: Option<PathBuf>,
}

struct Indexed {
    fst: fst::Set<Vec<u8>>,
    word_positions: FastMap4<SmallVec32<u8>, RoaringBitmap>,
    word_position_docids: FastMap4<(SmallVec32<u8>, Position), RoaringBitmap>,
    headers: Vec<u8>,
    documents: Vec<(DocumentId, Vec<u8>)>,
}

impl Indexed {
    fn new(
        word_positions: FastMap4<SmallVec32<u8>, RoaringBitmap>,
        word_position_docids: FastMap4<(SmallVec32<u8>, Position), RoaringBitmap>,
        headers: Vec<u8>,
        documents: Vec<(DocumentId, Vec<u8>)>,
    ) -> anyhow::Result<Indexed>
    {
        // We store the words from the postings.
        let new_words: BTreeSet<_> = word_position_docids.iter().map(|((w, _), _)| w).collect();
        let fst = fst::Set::from_iter(new_words)?;
        Ok(Indexed { fst, headers, word_positions, word_position_docids, documents })
    }
}

#[derive(Default)]
struct MtblKvStore(Option<File>);

impl MtblKvStore {
    fn from_indexed(mut indexed: Indexed) -> anyhow::Result<MtblKvStore> {
        debug!("Creating an MTBL store from an Indexed...");

        let outfile = tempfile::tempfile()?;
        let mut out = Writer::new(outfile, None)?;

        out.add(b"\0headers", indexed.headers)?;
        out.add(b"\0words-fst", indexed.fst.as_fst().as_bytes())?;

        // postings ids keys are all prefixed by a '1'
        let mut key = vec![0];
        let mut buffer = Vec::new();

        // We must write the postings attrs
        key[0] = 1;
        // We must write the postings ids in order for mtbl therefore
        // we iterate over the fst to read the words in order
        let mut stream = indexed.fst.stream();
        while let Some(word) = stream.next() {
            if let Some(positions) = indexed.word_positions.get(word) {
                key.truncate(1);
                key.extend_from_slice(word);
                // We serialize the positions into a buffer
                buffer.clear();
                positions.serialize_into(&mut buffer)?;
                // that we write under the generated key into MTBL
                out.add(&key, &buffer).unwrap();
            }
        }

        // We must write the postings ids
        key[0] = 3;
        // We must write the postings ids in order for mtbl therefore
        // we iterate over the fst to read the words in order
        let mut stream = indexed.fst.stream();
        while let Some(word) = stream.next() {
            key.truncate(1);
            key.extend_from_slice(word);
            if let Some(positions) = indexed.word_positions.remove(word) {
                // We iterate over all the attributes containing the documents ids
                for pos in positions {
                    let ids = indexed.word_position_docids.remove(&(SmallVec32::from(word), pos)).unwrap();
                    // we postfix the word by the positions it appears in
                    let position_bytes = pos.to_be_bytes();
                    key.extend_from_slice(&position_bytes);
                    // We serialize the document ids into a buffer
                    buffer.clear();
                    ids.serialize_into(&mut buffer)?;
                    // that we write under the generated key into MTBL
                    out.add(&key, &buffer).unwrap();
                    // And cleanup the position afterward
                    key.truncate(key.len() - position_bytes.len());
                }
            }
        }

        // postings ids keys are all prefixed
        key[0] = 5;
        indexed.documents.sort_unstable_by_key(|(id, _)| *id);
        for (id, content) in indexed.documents {
            key.truncate(1);
            key.extend_from_slice(&id.to_be_bytes());
            out.add(&key, content).unwrap();
        }

        let out = out.into_inner()?;

        debug!("MTBL store created!");
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
        // We either merge postings attrs, prefix postings or postings ids.
        else if key[0] == 1 || key[0] == 2 || key[0] == 3 || key[0] == 4 {
            let mut first = RoaringBitmap::deserialize_from(values[0].as_slice()).unwrap();

            for value in &values[1..] {
                let bitmap = RoaringBitmap::deserialize_from(value.as_slice()).unwrap();
                first.union_with(&bitmap);
            }

            let mut vec = Vec::new();
            first.serialize_into(&mut vec).unwrap();
            Some(vec)
        }
        else if key[0] == 5 {
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
        debug!("Merging {} MTBL stores...", stores.len());
        let before = Instant::now();

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

        debug!("MTBL stores merged in {:.02?}!", before.elapsed());
        Ok(())
    }
}

fn mem_usage(
    word_positions: &FastMap4<SmallVec32<u8>, RoaringBitmap>,
    word_position_docids: &FastMap4<(SmallVec32<u8>, Position), RoaringBitmap>,
    documents: &Vec<(u32, Vec<u8>)>,
) -> usize
{
    use std::mem::size_of;

    let documents =
          documents.iter().map(|(_, d)| d.capacity()).sum::<usize>()
        + documents.capacity() * size_of::<(Position, Vec<u8>)>();

    let word_positions =
          word_positions.iter().map(|(k, r)| {
            (if k.spilled() { k.capacity() } else { 0 }) + r.mem_usage()
          }).sum::<usize>()
        + word_positions.capacity() * size_of::<(SmallVec32<u8>, RoaringBitmap)>();

    let word_position_docids =
          word_position_docids.iter().map(|((k, _), r)| {
            (if k.spilled() { k.capacity() } else { 0 }) + r.mem_usage()
          }).sum::<usize>()
        + word_position_docids.capacity() * size_of::<((SmallVec32<u8>, Position), RoaringBitmap)>();

    documents + word_positions + word_position_docids
}

fn index_csv(
    mut rdr: csv::Reader<File>,
    thread_index: usize,
    num_threads: usize,
    max_mem_usage: usize,
) -> anyhow::Result<Vec<MtblKvStore>>
{
    debug!("{:?}: Indexing into an Indexed...", thread_index);

    let mut stores = Vec::new();

    let mut word_positions = FastMap4::default();
    let mut word_position_docids = FastMap4::default();
    let mut documents = Vec::new();

    // Write the headers into a Vec of bytes.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;

    let mut document_id: usize = 0;
    let mut document = csv::StringRecord::new();
    while rdr.read_record(&mut document)? {
        document_id = document_id + 1;

        // We skip documents that must not be indexed by this thread
        if document_id % num_threads != thread_index { continue }

        let document_id = DocumentId::try_from(document_id).context("generated id is too big")?;

        if document_id % (ONE_MILLION as u32) == 0 {
            debug!("We have seen {}m documents so far.", document_id / ONE_MILLION as u32);
        }

        for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (pos, word) in simple_alphanumeric_tokens(&content).enumerate().take(MAX_POSITION) {
                if !word.is_empty() && word.len() < LMDB_MAX_KEY_LENGTH {
                    let word = word.cow_to_lowercase();
                    let position = (attr * MAX_POSITION + pos) as u32;

                    // We save the positions where this word has been seen.
                    word_positions.entry(SmallVec32::from(word.as_bytes()))
                        .or_insert_with(RoaringBitmap::new).insert(position);

                    // We save the documents ids under the position and word we have seen it.
                    word_position_docids.entry((SmallVec32::from(word.as_bytes()), position)) // word + position
                        .or_insert_with(RoaringBitmap::new).insert(document_id); // document ids
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        documents.push((document_id, document));

        if documents.len() % 100_000 == 0 {
            let usage = mem_usage(&word_positions, &word_position_docids, &documents);
            if usage > max_mem_usage {
                debug!("Whoops too much memory used ({}B).", usage);

                let word_positions = mem::take(&mut word_positions);
                let word_position_docids = mem::take(&mut word_position_docids);
                let documents = mem::take(&mut documents);

                let indexed = Indexed::new(word_positions, word_position_docids, headers.clone(), documents)?;
                debug!("{:?}: Indexed created!", thread_index);
                stores.push(MtblKvStore::from_indexed(indexed)?);
            }
        }
    }

    let indexed = Indexed::new(word_positions, word_position_docids, headers, documents)?;
    debug!("{:?}: Indexed created!", thread_index);
    stores.push(MtblKvStore::from_indexed(indexed)?);

    Ok(stores)
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
        index.word_positions.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[2]) {
        // Write the prefix postings lists
        index.prefix_word_positions.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[3]) {
        // Write the postings lists
        index.word_position_docids.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[4]) {
        // Write the prefix postings lists
        index.prefix_word_position_docids.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[5]) {
        // Write the documents
        index.documents.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }

    Ok(())
}

fn compute_words_attributes_docids(wtxn: &mut heed::RwTxn, index: &Index) -> anyhow::Result<()> {
    let before = Instant::now();

    debug!("Computing the attributes documents ids...");

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
                let attribute = key_pos / MAX_POSITION as u32;
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

    debug!("Computing the attributes documents ids took {:.02?}.", before.elapsed());

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    stderrlog::new()
        .verbosity(opt.verbose)
        .show_level(false)
        .timestamp(stderrlog::Timestamp::Off)
        .init()?;

    if let Some(jobs) = opt.jobs {
        rayon::ThreadPoolBuilder::new().num_threads(jobs).build_global()?;
    }

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(10)
        .open(opt.database)?;

    let index = Index::new(&env)?;

    let num_threads = rayon::current_num_threads();
    let max_memory_usage = (opt.max_memory_usage / num_threads).max(50 * 1024 * 1024); // 50MB

    // We duplicate the file # jobs times.
    let file = opt.csv_file.unwrap();
    let csv_readers: Vec<_> = (0..num_threads).map(|_| csv::Reader::from_path(&file)).collect::<Result<_, _>>()?;

    let stores: Vec<_> = csv_readers
        .into_par_iter()
        .enumerate()
        .map(|(i, rdr)| index_csv(rdr, i, num_threads, max_memory_usage))
        .collect::<Result<_, _>>()?;

    let stores: Vec<_> = stores.into_iter().flatten().collect();

    debug!("We are writing into LMDB...");
    let mut wtxn = env.write_txn()?;

    MtblKvStore::from_many(stores, |k, v| writer(&mut wtxn, &index, k, v))?;
    compute_words_attributes_docids(&mut wtxn, &index)?;
    let count = index.documents.len(&wtxn)?;

    wtxn.commit()?;
    debug!("Wrote {} documents into LMDB", count);

    Ok(())
}
