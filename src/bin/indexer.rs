use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use arc_cache::ArcCache;
use cow_utils::CowUtils;
use fst::{Streamer, IntoStreamer};
use heed::EnvOpenOptions;
use heed::types::*;
use log::debug;
use memmap::Mmap;
use oxidized_mtbl::{Reader, Writer, Merger, Sorter, CompressionType};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;
use structopt::StructOpt;

use milli::{SmallVec32, Index, DocumentId, Position};

const LMDB_MAX_KEY_LENGTH: usize = 512;
const ONE_MILLION: usize = 1_000_000;

const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

const HEADERS_KEY: &[u8] = b"\0headers";
const WORDS_FST_KEY: &[u8] = b"\x06words-fst";

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn simple_alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

#[derive(Debug, StructOpt)]
#[structopt(name = "milli-indexer", about = "The indexer binary of the milli project.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// Number of parallel jobs, defaults to # of CPUs.
    #[structopt(short, long)]
    jobs: Option<usize>,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// CSV file to index, if unspecified the CSV is read from standard input.
    csv_file: Option<PathBuf>,
}

type MergeFn = fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, ()>;

struct Store {
    word_positions: ArcCache<SmallVec32<u8>, RoaringBitmap>,
    word_position_docids: ArcCache<(SmallVec32<u8>, Position), RoaringBitmap>,
    sorter: Sorter<MergeFn>,
}

impl Store {
    fn new() -> Store {
        let sorter = Sorter::builder(merge as MergeFn)
            .chunk_compression_type(CompressionType::Snappy)
            .build();

        Store {
            word_positions: ArcCache::new(65_535),
            word_position_docids: ArcCache::new(65_535),
            sorter,
        }
    }

    // Save the positions where this word has been seen.
    pub fn insert_word_position(&mut self, word: &str, position: Position) -> anyhow::Result<()> {
        let word = SmallVec32::from(word.as_bytes());
        let position = RoaringBitmap::from_iter(Some(position));
        let (_, lrus) = self.word_positions.insert(word, position, |old, new| old.union_with(&new));
        Self::write_word_positions(&mut self.sorter, lrus)
    }

    // Save the documents ids under the position and word we have seen it.
    pub fn insert_word_position_docid(&mut self, word: &str, position: Position, id: DocumentId) -> anyhow::Result<()> {
        let word = SmallVec32::from(word.as_bytes());
        let ids = RoaringBitmap::from_iter(Some(id));
        let (_, lrus) = self.word_position_docids.insert((word, position), ids, |old, new| old.union_with(&new));
        Self::write_word_position_docids(&mut self.sorter, lrus)
    }

    pub fn write_headers(&mut self, headers: &[u8]) -> anyhow::Result<()> {
        Ok(self.sorter.insert(HEADERS_KEY, headers)?)
    }

    pub fn write_document(&mut self, id: DocumentId, content: &[u8]) -> anyhow::Result<()> {
        let id =  id.to_be_bytes();
        let mut key = Vec::with_capacity(1 + id.len());

        // postings ids keys are all prefixed by a '5'
        key.push(5);
        key.extend_from_slice(&id);

        Ok(self.sorter.insert(&key, content)?)
    }

    fn write_word_positions<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=(SmallVec32<u8>, RoaringBitmap)>
    {
        // postings ids keys are all prefixed by a '1'
        let mut key = vec![1];
        let mut buffer = Vec::new();

        for (word, positions) in iter {
            key.truncate(1);
            key.extend_from_slice(&word);
            // We serialize the positions into a buffer
            buffer.clear();
            positions.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            sorter.insert(&key, &buffer)?;
        }

        Ok(())
    }

    fn write_word_position_docids<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=((SmallVec32<u8>, Position), RoaringBitmap)>
    {
        // postings positions ids keys are all prefixed by a '3'
        let mut key = vec![3];
        let mut buffer = Vec::new();

        for ((word, pos), ids) in iter {
            key.truncate(1);
            key.extend_from_slice(&word);
            // we postfix the word by the positions it appears in
            let position_bytes = pos.to_be_bytes();
            key.extend_from_slice(&position_bytes);
            // We serialize the document ids into a buffer
            buffer.clear();
            ids.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            sorter.insert(&key, &buffer)?;
            // And cleanup the position afterward
            key.truncate(key.len() - position_bytes.len());
        }

        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<Reader<Mmap>> {
        Self::write_word_positions(&mut self.sorter, self.word_positions)?;
        Self::write_word_position_docids(&mut self.sorter, self.word_position_docids)?;

        let mut wtr = tempfile::tempfile().map(Writer::new)?;
        let mut builder = fst::SetBuilder::memory();

        let mut iter = self.sorter.into_iter()?;
        while let Some(result) = iter.next() {
            let (key, val) = result?;
            if let Some((&1, word)) = key.split_first() {
                // This is a lexicographically ordered word position
                // we use the key to construct the words fst.
                builder.insert(word)?;
            }
            wtr.insert(key, val)?;
        }

        let fst = builder.into_set();
        wtr.insert(WORDS_FST_KEY, fst.as_fst().as_bytes())?;

        let file = wtr.into_inner()?;
        let mmap = unsafe { Mmap::map(&file)? };
        let reader = Reader::new(mmap)?;

        Ok(reader)
    }
}

fn merge(key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    if key == WORDS_FST_KEY {
        let fsts: Vec<_> = values.iter().map(|v| fst::Set::new(v).unwrap()).collect();

        // Union of the two FSTs
        let mut op = fst::set::OpBuilder::new();
        fsts.iter().for_each(|fst| op.push(fst.into_stream()));
        let op = op.r#union();

        let mut build = fst::SetBuilder::memory();
        build.extend_stream(op.into_stream()).unwrap();
        Ok(build.into_inner().unwrap())
    }
    else if key == HEADERS_KEY {
        assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
        Ok(values[0].to_vec())
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
        Ok(vec)
    }
    else if key[0] == 5 {
        assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
        Ok(values[0].to_vec())
    }
    else {
        panic!("wut? {:?}", key)
    }
}

// TODO merge with the previous values
fn lmdb_writer(wtxn: &mut heed::RwTxn, index: &Index, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
    if key == WORDS_FST_KEY {
        // Write the words fst
        index.main.put::<_, Str, ByteSlice>(wtxn, "words-fst", val)?;
    }
    else if key == HEADERS_KEY {
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

fn merge_into_lmdb<F>(sources: Vec<Reader<Mmap>>, mut f: F) -> anyhow::Result<()>
where F: FnMut(&[u8], &[u8]) -> anyhow::Result<()>
{
    debug!("Merging {} MTBL stores...", sources.len());
    let before = Instant::now();

    let mut builder = Merger::builder(merge);
    builder.extend(sources);
    let merger = builder.build();

    let mut iter = merger.into_merge_iter()?;
    while let Some(result) = iter.next() {
        let (k, v) = result?;
        (f)(&k, &v)?;
    }

    debug!("MTBL stores merged in {:.02?}!", before.elapsed());
    Ok(())
}

fn index_csv(
    mut rdr: csv::Reader<File>,
    thread_index: usize,
    num_threads: usize,
) -> anyhow::Result<Reader<Mmap>>
{
    debug!("{:?}: Indexing into an Indexed...", thread_index);

    let mut store = Store::new();

    // Write the headers into a Vec of bytes and then into the store.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;
    store.write_headers(&headers)?;

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
                    store.insert_word_position(&word, position)?;
                    store.insert_word_position_docid(&word, position, document_id)?;
                }
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        store.write_document(document_id, &document)?;
    }

    let reader = store.finish()?;
    debug!("{:?}: Store created!", thread_index);
    Ok(reader)
}

// TODO do that in the threads.
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

    // We duplicate the file # jobs times.
    let file = opt.csv_file.unwrap();
    let csv_readers: Vec<_> = (0..num_threads).map(|_| csv::Reader::from_path(&file)).collect::<Result<_, _>>()?;

    let stores: Vec<_> = csv_readers
        .into_par_iter()
        .enumerate()
        .map(|(i, rdr)| index_csv(rdr, i, num_threads))
        .collect::<Result<_, _>>()?;

    debug!("We are writing into LMDB...");
    let mut wtxn = env.write_txn()?;

    merge_into_lmdb(stores, |k, v| lmdb_writer(&mut wtxn, &index, k, v))?;
    compute_words_attributes_docids(&mut wtxn, &index)?;
    let count = index.documents.len(&wtxn)?;

    wtxn.commit()?;
    debug!("Wrote {} documents into LMDB", count);

    Ok(())
}
