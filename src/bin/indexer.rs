use std::convert::TryInto;
use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use anyhow::Context;
use arc_cache::ArcCache;
use bstr::ByteSlice as _;
use cow_utils::CowUtils;
use fst::IntoStreamer;
use heed::EnvOpenOptions;
use heed::types::*;
use log::debug;
use memmap::Mmap;
use oxidized_mtbl::{Reader, Writer, Merger, Sorter, CompressionType};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use structopt::StructOpt;

use milli::{lexer, SmallVec32, Index, DocumentId, Position, Attribute};

const LMDB_MAX_KEY_LENGTH: usize = 511;
const ONE_MILLION: usize = 1_000_000;

const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

const HEADERS_KEY: &[u8] = b"\0headers";
const WORDS_FST_KEY: &[u8] = b"\x05words-fst";
const WORD_POSITIONS_BYTE: u8 = 1;
const WORD_POSITION_DOCIDS_BYTE: u8 = 2;
const WORD_ATTRIBUTE_DOCIDS_BYTE: u8 = 3;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "milli-indexer", about = "The indexer binary of the milli project.")]
struct Opt {
    /// The database path where the database is located.
    /// It is created if it doesn't already exist.
    #[structopt(long = "db", parse(from_os_str))]
    database: PathBuf,

    /// The maximum size the database can take on disk. It is recommended to specify
    /// the whole disk space (value must be a multiple of a page size).
    #[structopt(long = "db-size", default_value = "107374182400")] // 100 GB
    database_size: usize,

    /// Number of parallel jobs, defaults to # of CPUs.
    #[structopt(short, long)]
    jobs: Option<usize>,

    /// MTBL max number of chunks in bytes.
    #[structopt(long)]
    max_nb_chunks: Option<usize>,

    /// MTBL max memory in bytes.
    #[structopt(long)]
    max_memory: Option<usize>,

    /// Size of the ARC cache when indexing.
    #[structopt(long)]
    arc_cache_size: Option<usize>,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// CSV file to index, if unspecified the CSV is read from standard input.
    /// Note that it is much faster to index from a file.
    csv_file: Option<PathBuf>,
}

fn lmdb_key_valid_size(key: &[u8]) -> bool {
    !key.is_empty() && key.len() <= LMDB_MAX_KEY_LENGTH
}

type MergeFn = fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, ()>;

struct Store {
    word_positions: ArcCache<SmallVec32<u8>, RoaringBitmap>,
    word_position_docids: ArcCache<(SmallVec32<u8>, Position), RoaringBitmap>,
    word_attribute_docids: ArcCache<(SmallVec32<u8>, Attribute), RoaringBitmap>,
    sorter: Sorter<MergeFn>,
    documents_sorter: Sorter<MergeFn>,
}

impl Store {
    fn new(arc_cache_size: Option<usize>, max_nb_chunks: Option<usize>, max_memory: Option<usize>) -> Store {
        let mut builder = Sorter::builder(merge as MergeFn);
        builder.chunk_compression_type(CompressionType::Snappy);
        if let Some(nb_chunks) = max_nb_chunks {
            builder.max_nb_chunks(nb_chunks);
        }
        if let Some(memory) = max_memory {
            builder.max_memory(memory);
        }

        let mut documents_builder = Sorter::builder(docs_merge as MergeFn);
        documents_builder.chunk_compression_type(CompressionType::Snappy);

        let arc_cache_size = arc_cache_size.unwrap_or(65_535);

        Store {
            word_positions: ArcCache::new(arc_cache_size),
            word_position_docids: ArcCache::new(arc_cache_size),
            word_attribute_docids: ArcCache::new(arc_cache_size),
            sorter: builder.build(),
            documents_sorter: documents_builder.build(),
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
        let word_vec = SmallVec32::from(word.as_bytes());
        let ids = RoaringBitmap::from_iter(Some(id));
        let (_, lrus) = self.word_position_docids.insert((word_vec, position), ids, |old, new| old.union_with(&new));
        Self::write_word_position_docids(&mut self.sorter, lrus)?;
        self.insert_word_attribute_docid(word, position / MAX_POSITION as u32, id)
    }

    // Save the documents ids under the attribute and word we have seen it.
    fn insert_word_attribute_docid(&mut self, word: &str, attribute: Attribute, id: DocumentId) -> anyhow::Result<()> {
        let word = SmallVec32::from(word.as_bytes());
        let ids = RoaringBitmap::from_iter(Some(id));
        let (_, lrus) = self.word_attribute_docids.insert((word, attribute), ids, |old, new| old.union_with(&new));
        Self::write_word_attribute_docids(&mut self.sorter, lrus)
    }

    pub fn write_headers(&mut self, headers: &[u8]) -> anyhow::Result<()> {
        Ok(self.sorter.insert(HEADERS_KEY, headers)?)
    }

    pub fn write_document(&mut self, id: DocumentId, content: &[u8]) -> anyhow::Result<()> {
        Ok(self.documents_sorter.insert(id.to_be_bytes(), content)?)
    }

    fn write_word_positions<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=(SmallVec32<u8>, RoaringBitmap)>
    {
        // postings ids keys are all prefixed
        let mut key = vec![WORD_POSITIONS_BYTE];
        let mut buffer = Vec::new();

        for (word, positions) in iter {
            key.truncate(1);
            key.extend_from_slice(&word);
            // We serialize the positions into a buffer
            buffer.clear();
            positions.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            }
        }

        Ok(())
    }

    fn write_word_position_docids<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=((SmallVec32<u8>, Position), RoaringBitmap)>
    {
        // postings positions ids keys are all prefixed
        let mut key = vec![WORD_POSITION_DOCIDS_BYTE];
        let mut buffer = Vec::new();

        for ((word, pos), ids) in iter {
            key.truncate(1);
            key.extend_from_slice(&word);
            // we postfix the word by the positions it appears in
            key.extend_from_slice(&pos.to_be_bytes());
            // We serialize the document ids into a buffer
            buffer.clear();
            ids.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            }
        }

        Ok(())
    }

    fn write_word_attribute_docids<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=((SmallVec32<u8>, Attribute), RoaringBitmap)>
    {
        // postings attributes keys are all prefixed
        let mut key = vec![WORD_ATTRIBUTE_DOCIDS_BYTE];
        let mut buffer = Vec::new();

        for ((word, attr), ids) in iter {
            key.truncate(1);
            key.extend_from_slice(&word);
            // we postfix the word by the positions it appears in
            key.extend_from_slice(&attr.to_be_bytes());
            // We serialize the document ids into a buffer
            buffer.clear();
            ids.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            }
        }

        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<(Reader<Mmap>, Reader<Mmap>)> {
        Self::write_word_positions(&mut self.sorter, self.word_positions)?;
        Self::write_word_position_docids(&mut self.sorter, self.word_position_docids)?;
        Self::write_word_attribute_docids(&mut self.sorter, self.word_attribute_docids)?;

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

        let mut docs_wtr = tempfile::tempfile().map(Writer::new)?;
        self.documents_sorter.write_into(&mut docs_wtr)?;
        let docs_file = docs_wtr.into_inner()?;
        let docs_mmap = unsafe { Mmap::map(&docs_file)? };
        let docs_reader = Reader::new(docs_mmap)?;

        let file = wtr.into_inner()?;
        let mmap = unsafe { Mmap::map(&file)? };
        let reader = Reader::new(mmap)?;

        Ok((reader, docs_reader))
    }
}

fn docs_merge(key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    let key = key.try_into().unwrap();
    let id = u32::from_be_bytes(key);
    panic!("documents must not conflict ({} with {} values)!", id, values.len())
}

fn merge(key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    match key {
        WORDS_FST_KEY => {
            let fsts: Vec<_> = values.iter().map(|v| fst::Set::new(v).unwrap()).collect();

            // Union of the two FSTs
            let mut op = fst::set::OpBuilder::new();
            fsts.iter().for_each(|fst| op.push(fst.into_stream()));
            let op = op.r#union();

            let mut build = fst::SetBuilder::memory();
            build.extend_stream(op.into_stream()).unwrap();
            Ok(build.into_inner().unwrap())
        },
        HEADERS_KEY => {
            assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
            Ok(values[0].to_vec())
        },
        key => match key[0] {
              WORD_POSITIONS_BYTE | WORD_POSITION_DOCIDS_BYTE | WORD_ATTRIBUTE_DOCIDS_BYTE => {
                let mut first = RoaringBitmap::deserialize_from(values[0].as_slice()).unwrap();

                for value in &values[1..] {
                    let bitmap = RoaringBitmap::deserialize_from(value.as_slice()).unwrap();
                    first.union_with(&bitmap);
                }

                let mut vec = Vec::new();
                first.serialize_into(&mut vec).unwrap();
                Ok(vec)
            },
            otherwise => panic!("wut {:?}", otherwise),
        }
    }
}

// TODO merge with the previous values
// TODO store the documents in a compressed MTBL
fn lmdb_writer(wtxn: &mut heed::RwTxn, index: &Index, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
    if key == WORDS_FST_KEY {
        // Write the words fst
        index.main.put::<_, Str, ByteSlice>(wtxn, "words-fst", val)?;
    }
    else if key == HEADERS_KEY {
        // Write the headers
        index.main.put::<_, Str, ByteSlice>(wtxn, "headers", val)?;
    }
    else if key.starts_with(&[WORD_POSITIONS_BYTE]) {
        // Write the postings lists
        index.word_positions.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[WORD_POSITION_DOCIDS_BYTE]) {
        // Write the postings lists
        index.word_position_docids.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[WORD_ATTRIBUTE_DOCIDS_BYTE]) {
        // Write the attribute postings lists
        index.word_attribute_docids.as_polymorph()
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
        (f)(&k, &v).with_context(|| format!("writing {:?} {:?} into LMDB", k.as_bstr(), k.as_bstr()))?;
    }

    debug!("MTBL stores merged in {:.02?}!", before.elapsed());
    Ok(())
}

fn index_csv(
    mut rdr: csv::Reader<Box<dyn Read + Send>>,
    thread_index: usize,
    num_threads: usize,
    arc_cache_size: Option<usize>,
    max_nb_chunks: Option<usize>,
    max_memory: Option<usize>,
) -> anyhow::Result<(Reader<Mmap>, Reader<Mmap>)>
{
    debug!("{:?}: Indexing into a Store...", thread_index);

    let mut store = Store::new(arc_cache_size, max_nb_chunks, max_memory);

    // Write the headers into a Vec of bytes and then into the store.
    let headers = rdr.headers()?;
    let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
    writer.write_byte_record(headers.as_byte_record())?;
    let headers = writer.into_inner()?;
    store.write_headers(&headers)?;

    let mut before = Instant::now();
    let mut document_id: usize = 0;
    let mut document = csv::StringRecord::new();
    while rdr.read_record(&mut document)? {
        document_id = document_id + 1;

        // We skip documents that must not be indexed by this thread
        if document_id % num_threads != thread_index { continue }

        let document_id = DocumentId::try_from(document_id).context("generated id is too big")?;
        if document_id % (ONE_MILLION as u32) == 0 {
            debug!("We have seen {}m documents so far ({:.02?}).",
                document_id / ONE_MILLION as u32, before.elapsed());
            before = Instant::now();
        }

        for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
            for (pos, word) in lexer::break_string(&content).enumerate().take(MAX_POSITION) {
                let word = word.cow_to_lowercase();
                let position = (attr * MAX_POSITION + pos) as u32;
                store.insert_word_position(&word, position)?;
                store.insert_word_position_docid(&word, position, document_id)?;
            }
        }

        // We write the document in the database.
        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        writer.write_byte_record(document.as_byte_record())?;
        let document = writer.into_inner()?;
        store.write_document(document_id, &document)?;
    }

    let (reader, docs_reader) = store.finish()?;
    debug!("{:?}: Store created!", thread_index);
    Ok((reader, docs_reader))
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
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    let mut index = Index::new(&env, &opt.database)?;

    let documents_path = opt.database.join("documents.mtbl");
    let num_threads = rayon::current_num_threads();
    let arc_cache_size = opt.arc_cache_size;
    let max_nb_chunks = opt.max_nb_chunks;
    let max_memory = opt.max_memory;

    let csv_readers = match opt.csv_file {
        Some(file_path) => {
            // We open the file # jobs times.
            (0..num_threads)
                .map(|_| {
                    let file = File::open(&file_path)?;
                    let r = Box::new(file) as Box<dyn Read + Send>;
                    Ok(csv::Reader::from_reader(r)) as io::Result<_>
                })
                .collect::<Result<Vec<_>, _>>()?
        },
        None => {
            let mut csv_readers = Vec::new();
            let mut writers = Vec::new();
            for (r, w) in (0..num_threads).map(|_| pipe::pipe()) {
                let r = Box::new(r) as Box<dyn Read + Send>;
                csv_readers.push(csv::Reader::from_reader(r));
                writers.push(w);
            }

            thread::spawn(move || {
                let stdin = std::io::stdin();
                let mut stdin = stdin.lock();
                let mut buffer = [0u8; 4096];
                loop {
                    match stdin.read(&mut buffer)? {
                        0 => return Ok(()) as io::Result<()>,
                        size => for w in &mut writers {
                            w.write_all(&buffer[..size])?;
                        }
                    }
                }
            });

            csv_readers
        },
    };

    let readers = csv_readers
        .into_par_iter()
        .enumerate()
        .map(|(i, rdr)| index_csv(rdr, i, num_threads, arc_cache_size, max_nb_chunks, max_memory))
        .collect::<Result<Vec<_>, _>>()?;

    let mut stores = Vec::with_capacity(readers.len());
    let mut docs_stores = Vec::with_capacity(readers.len());

    readers.into_iter().for_each(|(s, d)| {
        stores.push(s);
        docs_stores.push(d);
    });

    debug!("We are writing into LMDB and MTBL...");

    // We run both merging steps in parallel.
    let (lmdb, mtbl) = rayon::join(|| {
        // We merge the postings lists into LMDB.
        let mut wtxn = env.write_txn()?;
        merge_into_lmdb(stores, |k, v| lmdb_writer(&mut wtxn, &index, k, v))?;
        Ok(wtxn.commit()?) as anyhow::Result<_>
    }, || {
        // We also merge the documents into its own MTBL store.
        let file = OpenOptions::new().create(true).truncate(true).write(true).read(true).open(documents_path)?;
        let mut writer = Writer::builder().compression_type(CompressionType::Snappy).build(file);
        let mut builder = Merger::builder(docs_merge);
        builder.extend(docs_stores);
        builder.build().write_into(&mut writer)?;
        Ok(writer.finish()?) as anyhow::Result<_>
    });

    lmdb.and(mtbl)?;
    index.refresh_documents()?;
    let count = index.number_of_documents();

    debug!("Wrote {} documents into LMDB", count);

    Ok(())
}
