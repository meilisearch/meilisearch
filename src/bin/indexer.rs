use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, Read, Write};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::{iter, thread};
use std::time::Instant;

use anyhow::Context;
use arc_cache::ArcCache;
use bstr::ByteSlice as _;
use csv::StringRecord;
use flate2::read::GzDecoder;
use fst::IntoStreamer;
use heed::{EnvOpenOptions, BytesEncode, types::*};
use log::{debug, info};
use memmap::Mmap;
use oxidized_mtbl::{Reader, Writer, Merger, Sorter, CompressionType};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use structopt::StructOpt;

use milli::heed_codec::{CsvStringRecordCodec, ByteorderXRoaringBitmapCodec};
use milli::tokenizer::{simple_tokenizer, only_words};
use milli::{SmallVec32, Index, DocumentId, BEU32};

const LMDB_MAX_KEY_LENGTH: usize = 511;
const ONE_MILLION: usize = 1_000_000;

const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

const HEADERS_KEY: &[u8] = b"\0headers";
const DOCUMENTS_IDS_KEY: &[u8] = b"\x04documents-ids";
const WORDS_FST_KEY: &[u8] = b"\x06words-fst";
const HEADERS_BYTE: u8 = 0;
const WORD_DOCID_POSITIONS_BYTE: u8 = 1;
const WORD_DOCIDS_BYTE: u8 = 2;
const DOCUMENTS_IDS_BYTE: u8 = 4;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "milli-indexer")]
/// The indexer binary of the milli project.
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

    #[structopt(flatten)]
    indexer: IndexerOpt,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// CSV file to index, if unspecified the CSV is read from standard input.
    ///
    /// You can also provide a ".gz" or ".gzip" CSV file, the indexer will figure out
    /// how to decode and read it.
    ///
    /// Note that it is much faster to index from a file as when the indexer reads from stdin
    /// it will dedicate a thread for that and context switches could slow down the indexing jobs.
    csv_file: Option<PathBuf>,
}

#[derive(Debug, StructOpt)]
struct IndexerOpt {
    /// MTBL max number of chunks in bytes.
    #[structopt(long)]
    max_nb_chunks: Option<usize>,

    /// MTBL max memory in bytes.
    #[structopt(long)]
    max_memory: Option<usize>,

    /// Size of the ARC cache when indexing.
    #[structopt(long, default_value = "43690")]
    arc_cache_size: usize,

    /// The name of the compression algorithm to use when compressing intermediate
    /// chunks during indexing documents.
    ///
    /// Choosing a fast algorithm will make the indexing faster but may consume more memory.
    #[structopt(long, default_value = "snappy", possible_values = &["snappy", "zlib", "lz4", "lz4hc", "zstd"])]
    chunk_compression_type: String,

    /// The level of compression of the chosen algorithm.
    #[structopt(long, requires = "chunk-compression-type")]
    chunk_compression_level: Option<u32>,
}

fn compression_type_from_str(name: &str) -> CompressionType {
    match name {
        "snappy" => CompressionType::Snappy,
        "zlib" => CompressionType::Zlib,
        "lz4" => CompressionType::Lz4,
        "lz4hc" => CompressionType::Lz4hc,
        "zstd" => CompressionType::Zstd,
        _ => panic!("invalid compression algorithm"),
    }
}

fn lmdb_key_valid_size(key: &[u8]) -> bool {
    !key.is_empty() && key.len() <= LMDB_MAX_KEY_LENGTH
}

type MergeFn = fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, ()>;

struct Store {
    word_docids: ArcCache<SmallVec32<u8>, RoaringBitmap>,
    documents_ids: RoaringBitmap,
    sorter: Sorter<MergeFn>,
    documents_sorter: Sorter<MergeFn>,
}

impl Store {
    pub fn new(
        arc_cache_size: usize,
        max_nb_chunks: Option<usize>,
        max_memory: Option<usize>,
        chunk_compression_type: CompressionType,
        chunk_compression_level: Option<u32>,
    ) -> Store
    {
        let mut builder = Sorter::builder(merge as MergeFn);
        builder.chunk_compression_type(chunk_compression_type);
        if let Some(level) = chunk_compression_level {
            builder.chunk_compression_level(level);
        }
        if let Some(nb_chunks) = max_nb_chunks {
            builder.max_nb_chunks(nb_chunks);
        }
        if let Some(memory) = max_memory {
            builder.max_memory(memory);
        }

        let mut documents_builder = Sorter::builder(docs_merge as MergeFn);
        documents_builder.chunk_compression_type(chunk_compression_type);
        if let Some(level) = chunk_compression_level {
            builder.chunk_compression_level(level);
        }

        Store {
            word_docids: ArcCache::new(arc_cache_size),
            documents_ids: RoaringBitmap::new(),
            sorter: builder.build(),
            documents_sorter: documents_builder.build(),
        }
    }

    // Save the documents ids under the position and word we have seen it.
    fn insert_word_docid(&mut self, word: &str, id: DocumentId) -> anyhow::Result<()> {
        let word_vec = SmallVec32::from(word.as_bytes());
        let ids = RoaringBitmap::from_iter(Some(id));
        let (_, lrus) = self.word_docids.insert(word_vec, ids, |old, new| old.union_with(&new));
        Self::write_word_docids(&mut self.sorter, lrus)?;
        Ok(())
    }

    fn write_headers(&mut self, headers: &StringRecord) -> anyhow::Result<()> {
        let headers = CsvStringRecordCodec::bytes_encode(headers)
            .with_context(|| format!("could not encode csv record"))?;
        Ok(self.sorter.insert(HEADERS_KEY, headers)?)
    }

    fn write_document(
        &mut self,
        id: DocumentId,
        iter: impl IntoIterator<Item=(String, RoaringBitmap)>,
        record: &StringRecord,
    ) -> anyhow::Result<()>
    {
        let record = CsvStringRecordCodec::bytes_encode(record)
            .with_context(|| format!("could not encode csv record"))?;
        self.documents_ids.insert(id);
        self.documents_sorter.insert(id.to_be_bytes(), record)?;
        Self::write_docid_word_positions(&mut self.sorter, id, iter)?;
        Ok(())
    }

    fn write_docid_word_positions<I>(sorter: &mut Sorter<MergeFn>, id: DocumentId, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=(String, RoaringBitmap)>
    {
        // postings positions ids keys are all prefixed
        let mut key = vec![WORD_DOCID_POSITIONS_BYTE];

        // We prefix the words by the document id.
        key.extend_from_slice(&id.to_be_bytes());
        let base_size = key.len();

        for (word, positions) in iter {
            key.truncate(base_size);
            key.extend_from_slice(word.as_bytes());
            // We serialize the positions into a buffer.
            let bytes = ByteorderXRoaringBitmapCodec::bytes_encode(&positions)
                .with_context(|| format!("could not serialize positions"))?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &bytes)?;
            }
        }

        Ok(())
    }

    fn write_word_docids<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=(SmallVec32<u8>, RoaringBitmap)>
    {
        // postings positions ids keys are all prefixed
        let mut key = vec![WORD_DOCIDS_BYTE];
        let mut buffer = Vec::new();

        for (word, ids) in iter {
            key.truncate(1);
            key.extend_from_slice(&word);
            // We serialize the document ids into a buffer
            buffer.clear();
            buffer.reserve(ids.serialized_size());
            ids.serialize_into(&mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            }
        }

        Ok(())
    }

    fn write_documents_ids(sorter: &mut Sorter<MergeFn>, ids: RoaringBitmap) -> anyhow::Result<()> {
        let mut buffer = Vec::with_capacity(ids.serialized_size());
        ids.serialize_into(&mut buffer)?;
        sorter.insert(DOCUMENTS_IDS_KEY, &buffer)?;
        Ok(())
    }

    pub fn index_csv(
        mut self,
        mut rdr: csv::Reader<Box<dyn Read + Send>>,
        thread_index: usize,
        num_threads: usize,
    ) -> anyhow::Result<(Reader<Mmap>, Reader<Mmap>)>
    {
        debug!("{:?}: Indexing in a Store...", thread_index);

        // Write the headers into the store.
        let headers = rdr.headers()?;
        self.write_headers(&headers)?;

        let mut before = Instant::now();
        let mut document_id: usize = 0;
        let mut document = csv::StringRecord::new();
        let mut word_positions = HashMap::new();
        while rdr.read_record(&mut document)? {

            // We skip documents that must not be indexed by this thread.
            if document_id % num_threads == thread_index {
                if document_id % ONE_MILLION == 0 {
                    let count = document_id / ONE_MILLION;
                    info!("We have seen {}m documents so far ({:.02?}).", count, before.elapsed());
                    before = Instant::now();
                }

                let document_id = DocumentId::try_from(document_id).context("generated id is too big")?;
                for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
                    for (pos, (_, token)) in simple_tokenizer(&content).filter(only_words).enumerate().take(MAX_POSITION) {
                        let word = token.to_lowercase();
                        let position = (attr * MAX_POSITION + pos) as u32;
                        self.insert_word_docid(&word, document_id)?;
                        word_positions.entry(word).or_insert_with(RoaringBitmap::new).insert(position);
                    }
                }

                // We write the document in the documents store.
                self.write_document(document_id, word_positions.drain(), &document)?;
            }

            // Compute the document id of the next document.
            document_id = document_id + 1;
        }

        let (reader, docs_reader) = self.finish()?;
        debug!("{:?}: Store created!", thread_index);
        Ok((reader, docs_reader))
    }

    fn finish(mut self) -> anyhow::Result<(Reader<Mmap>, Reader<Mmap>)> {
        Self::write_word_docids(&mut self.sorter, self.word_docids)?;
        Self::write_documents_ids(&mut self.sorter, self.documents_ids)?;

        let mut wtr = tempfile::tempfile().map(Writer::new)?;
        let mut builder = fst::SetBuilder::memory();

        let mut iter = self.sorter.into_iter()?;
        while let Some(result) = iter.next() {
            let (key, val) = result?;
            if let Some((&WORD_DOCIDS_BYTE, word)) = key.split_first() {
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

            // Union of the FSTs
            let mut op = fst::set::OpBuilder::new();
            fsts.iter().for_each(|fst| op.push(fst.into_stream()));
            let op = op.r#union();

            let mut build = fst::SetBuilder::memory();
            build.extend_stream(op.into_stream()).unwrap();
            Ok(build.into_inner().unwrap())
        },
        key => match key[0] {
            HEADERS_BYTE | WORD_DOCID_POSITIONS_BYTE => {
                assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
                Ok(values[0].to_vec())
            },
            DOCUMENTS_IDS_BYTE | WORD_DOCIDS_BYTE => {
                let (head, tail) = values.split_first().unwrap();

                let mut head = RoaringBitmap::deserialize_from(head.as_slice()).unwrap();
                for value in tail {
                    let bitmap = RoaringBitmap::deserialize_from(value.as_slice()).unwrap();
                    head.union_with(&bitmap);
                }

                let mut vec = Vec::with_capacity(head.serialized_size());
                head.serialize_into(&mut vec).unwrap();
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
    else if key == DOCUMENTS_IDS_KEY {
        // Write the documents ids list
        index.main.put::<_, Str, ByteSlice>(wtxn, "documents-ids", val)?;
    }
    else if key.starts_with(&[WORD_DOCIDS_BYTE]) {
        // Write the postings lists
        index.word_docids.as_polymorph()
            .put::<_, ByteSlice, ByteSlice>(wtxn, &key[1..], val)?;
    }
    else if key.starts_with(&[WORD_DOCID_POSITIONS_BYTE]) {
        // Write the postings lists
        index.docid_word_positions.as_polymorph()
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

/// Returns the list of CSV sources that the indexer must read.
///
/// There is `num_threads` sources. If the file is not specified, the standard input is used.
fn csv_readers(
    csv_file_path: Option<PathBuf>,
    num_threads: usize,
) -> anyhow::Result<Vec<csv::Reader<Box<dyn Read + Send>>>>
{
    match csv_file_path {
        Some(file_path) => {
            // We open the file # jobs times.
            iter::repeat_with(|| {
                let file = File::open(&file_path)
                    .with_context(|| format!("Failed to read CSV file {}", file_path.display()))?;
                // if the file extension is "gz" or "gzip" we can decode and read it.
                let r = if file_path.extension().map_or(false, |e| e == "gz" || e == "gzip") {
                    Box::new(GzDecoder::new(file)) as Box<dyn Read + Send>
                } else {
                    Box::new(file) as Box<dyn Read + Send>
                };
                Ok(csv::Reader::from_reader(r)) as anyhow::Result<_>
            })
            .take(num_threads)
            .collect()
        },
        None => {
            let mut csv_readers = Vec::new();
            let mut writers = Vec::new();
            for (r, w) in iter::repeat_with(ringtail::io::pipe).take(num_threads) {
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

            Ok(csv_readers)
        },
    }
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

    let before_indexing = Instant::now();
    let index = Index::new(&env)?;

    let num_threads = rayon::current_num_threads();
    let arc_cache_size = opt.indexer.arc_cache_size;
    let max_nb_chunks = opt.indexer.max_nb_chunks;
    let max_memory = opt.indexer.max_memory;
    let chunk_compression_type = compression_type_from_str(&opt.indexer.chunk_compression_type);
    let chunk_compression_level = opt.indexer.chunk_compression_level;

    let readers = csv_readers(opt.csv_file, num_threads)?
        .into_par_iter()
        .enumerate()
        .map(|(i, rdr)| {
            Store::new(
                arc_cache_size,
                max_nb_chunks,
                max_memory,
                chunk_compression_type,
                chunk_compression_level,
            ).index_csv(rdr, i, num_threads)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut stores = Vec::with_capacity(readers.len());
    let mut docs_stores = Vec::with_capacity(readers.len());
    readers.into_iter().for_each(|(s, d)| {
        stores.push(s);
        docs_stores.push(d);
    });

    let mut wtxn = env.write_txn()?;

    // We merge the postings lists into LMDB.
    debug!("We are writing the postings lists into LMDB on disk...");
    merge_into_lmdb(stores, |k, v| lmdb_writer(&mut wtxn, &index, k, v))?;

    // We merge the documents into LMDB.
    debug!("We are writing the documents into LMDB on disk...");
    merge_into_lmdb(docs_stores, |k, v| {
        let id = k.try_into().map(u32::from_be_bytes)?;
        Ok(index.documents.put(&mut wtxn, &BEU32::new(id), v)?)
    })?;

    // Retrieve the number of documents.
    let count = index.number_of_documents(&wtxn)?;

    wtxn.commit()?;

    info!("Wrote {} documents in {:.02?}", count, before_indexing.elapsed());

    Ok(())
}
