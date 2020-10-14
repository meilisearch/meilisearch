use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::mpsc::sync_channel;
use std::time::Instant;
use std::{cmp, iter, thread};

use anyhow::{Context, bail};
use bstr::ByteSlice as _;
use csv::StringRecord;
use flate2::read::GzDecoder;
use fst::IntoStreamer;
use heed::{EnvOpenOptions, BytesEncode, types::ByteSlice};
use linked_hash_map::LinkedHashMap;
use log::{debug, info};
use grenad::{Reader, FileFuse, Writer, Merger, Sorter, CompressionType};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use structopt::StructOpt;
use tempfile::tempfile;

use milli::heed_codec::{CsvStringRecordCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec};
use milli::tokenizer::{simple_tokenizer, only_token};
use milli::{SmallVec32, Index, Position, DocumentId};

const LMDB_MAX_KEY_LENGTH: usize = 511;
const ONE_KILOBYTE: usize = 1024 * 1024;

const MAX_POSITION: usize = 1000;
const MAX_ATTRIBUTES: usize = u32::max_value() as usize / MAX_POSITION;

const WORDS_FST_KEY: &[u8] = milli::WORDS_FST_KEY.as_bytes();
const HEADERS_KEY: &[u8] = milli::HEADERS_KEY.as_bytes();
const DOCUMENTS_IDS_KEY: &[u8] = milli::DOCUMENTS_IDS_KEY.as_bytes();

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
    /// The amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[structopt(long, default_value = "1000000")] // 1m
    log_every_n: usize,

    /// MTBL max number of chunks in bytes.
    #[structopt(long)]
    max_nb_chunks: Option<usize>,

    /// The maximum amount of memory to use for the MTBL buffer. It is recommended
    /// to use something like 80%-90% of the available memory.
    ///
    /// It is automatically split by the number of jobs e.g. if you use 7 jobs
    /// and 7 GB of max memory, each thread will use a maximum of 1 GB.
    #[structopt(long, default_value = "7516192768")] // 7 GB
    max_memory: usize,

    /// Size of the linked hash map cache when indexing.
    /// The bigger it is, the faster the indexing is but the more memory it takes.
    #[structopt(long, default_value = "500")]
    linked_hash_map_size: usize,

    /// The name of the compression algorithm to use when compressing intermediate
    /// chunks during indexing documents.
    ///
    /// Choosing a fast algorithm will make the indexing faster but may consume more memory.
    #[structopt(long, default_value = "snappy", possible_values = &["snappy", "zlib", "lz4", "lz4hc", "zstd"])]
    chunk_compression_type: CompressionType,

    /// The level of compression of the chosen algorithm.
    #[structopt(long, requires = "chunk-compression-type")]
    chunk_compression_level: Option<u32>,

    /// The number of bytes to remove from the begining of the chunks while reading/sorting
    /// or merging them.
    ///
    /// File fusing must only be enable on file systems that support the `FALLOC_FL_COLLAPSE_RANGE`,
    /// (i.e. ext4 and XFS). File fusing will only work if the `enable-chunk-fusing` is set.
    #[structopt(long, default_value = "4294967296")] // 4 GB
    chunk_fusing_shrink_size: u64,

    /// Enable the chunk fusing or not, this reduces the amount of disk used by a factor of 2.
    #[structopt(long)]
    enable_chunk_fusing: bool,
}

fn format_count(n: usize) -> String {
    human_format::Formatter::new().with_decimals(1).with_separator("").format(n as f64)
}

fn lmdb_key_valid_size(key: &[u8]) -> bool {
    !key.is_empty() && key.len() <= LMDB_MAX_KEY_LENGTH
}

fn create_writer(typ: CompressionType, level: Option<u32>, file: File) -> io::Result<Writer<File>> {
    let mut builder = Writer::builder();
    builder.compression_type(typ);
    if let Some(level) = level {
        builder.compression_level(level);
    }
    builder.build(file)
}

fn writer_into_reader(writer: Writer<File>, shrink_size: Option<u64>) -> anyhow::Result<Reader<FileFuse>> {
    let mut file = writer.into_inner()?;
    file.seek(SeekFrom::Start(0))?;
    let file = if let Some(shrink_size) = shrink_size {
        FileFuse::builder().shrink_size(shrink_size).build(file)
    } else {
        FileFuse::new(file)
    };
    Reader::new(file).map_err(Into::into)
}

fn create_sorter(
    merge: MergeFn,
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    chunk_fusing_shrink_size: Option<u64>,
    max_nb_chunks: Option<usize>,
    max_memory: Option<usize>,
) -> Sorter<MergeFn>
{
    let mut builder = Sorter::builder(merge);
    if let Some(shrink_size) = chunk_fusing_shrink_size {
        builder.file_fusing_shrink_size(shrink_size);
    }
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
    builder.build()
}

/// Outputs a list of all pairs of words with the shortest proximity between 1 and 7 inclusive.
///
/// This list is used by the engine to calculate the documents containing words that are
/// close to each other.
fn compute_words_pair_proximities(
    word_positions: &HashMap<String, SmallVec32<Position>>,
) -> HashMap<(&str, &str), u8>
{
    use itertools::Itertools;

    let mut words_pair_proximities = HashMap::new();
    for ((w1, ps1), (w2, ps2)) in word_positions.iter().cartesian_product(word_positions) {
        let mut min_prox = None;
        for (ps1, ps2) in ps1.iter().cartesian_product(ps2) {
            let prox = milli::proximity::positions_proximity(*ps1, *ps2);
            let prox = u8::try_from(prox).unwrap();
            // We don't care about a word that appear at the
            // same position or too far from the other.
            if prox >= 1 && prox <= 7 {
                match min_prox {
                    None => min_prox = Some(prox),
                    Some(mp) => if prox < mp { min_prox = Some(prox) },
                }
            }
        }

        if let Some(min_prox) = min_prox {
            words_pair_proximities.insert((w1.as_str(), w2.as_str()), min_prox);
        }
    }

    words_pair_proximities
}

type MergeFn = fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, ()>;

struct Readers {
    main: Reader<FileFuse>,
    word_docids: Reader<FileFuse>,
    docid_word_positions: Reader<FileFuse>,
    words_pairs_proximities_docids: Reader<FileFuse>,
    documents: Reader<FileFuse>,
}

struct Store {
    word_docids: LinkedHashMap<SmallVec32<u8>, RoaringBitmap>,
    word_docids_limit: usize,
    words_pairs_proximities_docids: LinkedHashMap<(SmallVec32<u8>, SmallVec32<u8>, u8), RoaringBitmap>,
    words_pairs_proximities_docids_limit: usize,
    documents_ids: RoaringBitmap,
    // MTBL parameters
    chunk_compression_type: CompressionType,
    chunk_compression_level: Option<u32>,
    chunk_fusing_shrink_size: Option<u64>,
    // MTBL sorters
    main_sorter: Sorter<MergeFn>,
    word_docids_sorter: Sorter<MergeFn>,
    words_pairs_proximities_docids_sorter: Sorter<MergeFn>,
    // MTBL writers
    docid_word_positions_writer: Writer<File>,
    documents_writer: Writer<File>,
}

impl Store {
    pub fn new(
        linked_hash_map_size: usize,
        max_nb_chunks: Option<usize>,
        max_memory: Option<usize>,
        chunk_compression_type: CompressionType,
        chunk_compression_level: Option<u32>,
        chunk_fusing_shrink_size: Option<u64>,
    ) -> anyhow::Result<Store>
    {
        // We divide the max memory by the number of sorter the Store have.
        let max_memory = max_memory.map(|mm| cmp::max(ONE_KILOBYTE, mm / 3));

        let main_sorter = create_sorter(
            main_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let word_docids_sorter = create_sorter(
            word_docids_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );
        let words_pairs_proximities_docids_sorter = create_sorter(
            words_pairs_proximities_docids_merge,
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,
            max_nb_chunks,
            max_memory,
        );

        let documents_writer = tempfile().and_then(|f| {
            create_writer(chunk_compression_type, chunk_compression_level, f)
        })?;
        let docid_word_positions_writer = tempfile().and_then(|f| {
            create_writer(chunk_compression_type, chunk_compression_level, f)
        })?;

        Ok(Store {
            word_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            word_docids_limit: linked_hash_map_size,
            words_pairs_proximities_docids: LinkedHashMap::with_capacity(linked_hash_map_size),
            words_pairs_proximities_docids_limit: linked_hash_map_size,
            documents_ids: RoaringBitmap::new(),
            chunk_compression_type,
            chunk_compression_level,
            chunk_fusing_shrink_size,

            main_sorter,
            word_docids_sorter,
            words_pairs_proximities_docids_sorter,

            docid_word_positions_writer,
            documents_writer,
        })
    }

    // Save the documents ids under the position and word we have seen it.
    fn insert_word_docid(&mut self, word: &str, id: DocumentId) -> anyhow::Result<()> {
        // if get_refresh finds the element it is assured to be at the end of the linked hash map.
        match self.word_docids.get_refresh(word.as_bytes()) {
            Some(old) => { old.insert(id); },
            None => {
                let word_vec = SmallVec32::from(word.as_bytes());
                // A newly inserted element is append at the end of the linked hash map.
                self.word_docids.insert(word_vec, RoaringBitmap::from_iter(Some(id)));
                // If the word docids just reached it's capacity we must make sure to remove
                // one element, this way next time we insert we doesn't grow the capacity.
                if self.word_docids.len() == self.word_docids_limit {
                    // Removing the front element is equivalent to removing the LRU element.
                    let lru = self.word_docids.pop_front();
                    Self::write_word_docids(&mut self.word_docids_sorter, lru)?;
                }
            }
        }
        Ok(())
    }

    // Save the documents ids under the words pairs proximities that it contains.
    fn insert_words_pairs_proximities_docids<'a>(
        &mut self,
        words_pairs_proximities: impl IntoIterator<Item=((&'a str, &'a str), u8)>,
        id: DocumentId,
    ) -> anyhow::Result<()>
    {
        for ((w1, w2), prox) in words_pairs_proximities {
            let w1 = SmallVec32::from(w1.as_bytes());
            let w2 = SmallVec32::from(w2.as_bytes());
            let key = (w1, w2, prox);
            // if get_refresh finds the element it is assured
            // to be at the end of the linked hash map.
            match self.words_pairs_proximities_docids.get_refresh(&key) {
                Some(old) => { old.insert(id); },
                None => {
                    // A newly inserted element is append at the end of the linked hash map.
                    let ids = RoaringBitmap::from_iter(Some(id));
                    self.words_pairs_proximities_docids.insert(key, ids);
                }
            }
        }

        // If the linked hashmap is over capacity we must remove the overflowing elements.
        let len = self.words_pairs_proximities_docids.len();
        let overflow = len.checked_sub(self.words_pairs_proximities_docids_limit);
        if let Some(overflow) = overflow {
            let mut lrus = Vec::with_capacity(overflow);
            // Removing front elements is equivalent to removing the LRUs.
            let iter = iter::from_fn(|| self.words_pairs_proximities_docids.pop_front());
            iter.take(overflow).for_each(|x| lrus.push(x));
            Self::write_words_pairs_proximities(&mut self.words_pairs_proximities_docids_sorter, lrus)?;
        }

        Ok(())
    }

    fn write_headers(&mut self, headers: &StringRecord) -> anyhow::Result<()> {
        let headers = CsvStringRecordCodec::bytes_encode(headers)
            .with_context(|| format!("could not encode csv record"))?;
        Ok(self.main_sorter.insert(HEADERS_KEY, headers)?)
    }

    fn write_document(
        &mut self,
        document_id: DocumentId,
        words_positions: &HashMap<String, SmallVec32<Position>>,
        record: &StringRecord,
    ) -> anyhow::Result<()>
    {
        // We compute the list of words pairs proximities (self-join) and write it directly to disk.
        let words_pair_proximities = compute_words_pair_proximities(&words_positions);
        self.insert_words_pairs_proximities_docids(words_pair_proximities, document_id)?;

        // We store document_id associated with all the words the record contains.
        for (word, _) in words_positions {
            self.insert_word_docid(word, document_id)?;
        }

        let record = CsvStringRecordCodec::bytes_encode(record)
            .with_context(|| format!("could not encode CSV record"))?;

        self.documents_ids.insert(document_id);
        self.documents_writer.insert(document_id.to_be_bytes(), record)?;
        Self::write_docid_word_positions(&mut self.docid_word_positions_writer, document_id, words_positions)?;

        Ok(())
    }

    fn write_words_pairs_proximities(
        sorter: &mut Sorter<MergeFn>,
        iter: impl IntoIterator<Item=((SmallVec32<u8>, SmallVec32<u8>, u8), RoaringBitmap)>,
    ) -> anyhow::Result<()>
    {
        let mut key = Vec::new();
        let mut buffer = Vec::new();

        for ((w1, w2, min_prox), docids) in iter {
            key.clear();
            key.extend_from_slice(w1.as_bytes());
            key.push(0);
            key.extend_from_slice(w2.as_bytes());
            // Storing the minimun proximity found between those words
            key.push(min_prox);
            // We serialize the document ids into a buffer
            buffer.clear();
            buffer.reserve(CboRoaringBitmapCodec::serialized_size(&docids));
            CboRoaringBitmapCodec::serialize_into(&docids, &mut buffer)?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                sorter.insert(&key, &buffer)?;
            }
        }

        Ok(())
    }

    fn write_docid_word_positions(
        writer: &mut Writer<File>,
        id: DocumentId,
        words_positions: &HashMap<String, SmallVec32<Position>>,
    ) -> anyhow::Result<()>
    {
        // We prefix the words by the document id.
        let mut key = id.to_be_bytes().to_vec();
        let base_size = key.len();

        // We order the words lexicographically, this way we avoid passing by a sorter.
        let words_positions = BTreeMap::from_iter(words_positions);

        for (word, positions) in words_positions {
            key.truncate(base_size);
            key.extend_from_slice(word.as_bytes());
            // We serialize the positions into a buffer.
            let positions = RoaringBitmap::from_iter(positions.iter().cloned());
            let bytes = BoRoaringBitmapCodec::bytes_encode(&positions)
                .with_context(|| "could not serialize positions")?;
            // that we write under the generated key into MTBL
            if lmdb_key_valid_size(&key) {
                writer.insert(&key, &bytes)?;
            }
        }

        Ok(())
    }

    fn write_word_docids<I>(sorter: &mut Sorter<MergeFn>, iter: I) -> anyhow::Result<()>
    where I: IntoIterator<Item=(SmallVec32<u8>, RoaringBitmap)>
    {
        let mut key = Vec::new();
        let mut buffer = Vec::new();

        for (word, ids) in iter {
            key.clear();
            key.extend_from_slice(&word);
            // We serialize the document ids into a buffer
            buffer.clear();
            let ids = RoaringBitmap::from_iter(ids);
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
        log_every_n: usize,
    ) -> anyhow::Result<Readers>
    {
        debug!("{:?}: Indexing in a Store...", thread_index);

        // Write the headers into the store.
        let headers = rdr.headers()?;
        self.write_headers(&headers)?;

        let mut before = Instant::now();
        let mut document_id: usize = 0;
        let mut document = csv::StringRecord::new();
        let mut words_positions = HashMap::new();

        while rdr.read_record(&mut document)? {
            // We skip documents that must not be indexed by this thread.
            if document_id % num_threads == thread_index {
                // This is a log routine that we do every `log_every_n` documents.
                if document_id % log_every_n == 0 {
                    let count = format_count(document_id);
                    info!("We have seen {} documents so far ({:.02?}).", count, before.elapsed());
                    before = Instant::now();
                }

                let document_id = DocumentId::try_from(document_id).context("generated id is too big")?;
                for (attr, content) in document.iter().enumerate().take(MAX_ATTRIBUTES) {
                    for (pos, token) in simple_tokenizer(&content).filter_map(only_token).enumerate().take(MAX_POSITION) {
                        let word = token.to_lowercase();
                        let position = (attr * MAX_POSITION + pos) as u32;
                        words_positions.entry(word).or_insert_with(SmallVec32::new).push(position);
                    }
                }

                // We write the document in the documents store.
                self.write_document(document_id, &words_positions, &document)?;
                words_positions.clear();
            }

            // Compute the document id of the next document.
            document_id = document_id + 1;
        }

        let readers = self.finish()?;
        debug!("{:?}: Store created!", thread_index);
        Ok(readers)
    }

    fn finish(mut self) -> anyhow::Result<Readers> {
        let comp_type = self.chunk_compression_type;
        let comp_level = self.chunk_compression_level;
        let shrink_size = self.chunk_fusing_shrink_size;

        Self::write_word_docids(&mut self.word_docids_sorter, self.word_docids)?;
        Self::write_documents_ids(&mut self.main_sorter, self.documents_ids)?;
        Self::write_words_pairs_proximities(
            &mut self.words_pairs_proximities_docids_sorter,
            self.words_pairs_proximities_docids,
        )?;

        let mut word_docids_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        let mut builder = fst::SetBuilder::memory();

        let mut iter = self.word_docids_sorter.into_iter()?;
        while let Some((word, val)) = iter.next()? {
            // This is a lexicographically ordered word position
            // we use the key to construct the words fst.
            builder.insert(word)?;
            word_docids_wtr.insert(word, val)?;
        }

        let fst = builder.into_set();
        self.main_sorter.insert(WORDS_FST_KEY, fst.as_fst().as_bytes())?;

        let mut main_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.main_sorter.write_into(&mut main_wtr)?;

        let mut words_pairs_proximities_docids_wtr = tempfile().and_then(|f| create_writer(comp_type, comp_level, f))?;
        self.words_pairs_proximities_docids_sorter.write_into(&mut words_pairs_proximities_docids_wtr)?;

        let main = writer_into_reader(main_wtr, shrink_size)?;
        let word_docids = writer_into_reader(word_docids_wtr, shrink_size)?;
        let words_pairs_proximities_docids = writer_into_reader(words_pairs_proximities_docids_wtr, shrink_size)?;
        let docid_word_positions = writer_into_reader(self.docid_word_positions_writer, shrink_size)?;
        let documents = writer_into_reader(self.documents_writer, shrink_size)?;

        Ok(Readers {
            main,
            word_docids,
            docid_word_positions,
            words_pairs_proximities_docids,
            documents,
        })
    }
}

fn main_merge(key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
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
        HEADERS_KEY => {
            assert!(values.windows(2).all(|vs| vs[0] == vs[1]));
            Ok(values[0].to_vec())
        },
        DOCUMENTS_IDS_KEY => word_docids_merge(&[], values),
        otherwise => panic!("wut {:?}", otherwise),
    }
}

fn word_docids_merge(_key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = RoaringBitmap::deserialize_from(head.as_slice()).unwrap();

    for value in tail {
        let bitmap = RoaringBitmap::deserialize_from(value.as_slice()).unwrap();
        head.union_with(&bitmap);
    }

    let mut vec = Vec::with_capacity(head.serialized_size());
    head.serialize_into(&mut vec).unwrap();
    Ok(vec)
}

fn docid_word_positions_merge(key: &[u8], _values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    panic!("merging docid word positions is an error ({:?})", key.as_bstr())
}

fn words_pairs_proximities_docids_merge(_key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    let (head, tail) = values.split_first().unwrap();
    let mut head = CboRoaringBitmapCodec::deserialize_from(head.as_slice()).unwrap();

    for value in tail {
        let bitmap = CboRoaringBitmapCodec::deserialize_from(value.as_slice()).unwrap();
        head.union_with(&bitmap);
    }

    let mut vec = Vec::new();
    CboRoaringBitmapCodec::serialize_into(&head, &mut vec).unwrap();
    Ok(vec)
}

fn documents_merge(key: &[u8], _values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
    panic!("merging documents is an error ({:?})", key.as_bstr())
}

fn merge_readers(sources: Vec<Reader<FileFuse>>, merge: MergeFn) -> Merger<FileFuse, MergeFn> {
    let mut builder = Merger::builder(merge);
    builder.extend(sources);
    builder.build()
}

fn merge_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    sources: Vec<Reader<FileFuse>>,
    merge: MergeFn,
) -> anyhow::Result<()> {
    debug!("Merging {} MTBL stores...", sources.len());
    let before = Instant::now();

    let merger = merge_readers(sources, merge);
    let mut in_iter = merger.into_merge_iter()?;

    let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
    while let Some((k, v)) = in_iter.next()? {
        out_iter.append(k, v).with_context(|| format!("writing {:?} into LMDB", k.as_bstr()))?;
    }

    debug!("MTBL stores merged in {:.02?}!", before.elapsed());
    Ok(())
}

fn write_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    mut reader: Reader<FileFuse>,
) -> anyhow::Result<()> {
    debug!("Writing MTBL stores...");
    let before = Instant::now();

    let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
    while let Some((k, v)) = reader.next()? {
        out_iter.append(k, v).with_context(|| format!("writing {:?} into LMDB", k.as_bstr()))?;
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

    if opt.database.exists() {
        bail!("Database ({}) already exists, delete it to continue.", opt.database.display());
    }

    std::fs::create_dir_all(&opt.database)?;
    let env = EnvOpenOptions::new()
        .map_size(opt.database_size)
        .max_dbs(10)
        .open(&opt.database)?;

    let before_indexing = Instant::now();
    let index = Index::new(&env)?;

    let num_threads = rayon::current_num_threads();
    let linked_hash_map_size = opt.indexer.linked_hash_map_size;
    let max_nb_chunks = opt.indexer.max_nb_chunks;
    let max_memory_by_job = opt.indexer.max_memory / num_threads;
    let chunk_compression_type = opt.indexer.chunk_compression_type;
    let chunk_compression_level = opt.indexer.chunk_compression_level;
    let log_every_n = opt.indexer.log_every_n;

    let chunk_fusing_shrink_size = if opt.indexer.enable_chunk_fusing {
        Some(opt.indexer.chunk_fusing_shrink_size)
    } else {
        None
    };

    let readers = csv_readers(opt.csv_file, num_threads)?
        .into_par_iter()
        .enumerate()
        .map(|(i, rdr)| {
            let store = Store::new(
                linked_hash_map_size,
                max_nb_chunks,
                Some(max_memory_by_job),
                chunk_compression_type,
                chunk_compression_level,
                chunk_fusing_shrink_size,
            )?;
            store.index_csv(rdr, i, num_threads, log_every_n)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut main_readers = Vec::with_capacity(readers.len());
    let mut word_docids_readers = Vec::with_capacity(readers.len());
    let mut docid_word_positions_readers = Vec::with_capacity(readers.len());
    let mut words_pairs_proximities_docids_readers = Vec::with_capacity(readers.len());
    let mut documents_readers = Vec::with_capacity(readers.len());
    readers.into_iter().for_each(|readers| {
        main_readers.push(readers.main);
        word_docids_readers.push(readers.word_docids);
        docid_word_positions_readers.push(readers.docid_word_positions);
        words_pairs_proximities_docids_readers.push(readers.words_pairs_proximities_docids);
        documents_readers.push(readers.documents);
    });

    // This is the function that merge the readers
    // by using the given merge function.
    let merge_readers = move |readers, merge| {
        let mut writer = tempfile().and_then(|f| {
            create_writer(chunk_compression_type, chunk_compression_level, f)
        })?;
        let merger = merge_readers(readers, merge);
        merger.write_into(&mut writer)?;
        writer_into_reader(writer, chunk_fusing_shrink_size)
    };

    // The enum and the channel which is used to transfert
    // the readers merges potentially done on another thread.
    enum DatabaseType { Main, WordDocids, WordsPairsProximitiesDocids };
    let (sender, receiver) = sync_channel(3);

    debug!("Merging the main, word docids and words pairs proximity docids in parallel...");
    rayon::spawn(move || {
        vec![
            (DatabaseType::Main, main_readers, main_merge as MergeFn),
            (DatabaseType::WordDocids, word_docids_readers, word_docids_merge),
            (
                DatabaseType::WordsPairsProximitiesDocids,
                words_pairs_proximities_docids_readers,
                words_pairs_proximities_docids_merge,
            ),
        ]
        .into_par_iter()
        .for_each(|(dbtype, readers, merge)| {
            let result = merge_readers(readers, merge);
            sender.send((dbtype, result)).unwrap();
        });
    });

    let mut wtxn = env.write_txn()?;

    debug!("Writing the docid word positions into LMDB on disk...");
    merge_into_lmdb_database(
        &mut wtxn,
        *index.docid_word_positions.as_polymorph(),
        docid_word_positions_readers,
        docid_word_positions_merge,
    )?;

    debug!("Writing the documents into LMDB on disk...");
    merge_into_lmdb_database(
        &mut wtxn,
        *index.documents.as_polymorph(),
        documents_readers,
        documents_merge,
    )?;

    for (db_type, result) in receiver {
        let content = result?;
        match db_type {
            DatabaseType::Main => {
                debug!("Writing the main elements into LMDB on disk...");
                write_into_lmdb_database(&mut wtxn, index.main, content)?;
            },
            DatabaseType::WordDocids => {
                debug!("Writing the words docids into LMDB on disk...");
                let db = *index.word_docids.as_polymorph();
                write_into_lmdb_database(&mut wtxn, db, content)?;
            },
            DatabaseType::WordsPairsProximitiesDocids => {
                debug!("Writing the words pairs proximities docids into LMDB on disk...");
                let db = *index.word_pair_proximity_docids.as_polymorph();
                write_into_lmdb_database(&mut wtxn, db, content)?;
            },
        }
    }

    debug!("Retrieving the number of documents...");
    let count = index.number_of_documents(&wtxn)?;

    wtxn.commit()?;

    info!("Wrote {} documents in {:.02?}", count, before_indexing.elapsed());

    Ok(())
}
