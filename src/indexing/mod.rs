use std::borrow::Cow;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::mpsc::sync_channel;
use std::time::Instant;

use anyhow::Context;
use bstr::ByteSlice as _;
use flate2::read::GzDecoder;
use grenad::{Writer, Sorter, Merger, Reader, FileFuse, CompressionType};
use heed::types::ByteSlice;
use log::{debug, info, error};
use rayon::prelude::*;
use structopt::StructOpt;
use tempfile::tempfile;

use crate::Index;
use self::store::Store;
use self::merge_function::{
    main_merge, word_docids_merge, words_pairs_proximities_docids_merge,
    docid_word_positions_merge, documents_merge,
};

mod store;
mod merge_function;

#[derive(Debug, Clone, StructOpt)]
pub struct IndexerOpt {
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

    /// Number of parallel jobs for indexing, defaults to # of CPUs.
    #[structopt(long)]
    indexing_jobs: Option<usize>,
}

#[derive(Debug, Copy, Clone)]
enum WriteMethod {
    Append,
    GetMergePut,
}

type MergeFn = for<'a> fn(&[u8], &[Cow<'a, [u8]>]) -> anyhow::Result<Vec<u8>>;

fn create_writer(typ: CompressionType, level: Option<u32>, file: File) -> io::Result<Writer<File>> {
    let mut builder = Writer::builder();
    builder.compression_type(typ);
    if let Some(level) = level {
        builder.compression_level(level);
    }
    builder.build(file)
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
    method: WriteMethod,
) -> anyhow::Result<()> {
    debug!("Merging {} MTBL stores...", sources.len());
    let before = Instant::now();

    let merger = merge_readers(sources, merge);
    let mut in_iter = merger.into_merge_iter()?;

    match method {
        WriteMethod::Append => {
            let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
            while let Some((k, v)) = in_iter.next()? {
                out_iter.append(k, v).with_context(|| format!("writing {:?} into LMDB", k.as_bstr()))?;
            }
        },
        WriteMethod::GetMergePut => {
            while let Some((k, v)) = in_iter.next()? {
                match database.get::<_, ByteSlice, ByteSlice>(wtxn, k)? {
                    Some(old_val) => {
                        let vals = vec![Cow::Borrowed(old_val), Cow::Borrowed(v)];
                        let val = merge(k, &vals).expect("merge failed");
                        database.put::<_, ByteSlice, ByteSlice>(wtxn, k, &val)?
                    },
                    None => database.put::<_, ByteSlice, ByteSlice>(wtxn, k, v)?,
                }
            }
        },
    }

    debug!("MTBL stores merged in {:.02?}!", before.elapsed());
    Ok(())
}

fn write_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    mut reader: Reader<FileFuse>,
    merge: MergeFn,
    method: WriteMethod,
) -> anyhow::Result<()> {
    debug!("Writing MTBL stores...");
    let before = Instant::now();

    match method {
        WriteMethod::Append => {
            let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
            while let Some((k, v)) = reader.next()? {
                out_iter.append(k, v).with_context(|| format!("writing {:?} into LMDB", k.as_bstr()))?;
            }
        },
        WriteMethod::GetMergePut => {
            while let Some((k, v)) = reader.next()? {
                match database.get::<_, ByteSlice, ByteSlice>(wtxn, k)? {
                    Some(old_val) => {
                        let vals = vec![Cow::Borrowed(old_val), Cow::Borrowed(v)];
                        let val = merge(k, &vals).expect("merge failed");
                        database.put::<_, ByteSlice, ByteSlice>(wtxn, k, &val)?
                    },
                    None => database.put::<_, ByteSlice, ByteSlice>(wtxn, k, v)?,
                }
            }
        }
    }

    debug!("MTBL stores merged in {:.02?}!", before.elapsed());
    Ok(())
}

fn csv_bytes_readers<'a>(
    content: &'a [u8],
    gzipped: bool,
    count: usize,
) -> Vec<csv::Reader<Box<dyn Read + Send + 'a>>>
{
    let mut readers = Vec::new();

    for _ in 0..count {
        let content = if gzipped {
            Box::new(GzDecoder::new(content)) as Box<dyn Read + Send>
        } else {
            Box::new(content) as Box<dyn Read + Send>
        };
        let reader = csv::Reader::from_reader(content);
        readers.push(reader);
    }

    readers
}

pub fn run<'a>(
    env: &heed::Env,
    index: &Index,
    opt: &IndexerOpt,
    content: &'a [u8],
    gzipped: bool,
) -> anyhow::Result<()>
{
    let jobs = opt.indexing_jobs.unwrap_or(0);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(jobs).build()?;
    pool.install(|| run_intern(env, index, opt, content, gzipped))
}

fn run_intern<'a>(
    env: &heed::Env,
    index: &Index,
    opt: &IndexerOpt,
    content: &'a [u8],
    gzipped: bool,
) -> anyhow::Result<()>
{
    let before_indexing = Instant::now();
    let num_threads = rayon::current_num_threads();
    let linked_hash_map_size = opt.linked_hash_map_size;
    let max_nb_chunks = opt.max_nb_chunks;
    let max_memory_by_job = opt.max_memory / num_threads;
    let chunk_compression_type = opt.chunk_compression_type;
    let chunk_compression_level = opt.chunk_compression_level;
    let log_every_n = opt.log_every_n;

    let chunk_fusing_shrink_size = if opt.enable_chunk_fusing {
        Some(opt.chunk_fusing_shrink_size)
    } else {
        None
    };

    let rtxn = env.read_txn()?;
    let number_of_documents = index.number_of_documents(&rtxn)?;
    drop(rtxn);

    let readers = csv_bytes_readers(content, gzipped, num_threads)
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
            let base_document_id = number_of_documents;
            store.index_csv(rdr, base_document_id, i, num_threads, log_every_n)
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
            if let Err(e) = sender.send((dbtype, result)) {
                error!("sender error: {}", e);
            }
        });
    });

    let mut wtxn = env.write_txn()?;

    let contains_documents = number_of_documents != 0;
    let write_method = if contains_documents { WriteMethod::GetMergePut } else { WriteMethod::Append };

    debug!("Writing the docid word positions into LMDB on disk...");
    merge_into_lmdb_database(
        &mut wtxn,
        *index.docid_word_positions.as_polymorph(),
        docid_word_positions_readers,
        docid_word_positions_merge,
        write_method
    )?;

    debug!("Writing the documents into LMDB on disk...");
    merge_into_lmdb_database(
        &mut wtxn,
        *index.documents.as_polymorph(),
        documents_readers,
        documents_merge,
        write_method
    )?;

    for (db_type, result) in receiver {
        let content = result?;
        match db_type {
            DatabaseType::Main => {
                debug!("Writing the main elements into LMDB on disk...");
                write_into_lmdb_database(&mut wtxn, index.main, content, main_merge, write_method)?;
            },
            DatabaseType::WordDocids => {
                debug!("Writing the words docids into LMDB on disk...");
                let db = *index.word_docids.as_polymorph();
                write_into_lmdb_database(&mut wtxn, db, content, word_docids_merge, write_method)?;
            },
            DatabaseType::WordsPairsProximitiesDocids => {
                debug!("Writing the words pairs proximities docids into LMDB on disk...");
                let db = *index.word_pair_proximity_docids.as_polymorph();
                write_into_lmdb_database(
                    &mut wtxn,
                    db,
                    content,
                    words_pairs_proximities_docids_merge,
                    write_method,
                )?;
            },
        }
    }

    wtxn.commit()?;

    info!("Update processed in {:.02?}", before_indexing.elapsed());

    Ok(())
}
