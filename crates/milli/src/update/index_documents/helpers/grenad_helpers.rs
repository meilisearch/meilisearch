use std::fs::File;
use std::io::{self, BufReader, BufWriter, Seek};

use grenad::{CompressionType, MergeFunction, Sorter};
use heed::types::Bytes;

use super::ClonableMmap;
use crate::update::index_documents::valid_lmdb_key;
use crate::Result;

/// This is something reasonable given the fact
/// that there is one grenad sorter by thread.
const MAX_GRENAD_SORTER_USAGE: usize = 500 * 1024 * 1024; // 500 MiB

pub type CursorClonableMmap = io::Cursor<ClonableMmap>;

pub fn create_writer<R: io::Write>(
    typ: grenad::CompressionType,
    level: Option<u32>,
    file: R,
) -> grenad::Writer<BufWriter<R>> {
    let mut builder = grenad::Writer::builder();
    builder.compression_type(typ);
    if let Some(level) = level {
        builder.compression_level(level);
    }
    builder.build(BufWriter::new(file))
}

/// A helper function that creates a grenad sorter
/// with the given parameters. The max memory is
/// clamped to something reasonable.
pub fn create_sorter<MF: MergeFunction>(
    sort_algorithm: grenad::SortAlgorithm,
    merge: MF,
    chunk_compression_type: grenad::CompressionType,
    chunk_compression_level: Option<u32>,
    max_nb_chunks: Option<usize>,
    max_memory: Option<usize>,
    sort_in_parallel: bool,
) -> grenad::Sorter<MF> {
    let mut builder = grenad::Sorter::builder(merge);
    builder.chunk_compression_type(chunk_compression_type);
    if let Some(level) = chunk_compression_level {
        builder.chunk_compression_level(level);
    }
    if let Some(nb_chunks) = max_nb_chunks {
        builder.max_nb_chunks(nb_chunks);
    }
    if let Some(memory) = max_memory {
        builder.dump_threshold(memory.min(MAX_GRENAD_SORTER_USAGE));
        builder.allow_realloc(false);
    }
    builder.sort_algorithm(sort_algorithm);
    builder.sort_in_parallel(sort_in_parallel);
    builder.build()
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::grenad")]
pub fn sorter_into_reader<MF>(
    sorter: grenad::Sorter<MF>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<BufReader<File>>>
where
    MF: MergeFunction,
    crate::Error: From<MF::Error>,
{
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    sorter.write_into_stream_writer(&mut writer)?;

    writer_into_reader(writer)
}

pub fn writer_into_reader(
    writer: grenad::Writer<BufWriter<File>>,
) -> Result<grenad::Reader<BufReader<File>>> {
    let mut file = writer.into_inner()?.into_inner().map_err(|err| err.into_error())?;
    file.rewind()?;
    grenad::Reader::new(BufReader::new(file)).map_err(Into::into)
}

/// # Safety
/// We use memory mapping inside. So, according to the Rust community, it's unsafe.
pub unsafe fn as_cloneable_grenad(
    reader: &grenad::Reader<BufReader<File>>,
) -> Result<grenad::Reader<CursorClonableMmap>> {
    let file = reader.get_ref().get_ref();
    let mmap = memmap2::Mmap::map(file)?;
    let cursor = io::Cursor::new(ClonableMmap::from(mmap));
    let reader = grenad::Reader::new(cursor)?;
    Ok(reader)
}

#[derive(Debug, Clone, Copy)]
pub struct GrenadParameters {
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: Option<u32>,
    pub max_memory: Option<usize>,
    pub max_nb_chunks: Option<usize>,
}

impl Default for GrenadParameters {
    fn default() -> Self {
        Self {
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_memory: None,
            max_nb_chunks: None,
        }
    }
}

impl GrenadParameters {
    /// This function use the number of threads in the current threadpool to compute the value.
    ///
    /// This should be called inside of a rayon thread pool,
    /// otherwise, it will take the global number of threads.
    pub fn max_memory_by_thread(&self) -> Option<usize> {
        self.max_memory.map(|max_memory| (max_memory / rayon::current_num_threads()))
    }
}

/// Returns an iterator that outputs grenad readers of obkv documents
/// with a maximum size of approximately `documents_chunks_size`.
///
/// The grenad obkv entries are composed of an incremental document id big-endian
/// encoded as the key and an obkv object with an `u8` for the field as the key
/// and a simple UTF-8 encoded string as the value.
pub fn grenad_obkv_into_chunks<R: io::Read + io::Seek>(
    reader: grenad::Reader<R>,
    indexer: GrenadParameters,
    documents_chunk_size: usize,
) -> Result<impl Iterator<Item = Result<grenad::Reader<BufReader<File>>>>> {
    let mut continue_reading = true;
    let mut cursor = reader.into_cursor()?;

    let mut transposer = move || {
        if !continue_reading {
            return Ok(None);
        }

        let mut current_chunk_size = 0u64;
        let mut obkv_documents = create_writer(
            indexer.chunk_compression_type,
            indexer.chunk_compression_level,
            tempfile::tempfile()?,
        );

        while let Some((document_id, obkv)) = cursor.move_on_next()? {
            if !obkv.is_empty() {
                obkv_documents.insert(document_id, obkv)?;
                current_chunk_size += document_id.len() as u64 + obkv.len() as u64;

                if current_chunk_size >= documents_chunk_size as u64 {
                    return writer_into_reader(obkv_documents).map(Some);
                }
            }
        }

        continue_reading = false;
        writer_into_reader(obkv_documents).map(Some)
    };

    Ok(std::iter::from_fn(move || transposer().transpose()))
}

/// Write provided sorter in database using serialize_value function.
/// merge_values function is used if an entry already exist in the database.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::grenad")]
pub fn write_sorter_into_database<K, V, FS, FM, MF>(
    sorter: Sorter<MF>,
    database: &heed::Database<K, V>,
    wtxn: &mut heed::RwTxn<'_>,
    index_is_empty: bool,
    serialize_value: FS,
    merge_values: FM,
) -> Result<()>
where
    FS: for<'a> Fn(&'a [u8], &'a mut Vec<u8>) -> Result<&'a [u8]>,
    FM: for<'a> Fn(&[u8], &[u8], &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>,
    MF: MergeFunction,
    crate::Error: From<MF::Error>,
{
    let mut buffer = Vec::new();
    let database = database.remap_types::<Bytes, Bytes>();

    let mut merger_iter = sorter.into_stream_merger_iter()?;
    while let Some((key, value)) = merger_iter.next()? {
        if valid_lmdb_key(key) {
            buffer.clear();
            let value = if index_is_empty {
                Some(serialize_value(value, &mut buffer)?)
            } else {
                match database.get(wtxn, key)? {
                    Some(prev_value) => merge_values(value, prev_value, &mut buffer)?,
                    None => Some(serialize_value(value, &mut buffer)?),
                }
            };
            match value {
                Some(value) => database.put(wtxn, key, value)?,
                None => {
                    database.delete(wtxn, key)?;
                }
            }
        }
    }

    Ok(())
}
