use std::borrow::Cow;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::time::Instant;

use grenad::{CompressionType, Sorter};
use heed::types::ByteSlice;
use log::debug;

use super::{ClonableMmap, MergeFn};
use crate::error::InternalError;
use crate::Result;

pub type CursorClonableMmap = io::Cursor<ClonableMmap>;

pub fn create_writer<R: io::Write>(
    typ: grenad::CompressionType,
    level: Option<u32>,
    file: R,
) -> grenad::Writer<R> {
    let mut builder = grenad::Writer::builder();
    builder.compression_type(typ);
    if let Some(level) = level {
        builder.compression_level(level);
    }
    builder.build(file)
}

pub fn create_sorter(
    sort_algorithm: grenad::SortAlgorithm,
    merge: MergeFn,
    chunk_compression_type: grenad::CompressionType,
    chunk_compression_level: Option<u32>,
    max_nb_chunks: Option<usize>,
    max_memory: Option<usize>,
) -> grenad::Sorter<MergeFn> {
    let mut builder = grenad::Sorter::builder(merge);
    builder.chunk_compression_type(chunk_compression_type);
    if let Some(level) = chunk_compression_level {
        builder.chunk_compression_level(level);
    }
    if let Some(nb_chunks) = max_nb_chunks {
        builder.max_nb_chunks(nb_chunks);
    }
    if let Some(memory) = max_memory {
        builder.dump_threshold(memory);
        builder.allow_realloc(false);
    }
    builder.sort_algorithm(sort_algorithm);
    builder.build()
}

pub fn sorter_into_reader(
    sorter: grenad::Sorter<MergeFn>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );
    sorter.write_into_stream_writer(&mut writer)?;

    writer_into_reader(writer)
}

pub fn writer_into_reader(writer: grenad::Writer<File>) -> Result<grenad::Reader<File>> {
    let mut file = writer.into_inner()?;
    file.seek(SeekFrom::Start(0))?;
    grenad::Reader::new(file).map_err(Into::into)
}

pub unsafe fn as_cloneable_grenad(
    reader: &grenad::Reader<File>,
) -> Result<grenad::Reader<CursorClonableMmap>> {
    let file = reader.get_ref();
    let mmap = memmap2::Mmap::map(file)?;
    let cursor = io::Cursor::new(ClonableMmap::from(mmap));
    let reader = grenad::Reader::new(cursor)?;
    Ok(reader)
}

pub trait MergeableReader
where
    Self: Sized,
{
    type Output;

    fn merge(self, merge_fn: MergeFn, indexer: &GrenadParameters) -> Result<Self::Output>;
}

impl MergeableReader for Vec<grenad::Reader<File>> {
    type Output = grenad::Reader<File>;

    fn merge(self, merge_fn: MergeFn, params: &GrenadParameters) -> Result<Self::Output> {
        let mut merger = MergerBuilder::new(merge_fn);
        self.into_iter().try_for_each(|r| merger.push(r))?;
        merger.finish(params)
    }
}

impl MergeableReader for Vec<(grenad::Reader<File>, grenad::Reader<File>)> {
    type Output = (grenad::Reader<File>, grenad::Reader<File>);

    fn merge(self, merge_fn: MergeFn, params: &GrenadParameters) -> Result<Self::Output> {
        let mut m1 = MergerBuilder::new(merge_fn);
        let mut m2 = MergerBuilder::new(merge_fn);
        for (r1, r2) in self.into_iter() {
            m1.push(r1)?;
            m2.push(r2)?;
        }
        Ok((m1.finish(params)?, m2.finish(params)?))
    }
}

struct MergerBuilder<R>(grenad::MergerBuilder<R, MergeFn>);

impl<R: io::Read + io::Seek> MergerBuilder<R> {
    fn new(merge_fn: MergeFn) -> Self {
        Self(grenad::MergerBuilder::new(merge_fn))
    }

    fn push(&mut self, reader: grenad::Reader<R>) -> Result<()> {
        self.0.push(reader.into_cursor()?);
        Ok(())
    }

    fn finish(self, params: &GrenadParameters) -> Result<grenad::Reader<File>> {
        let merger = self.0.build();
        let mut writer = create_writer(
            params.chunk_compression_type,
            params.chunk_compression_level,
            tempfile::tempfile()?,
        );
        merger.write_into_stream_writer(&mut writer)?;

        writer_into_reader(writer)
    }
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
    /// This should be called inside of a rayon thread pool,
    /// Otherwise, it will take the global number of threads.
    pub fn max_memory_by_thread(&self) -> Option<usize> {
        self.max_memory.map(|max_memory| max_memory / rayon::current_num_threads())
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
) -> Result<impl Iterator<Item = Result<grenad::Reader<File>>>> {
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
            obkv_documents.insert(document_id, obkv)?;
            current_chunk_size += document_id.len() as u64 + obkv.len() as u64;

            if current_chunk_size >= documents_chunk_size as u64 {
                return writer_into_reader(obkv_documents).map(Some);
            }
        }

        continue_reading = false;
        writer_into_reader(obkv_documents).map(Some)
    };

    Ok(std::iter::from_fn(move || transposer().transpose()))
}

pub fn sorter_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    sorter: Sorter<MergeFn>,
    merge: MergeFn,
) -> Result<()> {
    debug!("Writing MTBL sorter...");
    let before = Instant::now();

    let mut merger_iter = sorter.into_stream_merger_iter()?;
    if database.is_empty(wtxn)? {
        let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
        while let Some((k, v)) = merger_iter.next()? {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { out_iter.append(k, v)? };
        }
    } else {
        while let Some((k, v)) = merger_iter.next()? {
            let mut iter = database.prefix_iter_mut::<_, ByteSlice, ByteSlice>(wtxn, k)?;
            match iter.next().transpose()? {
                Some((key, old_val)) if key == k => {
                    let vals = vec![Cow::Borrowed(old_val), Cow::Borrowed(v)];
                    let val = merge(k, &vals).map_err(|_| {
                        // TODO just wrap this error?
                        InternalError::IndexingMergingKeys { process: "get-put-merge" }
                    })?;
                    // safety: we don't keep references from inside the LMDB database.
                    unsafe { iter.put_current(k, &val)? };
                }
                _ => {
                    drop(iter);
                    database.put::<_, ByteSlice, ByteSlice>(wtxn, k, v)?;
                }
            }
        }
    }

    debug!("MTBL sorter writen in {:.02?}!", before.elapsed());
    Ok(())
}

/// Used when trying to merge readers, but you don't actually care about the values.
pub fn merge_ignore_values<'a>(_key: &[u8], _values: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>> {
    Ok(Cow::Owned(Vec::new()))
}
