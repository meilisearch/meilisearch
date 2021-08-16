use std::borrow::Cow;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::time::Instant;

use byte_unit::Byte;
use grenad::{CompressionType, MergerIter, Reader, Sorter};
use heed::types::ByteSlice;
use log::debug;

use super::{ClonableMmap, MergeFn};
use crate::error::InternalError;
use crate::update::index_documents::WriteMethod;
use crate::Result;

pub type CursorClonableMmap = io::Cursor<ClonableMmap>;

pub fn create_writer<R: io::Write>(
    typ: grenad::CompressionType,
    level: Option<u32>,
    file: R,
) -> io::Result<grenad::Writer<R>> {
    let mut builder = grenad::Writer::builder();
    builder.compression_type(typ);
    if let Some(level) = level {
        builder.compression_level(level);
    }
    builder.build(file)
}

pub fn create_sorter(
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
    builder.build()
}

pub fn sorter_into_reader(
    sorter: grenad::Sorter<MergeFn>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(indexer.chunk_compression_type, indexer.chunk_compression_level, file)
    })?;
    sorter.write_into(&mut writer)?;
    Ok(writer_into_reader(writer)?)
}

pub fn writer_into_reader(writer: grenad::Writer<File>) -> Result<grenad::Reader<File>> {
    let mut file = writer.into_inner()?;
    file.seek(SeekFrom::Start(0))?;
    grenad::Reader::new(file).map_err(Into::into)
}

pub unsafe fn into_clonable_grenad(
    reader: grenad::Reader<File>,
) -> Result<grenad::Reader<CursorClonableMmap>> {
    let file = reader.into_inner();
    let mmap = memmap::Mmap::map(&file)?;
    let cursor = io::Cursor::new(ClonableMmap::from(mmap));
    let reader = grenad::Reader::new(cursor)?;
    Ok(reader)
}

pub fn merge_readers<R: io::Read>(
    readers: Vec<grenad::Reader<R>>,
    merge_fn: MergeFn,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let mut merger_builder = grenad::MergerBuilder::new(merge_fn);
    merger_builder.extend(readers);
    let merger = merger_builder.build();
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(indexer.chunk_compression_type, indexer.chunk_compression_level, file)
    })?;
    merger.write_into(&mut writer)?;
    let reader = writer_into_reader(writer)?;
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
pub fn grenad_obkv_into_chunks<R: io::Read>(
    mut reader: grenad::Reader<R>,
    indexer: GrenadParameters,
    log_frequency: Option<usize>,
    documents_chunk_size: Byte,
) -> Result<impl Iterator<Item = Result<grenad::Reader<File>>>> {
    let mut document_count = 0;
    let mut continue_reading = true;

    let indexer_clone = indexer.clone();
    let mut transposer = move || {
        if !continue_reading {
            return Ok(None);
        }

        let mut current_chunk_size = 0u64;
        let mut obkv_documents = tempfile::tempfile().and_then(|file| {
            create_writer(
                indexer_clone.chunk_compression_type,
                indexer_clone.chunk_compression_level,
                file,
            )
        })?;

        while let Some((document_id, obkv)) = reader.next()? {
            obkv_documents.insert(document_id, obkv)?;
            current_chunk_size += document_id.len() as u64 + obkv.len() as u64;

            document_count += 1;
            if log_frequency.map_or(false, |log_frequency| document_count % log_frequency == 0) {
                debug!("reached {} chunked documents", document_count);
            }

            if current_chunk_size >= documents_chunk_size.get_bytes() {
                return writer_into_reader(obkv_documents).map(Some);
            }
        }

        continue_reading = false;
        writer_into_reader(obkv_documents).map(Some)
    };

    Ok(std::iter::from_fn(move || {
        let result = transposer().transpose();
        if result.as_ref().map_or(false, |r| r.is_ok()) {
            debug!(
                "A new chunk of approximately {} has been generated",
                documents_chunk_size.get_appropriate_unit(true),
            );
        }
        result
    }))
}

pub fn write_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    mut reader: Reader<File>,
    merge: MergeFn,
    method: WriteMethod,
) -> Result<()> {
    debug!("Writing MTBL stores...");
    let before = Instant::now();

    match method {
        WriteMethod::Append => {
            let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
            while let Some((k, v)) = reader.next()? {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { out_iter.append(k, v)? };
            }
        }
        WriteMethod::GetMergePut => {
            while let Some((k, v)) = reader.next()? {
                let mut iter = database.prefix_iter_mut::<_, ByteSlice, ByteSlice>(wtxn, k)?;
                match iter.next().transpose()? {
                    Some((key, old_val)) if key == k => {
                        let vals = &[Cow::Borrowed(old_val), Cow::Borrowed(v)][..];
                        let val = merge(k, &vals)?;
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
    }

    debug!("MTBL stores merged in {:.02?}!", before.elapsed());
    Ok(())
}

pub fn sorter_into_lmdb_database(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    sorter: Sorter<MergeFn>,
    merge: MergeFn,
    method: WriteMethod,
) -> Result<()> {
    debug!("Writing MTBL sorter...");
    let before = Instant::now();

    merger_iter_into_lmdb_database(wtxn, database, sorter.into_merger_iter()?, merge, method)?;

    debug!("MTBL sorter writen in {:.02?}!", before.elapsed());
    Ok(())
}

fn merger_iter_into_lmdb_database<R: io::Read>(
    wtxn: &mut heed::RwTxn,
    database: heed::PolyDatabase,
    mut sorter: MergerIter<R, MergeFn>,
    merge: MergeFn,
    method: WriteMethod,
) -> Result<()> {
    match method {
        WriteMethod::Append => {
            let mut out_iter = database.iter_mut::<_, ByteSlice, ByteSlice>(wtxn)?;
            while let Some((k, v)) = sorter.next()? {
                // safety: we don't keep references from inside the LMDB database.
                unsafe { out_iter.append(k, v)? };
            }
        }
        WriteMethod::GetMergePut => {
            while let Some((k, v)) = sorter.next()? {
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
    }

    Ok(())
}
