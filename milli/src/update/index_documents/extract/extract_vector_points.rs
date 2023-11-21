use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::mem::size_of;
use std::str::from_utf8;

use bytemuck::cast_slice;
use grenad::Writer;
use itertools::EitherOrBoth;
use ordered_float::OrderedFloat;
use serde_json::{from_slice, Value};

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::UserError;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::helpers::try_split_at;
use crate::{DocumentId, FieldId, InternalError, Result, VectorOrArrayOfVectors};

/// The length of the elements that are always in the buffer when inserting new values.
const TRUNCATE_SIZE: usize = size_of::<DocumentId>();

/// Extracts the embedding vector contained in each document under the `_vectors` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the Vec<f32>
#[logging_timer::time]
pub fn extract_vector_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    vectors_fid: FieldId,
) -> Result<grenad::Reader<BufReader<File>>> {
    puffin::profile_function!();

    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut key_buffer = Vec::new();
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        // this must always be serialized as (docid, external_docid);
        let (docid_bytes, external_id_bytes) =
            try_split_at(key, std::mem::size_of::<DocumentId>()).unwrap();
        debug_assert!(from_utf8(external_id_bytes).is_ok());

        let obkv = obkv::KvReader::new(value);
        key_buffer.clear();
        key_buffer.extend_from_slice(docid_bytes);

        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value { from_utf8(external_id_bytes).unwrap().into() };

        // first we retrieve the _vectors field
        if let Some(value) = obkv.get(vectors_fid) {
            let vectors_obkv = KvReaderDelAdd::new(value);

            // then we extract the values
            let del_vectors = vectors_obkv
                .get(DelAdd::Deletion)
                .map(|vectors| extract_vectors(vectors, document_id))
                .transpose()?
                .flatten();
            let add_vectors = vectors_obkv
                .get(DelAdd::Addition)
                .map(|vectors| extract_vectors(vectors, document_id))
                .transpose()?
                .flatten();

            // and we finally push the unique vectors into the writer
            push_vectors_diff(
                &mut writer,
                &mut key_buffer,
                del_vectors.unwrap_or_default(),
                add_vectors.unwrap_or_default(),
            )?;
        }
    }

    writer_into_reader(writer)
}

/// Computes the diff between both Del and Add numbers and
/// only inserts the parts that differ in the sorter.
fn push_vectors_diff(
    writer: &mut Writer<BufWriter<File>>,
    key_buffer: &mut Vec<u8>,
    mut del_vectors: Vec<Vec<f32>>,
    mut add_vectors: Vec<Vec<f32>>,
) -> Result<()> {
    // We sort and dedup the vectors
    del_vectors.sort_unstable_by(|a, b| compare_vectors(a, b));
    add_vectors.sort_unstable_by(|a, b| compare_vectors(a, b));
    del_vectors.dedup_by(|a, b| compare_vectors(a, b).is_eq());
    add_vectors.dedup_by(|a, b| compare_vectors(a, b).is_eq());

    let merged_vectors_iter =
        itertools::merge_join_by(del_vectors, add_vectors, |del, add| compare_vectors(del, add));

    // insert vectors into the writer
    for (i, eob) in merged_vectors_iter.into_iter().enumerate().take(u16::MAX as usize) {
        // Generate the key by extending the unique index to it.
        key_buffer.truncate(TRUNCATE_SIZE);
        let index = u16::try_from(i).unwrap();
        key_buffer.extend_from_slice(&index.to_be_bytes());

        match eob {
            EitherOrBoth::Both(_, _) => (), // no need to touch anything
            EitherOrBoth::Left(vector) => {
                // We insert only the Del part of the Obkv to inform
                // that we only want to remove all those vectors.
                let mut obkv = KvWriterDelAdd::memory();
                obkv.insert(DelAdd::Deletion, cast_slice(&vector))?;
                let bytes = obkv.into_inner()?;
                writer.insert(&key_buffer, bytes)?;
            }
            EitherOrBoth::Right(vector) => {
                // We insert only the Add part of the Obkv to inform
                // that we only want to remove all those vectors.
                let mut obkv = KvWriterDelAdd::memory();
                obkv.insert(DelAdd::Addition, cast_slice(&vector))?;
                let bytes = obkv.into_inner()?;
                writer.insert(&key_buffer, bytes)?;
            }
        }
    }

    Ok(())
}

/// Compares two vectors by using the OrderingFloat helper.
fn compare_vectors(a: &[f32], b: &[f32]) -> Ordering {
    a.iter().copied().map(OrderedFloat).cmp(b.iter().copied().map(OrderedFloat))
}

/// Extracts the vectors from a JSON value.
fn extract_vectors(value: &[u8], document_id: impl Fn() -> Value) -> Result<Option<Vec<Vec<f32>>>> {
    match from_slice(value) {
        Ok(vectors) => Ok(VectorOrArrayOfVectors::into_array_of_vectors(vectors)),
        Err(_) => Err(UserError::InvalidVectorsType {
            document_id: document_id(),
            value: from_slice(value).map_err(InternalError::SerdeJson)?,
        }
        .into()),
    }
}
