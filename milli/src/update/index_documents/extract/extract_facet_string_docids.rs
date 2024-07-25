use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::iter::FromIterator;
use std::{io, str};

use charabia::normalizer::{Normalize, NormalizerOption};
use charabia::{Language, StrDetection, Token};
use heed::types::SerdeJson;
use heed::BytesEncode;

use super::helpers::{create_sorter, sorter_into_reader, try_split_array_at, GrenadParameters};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec};
use crate::heed_codec::{BEU16StrCodec, StrRefCodec};
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::helpers::{
    merge_deladd_btreeset_string, merge_deladd_cbo_roaring_bitmaps,
};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::{FieldId, Result, MAX_FACET_VALUE_LENGTH};

/// Extracts the facet string and the documents ids where this facet string appear.
///
/// Returns a grenad reader with the list of extracted facet strings and
/// documents ids from the given chunk of docid facet string positions.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_facet_string_docids<R: io::Read + io::Seek>(
    docid_fid_facet_string: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
) -> Result<(grenad::Reader<BufReader<File>>, grenad::Reader<BufReader<File>>)> {
    let max_memory = indexer.max_memory_by_thread();

    let mut facet_string_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );

    let mut normalized_facet_string_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        merge_deladd_btreeset_string,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );

    let mut buffer = Vec::new();
    let mut cursor = docid_fid_facet_string.into_cursor()?;
    while let Some((key, deladd_original_value_bytes)) = cursor.move_on_next()? {
        let deladd_reader = KvReaderDelAdd::new(deladd_original_value_bytes);

        let is_same_value = deladd_reader.get(DelAdd::Deletion).is_some()
            && deladd_reader.get(DelAdd::Addition).is_some();

        let (field_id_bytes, bytes) = try_split_array_at(key).unwrap();
        let field_id = FieldId::from_be_bytes(field_id_bytes);

        let (document_id_bytes, normalized_value_bytes) =
            try_split_array_at::<_, 4>(bytes).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);

        let normalized_value = str::from_utf8(normalized_value_bytes)?;

        // Facet search normalization
        {
            let locales = settings_diff.old.localized_faceted_fields_ids.locales(field_id);
            let old_hyper_normalized_value = normalize_facet_string(normalized_value, locales);
            let locales = settings_diff.new.localized_faceted_fields_ids.locales(field_id);
            let new_hyper_normalized_value = normalize_facet_string(normalized_value, locales);

            let set = BTreeSet::from_iter(std::iter::once(normalized_value));

            // if the facet string is the same, we can put the deletion and addition in the same obkv.
            if old_hyper_normalized_value == new_hyper_normalized_value {
                // nothing to do if we delete and re-add the value.
                if is_same_value {
                    continue;
                }

                buffer.clear();
                let mut obkv = KvWriterDelAdd::new(&mut buffer);
                for (deladd_key, _) in deladd_reader.iter() {
                    let val = SerdeJson::bytes_encode(&set).map_err(heed::Error::Encoding)?;
                    obkv.insert(deladd_key, val)?;
                }
                obkv.finish()?;

                let key: (u16, &str) = (field_id, new_hyper_normalized_value.as_ref());
                let key_bytes = BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
                normalized_facet_string_docids_sorter.insert(key_bytes, &buffer)?;
            } else {
                // if the facet string is different, we need to insert the deletion and addition in different obkv because the related key is different.
                // deletion
                if deladd_reader.get(DelAdd::Deletion).is_some() {
                    // insert old value
                    let val = SerdeJson::bytes_encode(&set).map_err(heed::Error::Encoding)?;
                    buffer.clear();
                    let mut obkv = KvWriterDelAdd::new(&mut buffer);
                    obkv.insert(DelAdd::Deletion, val)?;
                    obkv.finish()?;
                    let key: (u16, &str) = (field_id, old_hyper_normalized_value.as_ref());
                    let key_bytes =
                        BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
                    normalized_facet_string_docids_sorter.insert(key_bytes, &buffer)?;
                }

                // addition
                if deladd_reader.get(DelAdd::Addition).is_some() {
                    // insert new value
                    let val = SerdeJson::bytes_encode(&set).map_err(heed::Error::Encoding)?;
                    buffer.clear();
                    let mut obkv = KvWriterDelAdd::new(&mut buffer);
                    obkv.insert(DelAdd::Addition, val)?;
                    obkv.finish()?;
                    let key: (u16, &str) = (field_id, new_hyper_normalized_value.as_ref());
                    let key_bytes =
                        BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
                    normalized_facet_string_docids_sorter.insert(key_bytes, &buffer)?;
                }
            }
        }

        // nothing to do if we delete and re-add the value.
        if is_same_value {
            continue;
        }

        let key = FacetGroupKey { field_id, level: 0, left_bound: normalized_value };
        let key_bytes = FacetGroupKeyCodec::<StrRefCodec>::bytes_encode(&key).unwrap();

        buffer.clear();
        let mut obkv = KvWriterDelAdd::new(&mut buffer);
        for (deladd_key, _) in deladd_reader.iter() {
            obkv.insert(deladd_key, document_id.to_ne_bytes())?;
        }
        obkv.finish()?;
        facet_string_docids_sorter.insert(&key_bytes, &buffer)?;
    }

    let normalized = sorter_into_reader(normalized_facet_string_docids_sorter, indexer)?;
    sorter_into_reader(facet_string_docids_sorter, indexer).map(|s| (s, normalized))
}

/// Normalizes the facet string and truncates it to the max length.
fn normalize_facet_string(facet_string: &str, locales: Option<&[Language]>) -> String {
    let options = NormalizerOption { lossy: true, ..Default::default() };
    let mut detection = StrDetection::new(facet_string, locales);
    let token = Token {
        lemma: std::borrow::Cow::Borrowed(facet_string),
        script: detection.script(),
        language: detection.language(),
        ..Default::default()
    };

    // truncate the facet string to the max length
    token
        .normalize(&options)
        .lemma
        .char_indices()
        .take_while(|(idx, _)| *idx < MAX_FACET_VALUE_LENGTH)
        .map(|(_, c)| c)
        .collect()
}
