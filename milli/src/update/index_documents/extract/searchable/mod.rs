use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::fs::File;
use std::io;
use std::io::BufReader;

use field_word_position::FieldWordPositionExtractorBuilder;
use obkv::KvReader;
use roaring::RoaringBitmap;
use word_docids::{WordDocidsDump, WordDocidsExtractor};

use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::update::index_documents::extract::extract_docid_word_positions::ScriptLanguageDocidsMap;
use crate::update::index_documents::GrenadParameters;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::{FieldId, Result, SerializationError};

mod field_word_position;
mod word_docids;

#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_searchable_data<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
    max_positions_per_attributes: Option<u32>,
) -> Result<(grenad::Reader<BufReader<File>>, ScriptLanguageDocidsMap)> {
    let searchable_fields_to_index = settings_diff.searchable_fields_to_index();

    let mut documents_ids = RoaringBitmap::new();

    let add_builder =
        FieldWordPositionExtractorBuilder::new(max_positions_per_attributes, &settings_diff.new)?;
    let add_token_positions_extractor = add_builder.build();
    let del_builder;
    let del_token_positions_extractor = if settings_diff.settings_update_only {
        del_builder = FieldWordPositionExtractorBuilder::new(
            max_positions_per_attributes,
            &settings_diff.old,
        )?;
        del_builder.build()
    } else {
        add_builder.build()
    };
    let token_positions_extractor = &[del_token_positions_extractor, add_token_positions_extractor];

    let mut word_map = BTreeMap::new();
    let mut word_docids_extractor = WordDocidsExtractor::new(settings_diff);

    let mut cursor = obkv_documents.into_cursor()?;
    // loop over documents
    while let Some((key, value)) = cursor.move_on_next()? {
        let document_id = key
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = KvReader::<FieldId>::new(value);
        // if the searchable fields didn't change, skip the searchable indexing for this document.
        if !settings_diff.reindex_searchable()
            && !searchable_fields_changed(&obkv, &searchable_fields_to_index)
        {
            continue;
        }

        documents_ids.push(document_id);

        let mut buffer = String::new();
        for field_id in searchable_fields_to_index.iter() {
            let Some(field_obkv) = obkv.get(*field_id).map(KvReaderDelAdd::new) else { continue };

            for (deladd, field_bytes) in field_obkv {
                let mut extracted_positions =
                    token_positions_extractor[deladd as usize].extract(field_bytes, &mut buffer)?;
                for (position, token) in extracted_positions.iter() {
                    let word = token.lemma().trim();
                    if !word_map.contains_key(word) {
                        word_map.insert(word.to_string(), word_map.len() as u32);
                    }
                    let word_id = word_map.get(word).unwrap();
                    word_docids_extractor.insert(*word_id, *field_id, document_id, deladd);
                }
            }
        }

        if word_docids_extractor.rough_size_estimate()
            > indexer.max_memory.map_or(512 * 1024 * 1024, |s| s.min(512 * 1024 * 1024))
        {
            let WordDocidsDump { .. } =
                word_docids_extractor.dump(&word_map, &searchable_fields_to_index, indexer)?;
        }
    }

    todo!()
}

/// Check if any searchable fields of a document changed.
fn searchable_fields_changed(
    obkv: &KvReader<FieldId>,
    searchable_fields: &BTreeSet<FieldId>,
) -> bool {
    for field_id in searchable_fields {
        let Some(field_obkv) = obkv.get(*field_id).map(KvReaderDelAdd::new) else { continue };
        match (field_obkv.get(DelAdd::Deletion), field_obkv.get(DelAdd::Addition)) {
            // if both fields are None, check the next field.
            (None, None) => (),
            // if both contains a value and values are the same, check the next field.
            (Some(del), Some(add)) if del == add => (),
            // otherwise the fields are different, return true.
            _otherwise => return true,
        }
    }

    false
}
