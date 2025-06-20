use std::convert::TryInto;
use std::fs::File;
use std::io::BufReader;
use std::{io, mem, str};

use charabia::{SeparatorKind, Token, TokenKind, Tokenizer, TokenizerBuilder};
use obkv::{KvReader, KvWriterU16};
use roaring::RoaringBitmap;
use serde_json::Value;

use super::helpers::{create_sorter, sorter_into_reader, GrenadParameters, KeepLatestObkv};
use crate::error::{InternalError, SerializationError};
use crate::update::del_add::{del_add_from_two_obkvs, DelAdd, KvReaderDelAdd};
use crate::update::settings::{InnerIndexSettings, InnerIndexSettingsDiff};
use crate::{FieldId, Result, MAX_POSITION_PER_ATTRIBUTE, MAX_WORD_LENGTH};

/// Extracts the word and positions where this word appear and
/// prefixes it by the document id.
///
/// Returns the generated internal documents ids and a grenad reader
/// with the list of extracted words from the given chunk of documents.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_docid_word_positions<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    settings_diff: &InnerIndexSettingsDiff,
    max_positions_per_attributes: Option<u32>,
) -> Result<grenad::Reader<BufReader<File>>> {
    let max_positions_per_attributes = max_positions_per_attributes
        .map_or(MAX_POSITION_PER_ATTRIBUTE, |max| max.min(MAX_POSITION_PER_ATTRIBUTE));
    let max_memory = indexer.max_memory_by_thread();

    // initialize destination values.
    let mut documents_ids = RoaringBitmap::new();
    let mut docid_word_positions_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        KeepLatestObkv,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
        true,
    );

    let force_reindexing = settings_diff.reindex_searchable();
    let skip_indexing = !force_reindexing && settings_diff.settings_update_only();
    if skip_indexing {
        return sorter_into_reader(docid_word_positions_sorter, indexer);
    }

    // initialize buffers.
    let mut del_buffers = Buffers::default();
    let mut add_buffers = Buffers::default();
    let mut key_buffer = Vec::new();
    let mut value_buffer = Vec::new();

    // initialize tokenizer.
    let old_stop_words = settings_diff.old.stop_words.as_ref();
    let old_separators: Option<Vec<_>> = settings_diff
        .old
        .allowed_separators
        .as_ref()
        .map(|s| s.iter().map(String::as_str).collect());
    let old_dictionary: Option<Vec<_>> =
        settings_diff.old.dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
    let mut del_builder =
        tokenizer_builder(old_stop_words, old_separators.as_deref(), old_dictionary.as_deref());
    let del_tokenizer = del_builder.build();

    let new_stop_words = settings_diff.new.stop_words.as_ref();
    let new_separators: Option<Vec<_>> = settings_diff
        .new
        .allowed_separators
        .as_ref()
        .map(|s| s.iter().map(String::as_str).collect());
    let new_dictionary: Option<Vec<_>> =
        settings_diff.new.dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
    let mut add_builder =
        tokenizer_builder(new_stop_words, new_separators.as_deref(), new_dictionary.as_deref());
    let add_tokenizer = add_builder.build();

    // iterate over documents.
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let document_id = key
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = KvReader::<FieldId>::from_slice(value);

        // if the searchable fields didn't change, skip the searchable indexing for this document.
        if !force_reindexing && !searchable_fields_changed(obkv, settings_diff) {
            continue;
        }

        documents_ids.push(document_id);

        // Update key buffer prefix.
        key_buffer.clear();
        key_buffer.extend_from_slice(&document_id.to_be_bytes());

        // Tokenize deletions and additions in 2 diffferent threads.
        let (del, add): (Result<_>, Result<_>) = rayon::join(
            || {
                // deletions
                tokens_from_document(
                    obkv,
                    &settings_diff.old,
                    &del_tokenizer,
                    max_positions_per_attributes,
                    DelAdd::Deletion,
                    &mut del_buffers,
                )
            },
            || {
                // additions
                tokens_from_document(
                    obkv,
                    &settings_diff.new,
                    &add_tokenizer,
                    max_positions_per_attributes,
                    DelAdd::Addition,
                    &mut add_buffers,
                )
            },
        );

        let del_obkv = del?;
        let add_obkv = add?;

        // merge deletions and additions.
        // transforming two KV<FieldId, KV<u16, String>> into one KV<FieldId, KV<DelAdd, KV<u16, String>>>
        value_buffer.clear();
        del_add_from_two_obkvs(
            KvReader::<FieldId>::from_slice(del_obkv),
            KvReader::<FieldId>::from_slice(add_obkv),
            &mut value_buffer,
        )?;

        // write each KV<DelAdd, KV<u16, String>> into the sorter, field by field.
        let obkv = KvReader::<FieldId>::from_slice(&value_buffer);
        for (field_id, value) in obkv.iter() {
            key_buffer.truncate(mem::size_of::<u32>());
            key_buffer.extend_from_slice(&field_id.to_be_bytes());
            docid_word_positions_sorter.insert(&key_buffer, value)?;
        }
    }

    // the returned sorter is serialized as: key: (DocId, FieldId), value: KV<DelAdd, KV<u16, String>>.
    sorter_into_reader(docid_word_positions_sorter, indexer)
}

/// Check if any searchable fields of a document changed.
fn searchable_fields_changed(
    obkv: &KvReader<FieldId>,
    settings_diff: &InnerIndexSettingsDiff,
) -> bool {
    for (field_id, field_bytes) in obkv.iter() {
        let Some(metadata) = settings_diff.new.fields_ids_map.metadata(field_id) else {
            // If the field id is not in the fields ids map, skip it.
            // This happens for the vectors sub-fields. for example:
            // "_vectors": { "manual": [1, 2, 3]} -> "_vectors.manual" is not registered.
            continue;
        };
        if metadata.is_searchable() {
            let del_add = KvReaderDelAdd::from_slice(field_bytes);
            match (del_add.get(DelAdd::Deletion), del_add.get(DelAdd::Addition)) {
                // if both fields are None, check the next field.
                (None, None) => (),
                // if both contains a value and values are the same, check the next field.
                (Some(del), Some(add)) if del == add => (),
                // otherwise the fields are different, return true.
                _otherwise => return true,
            }
        }
    }

    false
}

/// Factorize tokenizer building.
fn tokenizer_builder<'a>(
    stop_words: Option<&'a fst::Set<Vec<u8>>>,
    allowed_separators: Option<&'a [&str]>,
    dictionary: Option<&'a [&str]>,
) -> TokenizerBuilder<'a, Vec<u8>> {
    let mut tokenizer_builder = TokenizerBuilder::new();
    if let Some(stop_words) = stop_words {
        tokenizer_builder.stop_words(stop_words);
    }
    if let Some(dictionary) = dictionary {
        tokenizer_builder.words_dict(dictionary);
    }
    if let Some(separators) = allowed_separators {
        tokenizer_builder.separators(separators);
    }

    tokenizer_builder
}

/// Extract words mapped with their positions of a document.
fn tokens_from_document<'a>(
    obkv: &'a KvReader<FieldId>,
    settings: &InnerIndexSettings,
    tokenizer: &Tokenizer<'_>,
    max_positions_per_attributes: u32,
    del_add: DelAdd,
    buffers: &'a mut Buffers,
) -> Result<&'a [u8]> {
    buffers.obkv_buffer.clear();
    let mut document_writer = KvWriterU16::new(&mut buffers.obkv_buffer);
    for (field_id, field_bytes) in obkv.iter() {
        let Some(metadata) = settings.fields_ids_map.metadata(field_id) else {
            // If the field id is not in the fields ids map, skip it.
            // This happens for the vectors sub-fields. for example:
            // "_vectors": { "manual": [1, 2, 3]} -> "_vectors.manual" is not registered.
            continue;
        };
        // if field is searchable.
        if metadata.is_searchable() {
            // extract deletion or addition only.
            if let Some(field_bytes) = KvReaderDelAdd::from_slice(field_bytes).get(del_add) {
                // parse json.
                let value =
                    serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;

                // prepare writing destination.
                buffers.obkv_positions_buffer.clear();
                let mut writer = KvWriterU16::new(&mut buffers.obkv_positions_buffer);

                // convert json into a unique string.
                buffers.field_buffer.clear();
                if let Some(field) = json_to_string(&value, &mut buffers.field_buffer) {
                    // create an iterator of token with their positions.
                    let locales = metadata.locales(&settings.localized_attributes_rules);
                    let tokens = process_tokens(tokenizer.tokenize_with_allow_list(field, locales))
                        .take_while(|(p, _)| (*p as u32) < max_positions_per_attributes);

                    for (index, token) in tokens {
                        // keep a word only if it is not empty and fit in a LMDB key.
                        let token = token.lemma().trim();
                        if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
                            let position: u16 = index
                                .try_into()
                                .map_err(|_| SerializationError::InvalidNumberSerialization)?;
                            writer.insert(position, token.as_bytes())?;
                        }
                    }

                    // write positions into document.
                    let positions = writer.into_inner()?;
                    document_writer.insert(field_id, positions)?;
                }
            }
        }
    }

    // returns a KV<FieldId, KV<u16, String>>
    Ok(document_writer.into_inner().map(|v| v.as_slice())?)
}

/// Transform a JSON value into a string that can be indexed.
fn json_to_string<'a>(value: &'a Value, buffer: &'a mut String) -> Option<&'a str> {
    fn inner(value: &Value, output: &mut String) -> bool {
        use std::fmt::Write;
        match value {
            Value::Null | Value::Object(_) => false,
            Value::Bool(boolean) => write!(output, "{}", boolean).is_ok(),
            Value::Number(number) => write!(output, "{}", number).is_ok(),
            Value::String(string) => write!(output, "{}", string).is_ok(),
            Value::Array(array) => {
                let mut count = 0;
                for value in array {
                    if inner(value, output) {
                        output.push_str(". ");
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
        }
    }

    if let Value::String(string) = value {
        Some(string)
    } else if inner(value, buffer) {
        Some(buffer)
    } else {
        None
    }
}

/// take an iterator on tokens and compute their relative position depending on separator kinds
/// if it's an `Hard` separator we add an additional relative proximity of 8 between words,
/// else we keep the standard proximity of 1 between words.
fn process_tokens<'a>(
    tokens: impl Iterator<Item = Token<'a>>,
) -> impl Iterator<Item = (usize, Token<'a>)> {
    tokens
        .skip_while(|token| token.is_separator())
        .scan((0, None), |(offset, prev_kind), mut token| {
            match token.kind {
                TokenKind::Word | TokenKind::StopWord if !token.lemma().is_empty() => {
                    *offset += match *prev_kind {
                        Some(TokenKind::Separator(SeparatorKind::Hard)) => 8,
                        Some(_) => 1,
                        None => 0,
                    };
                    *prev_kind = Some(token.kind)
                }
                TokenKind::Separator(SeparatorKind::Hard) => {
                    *prev_kind = Some(token.kind);
                }
                TokenKind::Separator(SeparatorKind::Soft)
                    if *prev_kind != Some(TokenKind::Separator(SeparatorKind::Hard)) =>
                {
                    *prev_kind = Some(token.kind);
                }
                _ => token.kind = TokenKind::Unknown,
            }
            Some((*offset, token))
        })
        .filter(|(_, t)| t.is_word())
}

#[derive(Default)]
struct Buffers {
    // the field buffer for each fields desserialization, and must be cleared between each field.
    field_buffer: String,
    // buffer used to store the value data containing an obkv.
    obkv_buffer: Vec<u8>,
    // buffer used to store the value data containing an obkv of tokens with their positions.
    obkv_positions_buffer: Vec<u8>,
}
