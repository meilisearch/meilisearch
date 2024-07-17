use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::BufReader;
use std::{io, mem, str};

use charabia::{Language, Script, SeparatorKind, Token, TokenKind, Tokenizer, TokenizerBuilder};
use obkv::{KvReader, KvWriterU16};
use roaring::RoaringBitmap;
use serde_json::Value;

use super::helpers::{create_sorter, keep_latest_obkv, sorter_into_reader, GrenadParameters};
use crate::error::{InternalError, SerializationError};
use crate::update::del_add::{del_add_from_two_obkvs, DelAdd, KvReaderDelAdd};
use crate::update::settings::{InnerIndexSettings, InnerIndexSettingsDiff};
use crate::{FieldId, Result, MAX_POSITION_PER_ATTRIBUTE, MAX_WORD_LENGTH};

pub type ScriptLanguageDocidsMap = HashMap<(Script, Language), (RoaringBitmap, RoaringBitmap)>;

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
) -> Result<(grenad::Reader<BufReader<File>>, ScriptLanguageDocidsMap)> {
    let mut conn = super::REDIS_CLIENT.get_connection().unwrap();

    let max_positions_per_attributes = max_positions_per_attributes
        .map_or(MAX_POSITION_PER_ATTRIBUTE, |max| max.min(MAX_POSITION_PER_ATTRIBUTE));
    let max_memory = indexer.max_memory_by_thread();
    let force_reindexing = settings_diff.reindex_searchable();

    // initialize destination values.
    let mut documents_ids = RoaringBitmap::new();
    let mut script_language_docids = HashMap::new();
    let mut docid_word_positions_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        keep_latest_obkv,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

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
    let mut del_builder = tokenizer_builder(
        old_stop_words,
        old_separators.as_deref(),
        old_dictionary.as_deref(),
        None,
    );
    let del_tokenizer = del_builder.build();

    let new_stop_words = settings_diff.new.stop_words.as_ref();
    let new_separators: Option<Vec<_>> = settings_diff
        .new
        .allowed_separators
        .as_ref()
        .map(|s| s.iter().map(String::as_str).collect());
    let new_dictionary: Option<Vec<_>> =
        settings_diff.new.dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
    let mut add_builder = tokenizer_builder(
        new_stop_words,
        new_separators.as_deref(),
        new_dictionary.as_deref(),
        None,
    );
    let add_tokenizer = add_builder.build();

    // iterate over documents.
    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let document_id = key
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = KvReader::<FieldId>::new(value);

        // if the searchable fields didn't change, skip the searchable indexing for this document.
        if !force_reindexing && !searchable_fields_changed(&obkv, settings_diff) {
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
                lang_safe_tokens_from_document(
                    &obkv,
                    &settings_diff.old,
                    &del_tokenizer,
                    max_positions_per_attributes,
                    DelAdd::Deletion,
                    &mut del_buffers,
                )
            },
            || {
                // additions
                lang_safe_tokens_from_document(
                    &obkv,
                    &settings_diff.new,
                    &add_tokenizer,
                    max_positions_per_attributes,
                    DelAdd::Addition,
                    &mut add_buffers,
                )
            },
        );

        let (del_obkv, del_script_language_word_count) = del?;
        let (add_obkv, add_script_language_word_count) = add?;

        // merge deletions and additions.
        // transforming two KV<FieldId, KV<u16, String>> into one KV<FieldId, KV<DelAdd, KV<u16, String>>>
        value_buffer.clear();
        del_add_from_two_obkvs(
            &KvReader::<FieldId>::new(del_obkv),
            &KvReader::<FieldId>::new(add_obkv),
            &mut value_buffer,
        )?;

        // write each KV<DelAdd, KV<u16, String>> into the sorter, field by field.
        let obkv = KvReader::<FieldId>::new(&value_buffer);
        for (field_id, value) in obkv.iter() {
            key_buffer.truncate(mem::size_of::<u32>());
            key_buffer.extend_from_slice(&field_id.to_be_bytes());
            redis::cmd("INCR").arg(key_buffer.as_slice()).query::<usize>(&mut conn).unwrap();
            docid_word_positions_sorter.insert(&key_buffer, value)?;
        }

        // update script_language_docids deletions.
        for (script, languages_frequency) in del_script_language_word_count {
            for (language, _) in languages_frequency {
                let entry = script_language_docids
                    .entry((script, language))
                    .or_insert_with(|| (RoaringBitmap::new(), RoaringBitmap::new()));
                entry.0.push(document_id);
            }
        }

        // update script_language_docids additions.
        for (script, languages_frequency) in add_script_language_word_count {
            for (language, _) in languages_frequency {
                let entry = script_language_docids
                    .entry((script, language))
                    .or_insert_with(|| (RoaringBitmap::new(), RoaringBitmap::new()));
                entry.1.push(document_id);
            }
        }
    }

    // the returned sorter is serialized as: key: (DocId, FieldId), value: KV<DelAdd, KV<u16, String>>.
    sorter_into_reader(docid_word_positions_sorter, indexer)
        .map(|reader| (reader, script_language_docids))
}

/// Check if any searchable fields of a document changed.
fn searchable_fields_changed(
    obkv: &KvReader<'_, FieldId>,
    settings_diff: &InnerIndexSettingsDiff,
) -> bool {
    let searchable_fields = &settings_diff.new.searchable_fields_ids;
    for (field_id, field_bytes) in obkv.iter() {
        if searchable_fields.contains(&field_id) {
            let del_add = KvReaderDelAdd::new(field_bytes);
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
    script_language: Option<&'a HashMap<Script, Vec<Language>>>,
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

    if let Some(script_language) = script_language {
        tokenizer_builder.allow_list(script_language);
    }

    tokenizer_builder
}

/// Extract words mapped with their positions of a document,
/// ensuring no Language detection mistakes was made.
fn lang_safe_tokens_from_document<'a>(
    obkv: &KvReader<'_, FieldId>,
    settings: &InnerIndexSettings,
    tokenizer: &Tokenizer<'_>,
    max_positions_per_attributes: u32,
    del_add: DelAdd,
    buffers: &'a mut Buffers,
) -> Result<(&'a [u8], HashMap<Script, Vec<(Language, usize)>>)> {
    let mut script_language_word_count = HashMap::new();

    tokens_from_document(
        obkv,
        &settings.searchable_fields_ids,
        tokenizer,
        max_positions_per_attributes,
        del_add,
        buffers,
        &mut script_language_word_count,
    )?;

    // if we detect a potetial mistake in the language detection,
    // we rerun the extraction forcing the tokenizer to detect the most frequently detected Languages.
    // context: https://github.com/meilisearch/meilisearch/issues/3565
    if script_language_word_count
        .values()
        .map(Vec::as_slice)
        .any(potential_language_detection_error)
    {
        // build an allow list with the most frequent detected languages in the document.
        let script_language: HashMap<_, _> =
            script_language_word_count.iter().filter_map(most_frequent_languages).collect();

        // if the allow list is empty, meaning that no Language is considered frequent,
        // then we don't rerun the extraction.
        if !script_language.is_empty() {
            // build a new temporary tokenizer including the allow list.
            let stop_words = settings.stop_words.as_ref();
            let separators: Option<Vec<_>> = settings
                .allowed_separators
                .as_ref()
                .map(|s| s.iter().map(String::as_str).collect());
            let dictionary: Option<Vec<_>> =
                settings.dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
            let mut builder =
                tokenizer_builder(stop_words, separators.as_deref(), dictionary.as_deref(), None);
            let tokenizer = builder.build();

            script_language_word_count.clear();

            // rerun the extraction.
            tokens_from_document(
                obkv,
                &settings.searchable_fields_ids,
                &tokenizer,
                max_positions_per_attributes,
                del_add,
                buffers,
                &mut script_language_word_count,
            )?;
        }
    }

    // returns a (KV<FieldId, KV<u16, String>>, HashMap<Script, Vec<(Language, usize)>>)
    Ok((&buffers.obkv_buffer, script_language_word_count))
}

/// Extract words mapped with their positions of a document.
fn tokens_from_document<'a>(
    obkv: &KvReader<'a, FieldId>,
    searchable_fields: &[FieldId],
    tokenizer: &Tokenizer<'_>,
    max_positions_per_attributes: u32,
    del_add: DelAdd,
    buffers: &'a mut Buffers,
    script_language_word_count: &mut HashMap<Script, Vec<(Language, usize)>>,
) -> Result<&'a [u8]> {
    buffers.obkv_buffer.clear();
    let mut document_writer = KvWriterU16::new(&mut buffers.obkv_buffer);
    for (field_id, field_bytes) in obkv.iter() {
        // if field is searchable.
        if searchable_fields.as_ref().contains(&field_id) {
            // extract deletion or addition only.
            if let Some(field_bytes) = KvReaderDelAdd::new(field_bytes).get(del_add) {
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
                    let tokens = process_tokens(tokenizer.tokenize(field))
                        .take_while(|(p, _)| (*p as u32) < max_positions_per_attributes);

                    for (index, token) in tokens {
                        // if a language has been detected for the token, we update the counter.
                        if let Some(language) = token.language {
                            let script = token.script;
                            let entry = script_language_word_count.entry(script).or_default();
                            match entry.iter_mut().find(|(l, _)| *l == language) {
                                Some((_, n)) => *n += 1,
                                None => entry.push((language, 1)),
                            }
                        }

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

fn potential_language_detection_error(languages_frequency: &[(Language, usize)]) -> bool {
    if languages_frequency.len() > 1 {
        let threshold = compute_language_frequency_threshold(languages_frequency);
        languages_frequency.iter().any(|(_, c)| *c <= threshold)
    } else {
        false
    }
}

fn most_frequent_languages(
    (script, languages_frequency): (&Script, &Vec<(Language, usize)>),
) -> Option<(Script, Vec<Language>)> {
    if languages_frequency.len() > 1 {
        let threshold = compute_language_frequency_threshold(languages_frequency);

        let languages: Vec<_> =
            languages_frequency.iter().filter(|(_, c)| *c > threshold).map(|(l, _)| *l).collect();

        if languages.is_empty() {
            None
        } else {
            Some((*script, languages))
        }
    } else {
        None
    }
}

fn compute_language_frequency_threshold(languages_frequency: &[(Language, usize)]) -> usize {
    let total: usize = languages_frequency.iter().map(|(_, c)| c).sum();
    total / 10 // 10% is a completely arbitrary value.
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
