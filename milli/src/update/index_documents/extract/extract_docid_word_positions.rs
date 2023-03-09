use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fs::File;
use std::{io, mem, str};

use charabia::{Language, Script, SeparatorKind, Token, TokenKind, Tokenizer, TokenizerBuilder};
use obkv::KvReader;
use roaring::RoaringBitmap;
use serde_json::Value;

use super::helpers::{concat_u32s_array, create_sorter, sorter_into_reader, GrenadParameters};
use crate::error::{InternalError, SerializationError};
use crate::update::index_documents::MergeFn;
use crate::{
    absolute_from_relative_position, FieldId, Result, MAX_POSITION_PER_ATTRIBUTE, MAX_WORD_LENGTH,
};

pub type ScriptLanguageDocidsMap = HashMap<(Script, Language), RoaringBitmap>;

/// Extracts the word and positions where this word appear and
/// prefixes it by the document id.
///
/// Returns the generated internal documents ids and a grenad reader
/// with the list of extracted words from the given chunk of documents.
#[logging_timer::time]
pub fn extract_docid_word_positions<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    searchable_fields: &Option<HashSet<FieldId>>,
    stop_words: Option<&fst::Set<&[u8]>>,
    max_positions_per_attributes: Option<u32>,
) -> Result<(RoaringBitmap, grenad::Reader<File>, ScriptLanguageDocidsMap)> {
    let max_positions_per_attributes = max_positions_per_attributes
        .map_or(MAX_POSITION_PER_ATTRIBUTE, |max| max.min(MAX_POSITION_PER_ATTRIBUTE));
    let max_memory = indexer.max_memory_by_thread();

    let mut documents_ids = RoaringBitmap::new();
    let mut script_language_docids = HashMap::new();
    let mut docid_word_positions_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        concat_u32s_array,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut buffers = Buffers::default();
    let mut tokenizer_builder = TokenizerBuilder::new();
    if let Some(stop_words) = stop_words {
        tokenizer_builder.stop_words(stop_words);
    }
    let tokenizer = tokenizer_builder.build();

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let document_id = key
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = KvReader::<FieldId>::new(value);

        documents_ids.push(document_id);
        buffers.key_buffer.clear();
        buffers.key_buffer.extend_from_slice(&document_id.to_be_bytes());

        let mut script_language_word_count = HashMap::new();

        extract_tokens_from_document(
            &obkv,
            searchable_fields,
            &tokenizer,
            max_positions_per_attributes,
            &mut buffers,
            &mut script_language_word_count,
            &mut docid_word_positions_sorter,
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
                let mut tokenizer_builder = TokenizerBuilder::new();
                if let Some(stop_words) = stop_words {
                    tokenizer_builder.stop_words(stop_words);
                }
                tokenizer_builder.allow_list(&script_language);
                let tokenizer = tokenizer_builder.build();

                script_language_word_count.clear();

                // rerun the extraction.
                extract_tokens_from_document(
                    &obkv,
                    searchable_fields,
                    &tokenizer,
                    max_positions_per_attributes,
                    &mut buffers,
                    &mut script_language_word_count,
                    &mut docid_word_positions_sorter,
                )?;
            }
        }

        for (script, languages_frequency) in script_language_word_count {
            for (language, _) in languages_frequency {
                let entry = script_language_docids
                    .entry((script, language))
                    .or_insert_with(RoaringBitmap::new);
                entry.push(document_id);
            }
        }
    }

    sorter_into_reader(docid_word_positions_sorter, indexer)
        .map(|reader| (documents_ids, reader, script_language_docids))
}

fn extract_tokens_from_document<T: AsRef<[u8]>>(
    obkv: &KvReader<FieldId>,
    searchable_fields: &Option<HashSet<FieldId>>,
    tokenizer: &Tokenizer<T>,
    max_positions_per_attributes: u32,
    buffers: &mut Buffers,
    script_language_word_count: &mut HashMap<Script, Vec<(Language, usize)>>,
    docid_word_positions_sorter: &mut grenad::Sorter<MergeFn>,
) -> Result<()> {
    for (field_id, field_bytes) in obkv.iter() {
        if searchable_fields.as_ref().map_or(true, |sf| sf.contains(&field_id)) {
            let value = serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;
            buffers.field_buffer.clear();
            if let Some(field) = json_to_string(&value, &mut buffers.field_buffer) {
                let tokens = process_tokens(tokenizer.tokenize(field))
                    .take_while(|(p, _)| (*p as u32) < max_positions_per_attributes);

                for (index, token) in tokens {
                    // if a language has been detected for the token, we update the counter.
                    if let Some(language) = token.language {
                        let script = token.script;
                        let entry =
                            script_language_word_count.entry(script).or_insert_with(Vec::new);
                        match entry.iter_mut().find(|(l, _)| *l == language) {
                            Some((_, n)) => *n += 1,
                            None => entry.push((language, 1)),
                        }
                    }
                    let token = token.lemma().trim();
                    if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
                        buffers.key_buffer.truncate(mem::size_of::<u32>());
                        buffers.key_buffer.extend_from_slice(token.as_bytes());

                        let position: u16 = index
                            .try_into()
                            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
                        let position = absolute_from_relative_position(field_id, position);
                        docid_word_positions_sorter
                            .insert(&buffers.key_buffer, position.to_ne_bytes())?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Transform a JSON value into a string that can be indexed.
fn json_to_string<'a>(value: &'a Value, buffer: &'a mut String) -> Option<&'a str> {
    fn inner(value: &Value, output: &mut String) -> bool {
        use std::fmt::Write;
        match value {
            Value::Null => false,
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
            Value::Object(object) => {
                let mut buffer = String::new();
                let mut count = 0;
                for (key, value) in object {
                    buffer.clear();
                    let _ = write!(&mut buffer, "{}: ", key);
                    if inner(value, &mut buffer) {
                        buffer.push_str(". ");
                        // We write the "key: value. " pair only when
                        // we are sure that the value can be written.
                        output.push_str(&buffer);
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
/// else we keep the standart proximity of 1 between words.
fn process_tokens<'a>(
    tokens: impl Iterator<Item = Token<'a>>,
) -> impl Iterator<Item = (usize, Token<'a>)> {
    tokens
        .skip_while(|token| token.is_separator())
        .scan((0, None), |(offset, prev_kind), token| {
            match token.kind {
                TokenKind::Word | TokenKind::StopWord | TokenKind::Unknown => {
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
                _ => (),
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
    // the key buffer is the concatenation of the internal document id with the field id.
    // The buffer has to be completelly cleared between documents,
    // and the field id part must be cleared between each field.
    key_buffer: Vec<u8>,
    // the field buffer for each fields desserialization, and must be cleared between each field.
    field_buffer: String,
}
