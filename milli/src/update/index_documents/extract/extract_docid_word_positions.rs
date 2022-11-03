use std::collections::HashSet;
use std::convert::TryInto;
use std::fs::File;
use std::{io, mem, str};

use charabia::{SeparatorKind, Token, TokenKind, TokenizerBuilder};
use roaring::RoaringBitmap;
use serde_json::Value;

use super::helpers::{
    concat_u32s_array, create_sorter, sorter_into_reader, GrenadParameters, MAX_WORD_LENGTH,
};
use crate::error::{InternalError, SerializationError};
use crate::{absolute_from_relative_position, FieldId, Result, MAX_POSITION_PER_ATTRIBUTE};

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
) -> Result<(RoaringBitmap, grenad::Reader<File>)> {
    let max_positions_per_attributes = max_positions_per_attributes
        .map_or(MAX_POSITION_PER_ATTRIBUTE, |max| max.min(MAX_POSITION_PER_ATTRIBUTE));
    let max_memory = indexer.max_memory_by_thread();

    let mut documents_ids = RoaringBitmap::new();
    let mut docid_word_positions_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        concat_u32s_array,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut key_buffer = Vec::new();
    let mut field_buffer = String::new();
    let mut builder = TokenizerBuilder::new();
    if let Some(stop_words) = stop_words {
        builder.stop_words(stop_words);
    }
    let tokenizer = builder.build();

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((key, value)) = cursor.move_on_next()? {
        let document_id = key
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = obkv::KvReader::<FieldId>::new(value);

        documents_ids.push(document_id);
        key_buffer.clear();
        key_buffer.extend_from_slice(&document_id.to_be_bytes());

        for (field_id, field_bytes) in obkv.iter() {
            if searchable_fields.as_ref().map_or(true, |sf| sf.contains(&field_id)) {
                let value =
                    serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;
                field_buffer.clear();
                if let Some(field) = json_to_string(&value, &mut field_buffer) {
                    let tokens = process_tokens(tokenizer.tokenize(field))
                        .take_while(|(p, _)| (*p as u32) < max_positions_per_attributes);

                    for (index, token) in tokens {
                        let token = token.lemma().trim();
                        if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
                            key_buffer.truncate(mem::size_of::<u32>());
                            key_buffer.extend_from_slice(token.as_bytes());

                            let position: u16 = index
                                .try_into()
                                .map_err(|_| SerializationError::InvalidNumberSerialization)?;
                            let position = absolute_from_relative_position(field_id, position);
                            docid_word_positions_sorter
                                .insert(&key_buffer, position.to_ne_bytes())?;
                        }
                    }
                }
            }
        }
    }

    sorter_into_reader(docid_word_positions_sorter, indexer).map(|reader| (documents_ids, reader))
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
