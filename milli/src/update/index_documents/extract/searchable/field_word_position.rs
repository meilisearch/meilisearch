use std::collections::HashMap;

use charabia::normalizer::NormalizedTokenIter;
use charabia::{Language, Script, SeparatorKind, Token, TokenKind, Tokenizer, TokenizerBuilder};
use roaring::RoaringBitmap;
use serde_json::Value;

use crate::update::settings::InnerIndexSettings;
use crate::{InternalError, Result, MAX_POSITION_PER_ATTRIBUTE, MAX_WORD_LENGTH};

pub type ScriptLanguageDocidsMap = HashMap<(Script, Language), (RoaringBitmap, RoaringBitmap)>;

pub struct FieldWordPositionExtractorBuilder<'a> {
    max_positions_per_attributes: u16,
    stop_words: Option<&'a fst::Set<Vec<u8>>>,
    separators: Option<Vec<&'a str>>,
    dictionary: Option<Vec<&'a str>>,
}

impl<'a> FieldWordPositionExtractorBuilder<'a> {
    pub fn new(
        max_positions_per_attributes: Option<u32>,
        settings: &'a InnerIndexSettings,
    ) -> Result<Self> {
        let stop_words = settings.stop_words.as_ref();
        let separators: Option<Vec<_>> =
            settings.allowed_separators.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let dictionary: Option<Vec<_>> =
            settings.dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
        Ok(Self {
            max_positions_per_attributes: max_positions_per_attributes
                .map_or(MAX_POSITION_PER_ATTRIBUTE as u16, |max| {
                    max.min(MAX_POSITION_PER_ATTRIBUTE) as u16
                }),
            stop_words,
            separators,
            dictionary,
        })
    }

    pub fn build(&'a self) -> FieldWordPositionExtractor<'a> {
        let builder = tokenizer_builder(
            self.stop_words,
            self.separators.as_deref(),
            self.dictionary.as_deref(),
            None,
        );

        FieldWordPositionExtractor {
            tokenizer: builder.into_tokenizer(),
            max_positions_per_attributes: self.max_positions_per_attributes,
        }
    }
}

pub struct FieldWordPositionExtractor<'a> {
    tokenizer: Tokenizer<'a>,
    max_positions_per_attributes: u16,
}

impl<'a> FieldWordPositionExtractor<'a> {
    pub fn extract<'b>(
        &'a self,
        field_bytes: &[u8],
        buffer: &'b mut String,
    ) -> Result<ExtractedFieldWordPosition<'a, 'b>> {
        let field_value = serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;
        Ok(ExtractedFieldWordPosition {
            tokenizer: &self.tokenizer,
            max_positions_per_attributes: self.max_positions_per_attributes,
            field_value,
            buffer: buffer,
        })
    }
}

pub struct ExtractedFieldWordPosition<'a, 'b> {
    tokenizer: &'a Tokenizer<'a>,
    max_positions_per_attributes: u16,
    field_value: Value,
    buffer: &'b mut String,
}

impl<'a> ExtractedFieldWordPosition<'a, '_> {
    pub fn iter<'o>(&'o mut self) -> FieldWordPositionIter<'o> {
        self.buffer.clear();
        let inner = match json_to_string(&self.field_value, &mut self.buffer) {
            Some(field) => Some(self.tokenizer.tokenize(field)),
            None => None,
        };

        // create an iterator of token with their positions.
        FieldWordPositionIter {
            inner,
            max_positions_per_attributes: self.max_positions_per_attributes,
            position: 0,
            prev_kind: None,
        }
    }
}

pub struct FieldWordPositionIter<'a> {
    inner: Option<NormalizedTokenIter<'a, 'a>>,
    max_positions_per_attributes: u16,
    position: u16,
    prev_kind: Option<TokenKind>,
}

impl<'a> Iterator for FieldWordPositionIter<'a> {
    type Item = (u16, Token<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.max_positions_per_attributes {
            return None;
        }

        let token = self.inner.as_mut().map(|i| i.next()).flatten()?;

        match token.kind {
            TokenKind::Word | TokenKind::StopWord if !token.lemma().is_empty() => {
                self.position += match self.prev_kind {
                    Some(TokenKind::Separator(SeparatorKind::Hard)) => 8,
                    Some(_) => 1,
                    None => 0,
                };
                self.prev_kind = Some(token.kind)
            }
            TokenKind::Separator(_) if self.position == 0 => {
                return self.next();
            }
            TokenKind::Separator(SeparatorKind::Hard) => {
                self.prev_kind = Some(token.kind);
            }
            TokenKind::Separator(SeparatorKind::Soft)
                if self.prev_kind != Some(TokenKind::Separator(SeparatorKind::Hard)) =>
            {
                self.prev_kind = Some(token.kind);
            }
            _ => return self.next(),
        }

        if !token.is_word() {
            return self.next();
        }

        // keep a word only if it is not empty and fit in a LMDB key.
        let lemma = token.lemma().trim();
        if !lemma.is_empty() && lemma.len() <= MAX_WORD_LENGTH {
            Some((self.position, token))
        } else {
            self.next()
        }
    }
}

/// Factorize tokenizer building.
pub fn tokenizer_builder<'a>(
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
