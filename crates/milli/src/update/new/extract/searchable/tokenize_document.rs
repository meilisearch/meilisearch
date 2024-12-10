use std::collections::HashMap;

use charabia::{SeparatorKind, Token, TokenKind, Tokenizer, TokenizerBuilder};
use serde_json::Value;

use crate::update::new::document::Document;
use crate::update::new::extract::perm_json_p::{
    seek_leaf_values_in_array, seek_leaf_values_in_object, select_field, Depth, Selection,
};
use crate::{
    FieldId, GlobalFieldsIdsMap, InternalError, LocalizedAttributesRule, Result, UserError,
    MAX_WORD_LENGTH,
};

// todo: should be crate::proximity::MAX_DISTANCE but it has been forgotten
const MAX_DISTANCE: u32 = 8;

pub struct DocumentTokenizer<'a> {
    pub tokenizer: &'a Tokenizer<'a>,
    pub attribute_to_extract: Option<&'a [&'a str]>,
    pub attribute_to_skip: &'a [&'a str],
    pub localized_attributes_rules: &'a [LocalizedAttributesRule],
    pub max_positions_per_attributes: u32,
}

impl<'a> DocumentTokenizer<'a> {
    pub fn tokenize_document<'doc>(
        &self,
        document: impl Document<'doc>,
        field_id_map: &mut GlobalFieldsIdsMap,
        token_fn: &mut impl FnMut(&str, FieldId, u16, &str) -> Result<()>,
    ) -> Result<()> {
        let mut field_position = HashMap::new();

        for entry in document.iter_top_level_fields() {
            let (field_name, value) = entry?;

            let mut tokenize_field = |field_name: &str, _depth, value: &Value| {
                let Some(field_id) = field_id_map.id_or_insert(field_name) else {
                    return Err(UserError::AttributeLimitReached.into());
                };

                if select_field(field_name, self.attribute_to_extract, self.attribute_to_skip)
                    != Selection::Select
                {
                    return Ok(());
                }

                let position = field_position
                    .entry(field_id)
                    .and_modify(|counter| *counter += MAX_DISTANCE)
                    .or_insert(0);
                if *position >= self.max_positions_per_attributes {
                    return Ok(());
                }

                let text;
                let tokens = match value {
                    Value::Number(n) => {
                        text = n.to_string();
                        self.tokenizer.tokenize(text.as_str())
                    }
                    Value::Bool(b) => {
                        text = b.to_string();
                        self.tokenizer.tokenize(text.as_str())
                    }
                    Value::String(text) => {
                        let locales = self
                            .localized_attributes_rules
                            .iter()
                            .find(|rule| rule.match_str(field_name))
                            .map(|rule| rule.locales());
                        self.tokenizer.tokenize_with_allow_list(text.as_str(), locales)
                    }
                    _ => return Ok(()),
                };

                // create an iterator of token with their positions.
                let tokens = process_tokens(*position, tokens)
                    .take_while(|(p, _)| *p < self.max_positions_per_attributes);

                for (index, token) in tokens {
                    // keep a word only if it is not empty and fit in a LMDB key.
                    let token = token.lemma().trim();
                    if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
                        *position = index;
                        if let Ok(position) = (*position).try_into() {
                            token_fn(field_name, field_id, position, token)?;
                        }
                    }
                }

                Ok(())
            };

            // parse json.
            match serde_json::to_value(value).map_err(InternalError::SerdeJson)? {
                Value::Object(object) => seek_leaf_values_in_object(
                    &object,
                    None,
                    &[],
                    field_name,
                    Depth::OnBaseKey,
                    &mut tokenize_field,
                )?,
                Value::Array(array) => seek_leaf_values_in_array(
                    &array,
                    None,
                    &[],
                    field_name,
                    Depth::OnBaseKey,
                    &mut tokenize_field,
                )?,
                value => tokenize_field(field_name, Depth::OnBaseKey, &value)?,
            }
        }

        Ok(())
    }
}

/// take an iterator on tokens and compute their relative position depending on separator kinds
/// if it's an `Hard` separator we add an additional relative proximity of MAX_DISTANCE between words,
/// else we keep the standard proximity of 1 between words.
fn process_tokens<'a>(
    start_offset: u32,
    tokens: impl Iterator<Item = Token<'a>>,
) -> impl Iterator<Item = (u32, Token<'a>)> {
    tokens
        .skip_while(|token| token.is_separator())
        .scan((start_offset, None), |(offset, prev_kind), mut token| {
            match token.kind {
                TokenKind::Word | TokenKind::StopWord if !token.lemma().is_empty() => {
                    *offset += match *prev_kind {
                        Some(TokenKind::Separator(SeparatorKind::Hard)) => MAX_DISTANCE,
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

/// Factorize tokenizer building.
pub fn tokenizer_builder<'a>(
    stop_words: Option<&'a fst::Set<&'a [u8]>>,
    allowed_separators: Option<&'a [&str]>,
    dictionary: Option<&'a [&str]>,
) -> TokenizerBuilder<'a, &'a [u8]> {
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

#[cfg(test)]
mod test {
    use bumpalo::Bump;
    use bumparaw_collections::RawMap;
    use charabia::TokenizerBuilder;
    use meili_snap::snapshot;
    use rustc_hash::FxBuildHasher;
    use serde_json::json;
    use serde_json::value::RawValue;

    use super::*;
    use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};
    use crate::update::new::document::{DocumentFromVersions, Versions};
    use crate::FieldsIdsMap;

    #[test]
    fn test_tokenize_document() {
        let mut fields_ids_map = FieldsIdsMap::new();

        let document = json!({
            "doggo": {                "name": "doggo",
            "age": 10,},
            "catto": {
                "catto": {
                    "name": "pesti",
                    "age": 23,
                }
            },
            "doggo.name": ["doggo", "catto"],
            "not-me": "UNSEARCHABLE",
            "me-nether": {"nope": "unsearchable"}
        });

        let _field_1_id = fields_ids_map.insert("doggo").unwrap();
        let _field_2_id = fields_ids_map.insert("catto").unwrap();
        let _field_3_id = fields_ids_map.insert("doggo.name").unwrap();
        let _field_4_id = fields_ids_map.insert("not-me").unwrap();
        let _field_5_id = fields_ids_map.insert("me-nether").unwrap();

        let mut tb = TokenizerBuilder::default();
        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tb.build(),
            attribute_to_extract: None,
            attribute_to_skip: &["not-me", "me-nether.nope"],
            localized_attributes_rules: &[],
            max_positions_per_attributes: 1000,
        };

        let fields_ids_map = FieldIdMapWithMetadata::new(
            fields_ids_map,
            MetadataBuilder::new(Default::default(), Default::default(), Default::default(), None),
        );

        let fields_ids_map_lock = std::sync::RwLock::new(fields_ids_map);
        let mut global_fields_ids_map = GlobalFieldsIdsMap::new(&fields_ids_map_lock);

        let mut words = std::collections::BTreeMap::new();

        let document = document.to_string();

        let bump = Bump::new();
        let document: &RawValue = serde_json::from_str(&document).unwrap();
        let document = RawMap::from_raw_value_and_hasher(document, FxBuildHasher, &bump).unwrap();

        let document = Versions::single(document);
        let document = DocumentFromVersions::new(&document);

        document_tokenizer
            .tokenize_document(
                document,
                &mut global_fields_ids_map,
                &mut |_fname, fid, pos, word| {
                    words.insert([fid, pos], word.to_string());
                    Ok(())
                },
            )
            .unwrap();

        snapshot!(format!("{:#?}", words), @r###"
        {
            [
                2,
                0,
            ]: "doggo",
            [
                2,
                8,
            ]: "doggo",
            [
                2,
                16,
            ]: "catto",
            [
                5,
                0,
            ]: "10",
            [
                7,
                0,
            ]: "pesti",
            [
                8,
                0,
            ]: "23",
        }
        "###);
    }
}
