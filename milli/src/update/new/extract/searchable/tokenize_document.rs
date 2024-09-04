use std::collections::HashMap;

use charabia::{SeparatorKind, Token, TokenKind, Tokenizer, TokenizerBuilder};
use serde_json::Value;

use crate::update::new::extract::perm_json_p::{
    seek_leaf_values_in_array, seek_leaf_values_in_object, select_field,
};
use crate::update::new::KvReaderFieldId;
use crate::{
    FieldId, GlobalFieldsIdsMap, InternalError, LocalizedAttributesRule, Result, UserError,
    MAX_WORD_LENGTH,
};

pub struct DocumentTokenizer<'a> {
    pub tokenizer: &'a Tokenizer<'a>,
    pub attribute_to_extract: Option<&'a [&'a str]>,
    pub attribute_to_skip: &'a [&'a str],
    pub localized_attributes_rules: &'a [LocalizedAttributesRule],
    pub max_positions_per_attributes: u32,
}

impl<'a> DocumentTokenizer<'a> {
    pub fn tokenize_document(
        &self,
        obkv: &KvReaderFieldId,
        field_id_map: &mut GlobalFieldsIdsMap,
        token_fn: &mut impl FnMut(FieldId, u16, &str) -> Result<()>,
    ) -> Result<()> {
        let mut field_position = HashMap::new();
        let mut field_name = String::new();
        for (field_id, field_bytes) in obkv {
            let Some(field_name) = field_id_map.name(field_id).map(|s| {
                field_name.clear();
                field_name.push_str(s);
                &field_name
            }) else {
                unreachable!("field id not found in field id map");
            };

            let mut tokenize_field = |name: &str, value: &Value| {
                let Some(field_id) = field_id_map.id_or_insert(name) else {
                    return Err(UserError::AttributeLimitReached.into());
                };

                let position =
                    field_position.entry(field_id).and_modify(|counter| *counter += 8).or_insert(0);
                if *position as u32 >= self.max_positions_per_attributes {
                    return Ok(());
                }

                match value {
                    Value::Number(n) => {
                        let token = n.to_string();
                        if let Ok(position) = (*position).try_into() {
                            token_fn(field_id, position, token.as_str())?;
                        }

                        Ok(())
                    }
                    Value::String(text) => {
                        // create an iterator of token with their positions.
                        let locales = self
                            .localized_attributes_rules
                            .iter()
                            .find(|rule| rule.match_str(field_name))
                            .map(|rule| rule.locales());
                        let tokens = process_tokens(
                            *position,
                            self.tokenizer.tokenize_with_allow_list(text.as_str(), locales),
                        )
                        .take_while(|(p, _)| (*p as u32) < self.max_positions_per_attributes);

                        for (index, token) in tokens {
                            // keep a word only if it is not empty and fit in a LMDB key.
                            let token = token.lemma().trim();
                            if !token.is_empty() && token.len() <= MAX_WORD_LENGTH {
                                *position = index;
                                if let Ok(position) = (*position).try_into() {
                                    token_fn(field_id, position, token)?;
                                }
                            }
                        }

                        Ok(())
                    }
                    _ => Ok(()),
                }
            };

            // if the current field is searchable or contains a searchable attribute
            if select_field(&field_name, self.attribute_to_extract, self.attribute_to_skip) {
                // parse json.
                match serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)? {
                    Value::Object(object) => seek_leaf_values_in_object(
                        &object,
                        self.attribute_to_extract,
                        self.attribute_to_skip,
                        &field_name,
                        &mut tokenize_field,
                    )?,
                    Value::Array(array) => seek_leaf_values_in_array(
                        &array,
                        self.attribute_to_extract,
                        self.attribute_to_skip,
                        &field_name,
                        &mut tokenize_field,
                    )?,
                    value => tokenize_field(&field_name, &value)?,
                }
            }
        }

        Ok(())
    }
}

/// take an iterator on tokens and compute their relative position depending on separator kinds
/// if it's an `Hard` separator we add an additional relative proximity of 8 between words,
/// else we keep the standard proximity of 1 between words.
fn process_tokens<'a>(
    start_offset: usize,
    tokens: impl Iterator<Item = Token<'a>>,
) -> impl Iterator<Item = (usize, Token<'a>)> {
    tokens
        .skip_while(|token| token.is_separator())
        .scan((start_offset, None), |(offset, prev_kind), mut token| {
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
    use charabia::TokenizerBuilder;
    use meili_snap::snapshot;
    use obkv::KvReader;
    use serde_json::json;

    use super::*;
    use crate::FieldsIdsMap;

    #[test]
    fn test_tokenize_document() {
        let mut fields_ids_map = FieldsIdsMap::new();

        let field_1 = json!({
                "name": "doggo",
                "age": 10,
        });

        let field_2 = json!({
                "catto": {
                    "name": "pesti",
                    "age": 23,
                }
        });

        let field_3 = json!(["doggo", "catto"]);
        let field_4 = json!("UNSEARCHABLE");
        let field_5 = json!({"nope": "unsearchable"});

        let mut obkv = obkv::KvWriter::memory();
        let field_1_id = fields_ids_map.insert("doggo").unwrap();
        let field_1 = serde_json::to_string(&field_1).unwrap();
        obkv.insert(field_1_id, field_1.as_bytes()).unwrap();
        let field_2_id = fields_ids_map.insert("catto").unwrap();
        let field_2 = serde_json::to_string(&field_2).unwrap();
        obkv.insert(field_2_id, field_2.as_bytes()).unwrap();
        let field_3_id = fields_ids_map.insert("doggo.name").unwrap();
        let field_3 = serde_json::to_string(&field_3).unwrap();
        obkv.insert(field_3_id, field_3.as_bytes()).unwrap();
        let field_4_id = fields_ids_map.insert("not-me").unwrap();
        let field_4 = serde_json::to_string(&field_4).unwrap();
        obkv.insert(field_4_id, field_4.as_bytes()).unwrap();
        let field_5_id = fields_ids_map.insert("me-nether").unwrap();
        let field_5 = serde_json::to_string(&field_5).unwrap();
        obkv.insert(field_5_id, field_5.as_bytes()).unwrap();
        let value = obkv.into_inner().unwrap();
        let obkv = KvReader::from_slice(value.as_slice());

        let mut tb = TokenizerBuilder::default();
        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tb.build(),
            attribute_to_extract: None,
            attribute_to_skip: &["not-me", "me-nether.nope"],
            localized_attributes_rules: &[],
            max_positions_per_attributes: 1000,
        };

        let fields_ids_map_lock = std::sync::RwLock::new(fields_ids_map);
        let mut global_fields_ids_map = GlobalFieldsIdsMap::new(&fields_ids_map_lock);

        let mut words = std::collections::BTreeMap::new();
        document_tokenizer
            .tokenize_document(obkv, &mut global_fields_ids_map, &mut |fid, pos, word| {
                words.insert([fid, pos], word.to_string());
                Ok(())
            })
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
                3,
                0,
            ]: "10",
            [
                4,
                0,
            ]: "pesti",
            [
                5,
                0,
            ]: "23",
        }
        "###);
    }
}
