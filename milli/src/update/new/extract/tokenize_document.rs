pub struct DocumentTokenizer {
    tokenizer: &Tokenizer,
    searchable_attributes: Option<&[String]>,
    localized_attributes_rules: &[LocalizedAttributesRule],
    max_positions_per_attributes: u32,
}

impl DocumentTokenizer {
    // pub fn new(tokenizer: &Tokenizer, settings: &InnerIndexSettings) -> Self {
    //     Self { tokenizer, settings }
    // }

    pub fn tokenize_document<'a>(
        obkv: &KvReader<'a, FieldId>,
        field_id_map: &FieldsIdsMap,
        token_fn: impl Fn(FieldId, u16, &str),
    ) {
        let mut field_position = Hashmap::new();
        for (field_id, field_bytes) in obkv {
            let field_name = field_id_map.name(field_id);

            let tokenize_field = |name, value| {
                let field_id = field_id_map.id(name);
                match value {
                    Number(n) => {
                        let token = n.to_string();
                        let position = field_position
                            .entry(field_id)
                            .and_modify(|counter| *counter += 8)
                            .or_insert(0);
                        token_fn(field_id, position, token.as_str());
                    }
                    String(text) => {
                        // create an iterator of token with their positions.
                        let locales = self
                            .localized_attributes_rules
                            .iter()
                            .first(|rule| rule.match_str(field_name))
                            .map(|rule| rule.locales(field_id));
                        let tokens =
                            process_tokens(tokenizer.tokenize_with_allow_list(field, locales))
                                .take_while(|(p, _)| {
                                    (*p as u32) < self.max_positions_per_attributes
                                });

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
                    }
                    _ => (),
                }
            };

            // if the current field is searchable or contains a searchable attribute
            if searchable_attributes.map_or(true, |attributes| {
                attributes.iter().any(|name| contained_in(name, field_name))
            }) {
                // parse json.
                match serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)? {
                    Value::Object(object) => {
                        seek_leaf_values_in_object(object, selectors, &field_name, tokenize_field)
                    }
                    Value::Array(array) => {
                        seek_leaf_values_in_array(array, selectors, &field_name, tokenize_field)
                    }
                    value => tokenize_field(&base_key, value),
                }
            }
        }
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

/// Returns `true` if the `selector` match the `key`.
///
/// ```text
/// Example:
/// `animaux`           match `animaux`
/// `animaux.chien`     match `animaux`
/// `animaux.chien`     match `animaux`
/// `animaux.chien.nom` match `animaux`
/// `animaux.chien.nom` match `animaux.chien`
/// -----------------------------------------
/// `animaux`    doesn't match `animaux.chien`
/// `animaux.`   doesn't match `animaux`
/// `animaux.ch` doesn't match `animaux.chien`
/// `animau`     doesn't match `animaux`
/// ```
fn contained_in(selector: &str, key: &str) -> bool {
    selector.starts_with(key)
        && selector[key.len()..].chars().next().map(|c| c == SPLIT_SYMBOL).unwrap_or(true)
}

/// TODO move in permissive json pointer
mod perm_json_p {
    pub fn seek_leaf_values<'a>(
        value: &Map<String, Value>,
        selectors: impl IntoIterator<Item = &'a str>,
        seeker: impl Fn(&str, &Value),
    ) {
        let selectors: Vec<_> = selectors.into_iter().collect();
        seek_leaf_values_in_object(value, &selectors, "", &seeker);
    }

    pub fn seek_leaf_values_in_object(
        value: &Map<String, Value>,
        selectors: &[&str],
        base_key: &str,
        seeker: &impl Fn(&str, &Value),
    ) {
        for (key, value) in value.iter() {
            let base_key = if base_key.is_empty() {
                key.to_string()
            } else {
                format!("{}{}{}", base_key, SPLIT_SYMBOL, key)
            };

            // here if the user only specified `doggo` we need to iterate in all the fields of `doggo`
            // so we check the contained_in on both side
            let should_continue = selectors.iter().any(|selector| {
                contained_in(selector, &base_key) || contained_in(&base_key, selector)
            });

            if should_continue {
                match value {
                    Value::Object(object) => {
                        seek_leaf_values_in_object(object, selectors, &base_key, seeker)
                    }
                    Value::Array(array) => {
                        seek_leaf_values_in_array(array, selectors, &base_key, seeker)
                    }
                    value => seeker(&base_key, value),
                }
            }
        }
    }

    pub fn seek_leaf_values_in_array(
        values: &mut [Value],
        selectors: &[&str],
        base_key: &str,
        seeker: &impl Fn(&str, &Value),
    ) {
        for value in values.iter_mut() {
            match value {
                Value::Object(object) => {
                    seek_leaf_values_in_object(object, selectors, base_key, seeker)
                }
                Value::Array(array) => {
                    seek_leaf_values_in_array(array, selectors, base_key, seeker)
                }
                value => seeker(base_key, value),
            }
        }
    }
}
