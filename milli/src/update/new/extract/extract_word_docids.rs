pub fn extract_word_docids(
    document_change: DocumentChange,
    _tokenizer: &Tokenizer,
    output: &mut CachedSorter<DelAddRoaringBitmapMerger>,
) -> grenad::Result<(), io::Error> {
    match document_change {
        DocumentChange::Deletion(inner) => {
            unimplemented!()
        }
        DocumentChange::Update(inner) => {
            unimplemented!()
        }
        DocumentChange::Insertion(inner) => {
            unimplemented!()
        }
    }

    let normalizer_options = NormalizerOption::default();

    if let Some(previous_doc) = previous_doc {
        for (_, v) in previous_doc.iter() {
            // Only manage the direct JSON strings
            // TODO manage the JSON strings correctly (escaped chars)
            if v.first().zip(v.last()) == Some((&b'"', &b'"')) {
                let s = std::str::from_utf8(&v[1..v.len() - 1]).unwrap();
                // for token in tokenizer.tokenize(s).filter(|t| t.is_word()) {
                //     let key = token.lemma().normalize(&normalizer_options);
                for token in s.split_whitespace() {
                    let key = token.normalize(&normalizer_options);
                    output.insert_del_u32(key.as_bytes(), docid)?;
                }
            }
        }
    }

    for (_, v) in new_doc.iter() {
        // Only manage the direct JSON strings
        // TODO manage the JSON strings correctly (escaped chars)
        if v.first().zip(v.last()) == Some((&b'"', &b'"')) {
            let s = std::str::from_utf8(&v[1..v.len() - 1]).unwrap();
            // for token in tokenizer.tokenize(s).filter(|t| t.is_word()) {
            //     let key = token.lemma().normalize(&normalizer_options);
            for token in s.split_whitespace() {
                let key = token.normalize(&normalizer_options);
                output.insert_add_u32(key.as_bytes(), docid)?;
            }
        }
    }

    Ok(())
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
