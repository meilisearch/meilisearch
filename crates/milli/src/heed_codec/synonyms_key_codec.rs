use std::borrow::Cow;
use std::{fmt, str};

use heed::BoxedError;

/// This ascii character is used as a seperator for fields and
/// we use it as a way to separate the different words in synonyms.
///
/// <https://en.wikipedia.org/wiki/C0_and_C1_control_codes#Field_separators>
const UNIT_SEPARATOR: u8 = b'\x1F';

pub struct SynonymsKeyCodec;

impl<'a> heed::BytesEncode<'a> for SynonymsKeyCodec {
    type EItem = [&'a str];

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        if item.iter().any(|s| s.contains(char::from(UNIT_SEPARATOR))) {
            return Err(SynonymContainsUnitSeparator.into());
        }

        match item {
            [] => Ok(Cow::Borrowed(&[])),
            [single] => Ok(Cow::Borrowed(single.as_bytes())),
            item => {
                // The length of all words + the delimiters
                let length = item.len();
                let delimiters = length.saturating_sub(1);
                let capacity = item.iter().map(|s| s.len()).sum::<usize>() + delimiters;
                let mut encoded = Vec::with_capacity(capacity);

                for (i, word) in item.iter().enumerate() {
                    encoded.extend(word.as_bytes());
                    // Make sure not to put a separator at the end of list
                    // This way we make sure not to encode empty words
                    if i != length.saturating_sub(1) {
                        encoded.push(UNIT_SEPARATOR);
                    }
                }

                Ok(Cow::Owned(encoded))
            }
        }
    }
}

impl<'a> heed::BytesDecode<'a> for SynonymsKeyCodec {
    type DItem = Vec<&'a str>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        // Note that the Unit Separator (UB) ascii character is valid UTF-8
        let word_with_separators = str::from_utf8(bytes)?;
        Ok(word_with_separators.split(char::from(UNIT_SEPARATOR)).collect())
    }
}

#[derive(Debug, Clone)]
pub struct SynonymContainsUnitSeparator;

impl fmt::Display for SynonymContainsUnitSeparator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Synonym contains invalid Unit Separator (UB) ascii character")
    }
}

impl std::error::Error for SynonymContainsUnitSeparator {}
