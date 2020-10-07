use std::{str, iter, mem};

use fst::raw::{Fst, Output};
use once_cell::sync::Lazy;
use slice_group_by::StrGroupBy;

use CharCategory::*;

const CHINESE_FST_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/chinese-words.fst"));
static CHINESE_WORDS_FST: Lazy<Fst<&[u8]>> = Lazy::new(|| Fst::new(CHINESE_FST_BYTES).unwrap());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Word,
    Space,
    Other,
}

pub fn simple_tokenizer(text: &str) -> impl Iterator<Item=(TokenType, &str)> {
    text
        .linear_group_by_key(CharCategory::new)
        .flat_map(|mut string| {
            let first = string.chars().next().unwrap();
            let category = CharCategory::new(first);
            iter::from_fn(move || {
                if string.is_empty() { return None }
                match category {
                    Chinese => {
                        let fst = &CHINESE_WORDS_FST;
                        match find_longest_prefix(fst, string.as_bytes()) {
                            Some((_, l)) => {
                                let s = &string[..l];
                                string = &string[l..];
                                Some((TokenType::Word, s))
                            },
                            None => {
                                let first = string.chars().next().unwrap();
                                let len = first.len_utf8();
                                let (head, tail) = string.split_at(len);
                                string = tail;
                                Some((TokenType::Word, head))
                            },
                        }
                    },
                    Alphanumeric => Some((TokenType::Word, mem::take(&mut string))),
                    Space => Some((TokenType::Space, mem::take(&mut string))),
                    Other => Some((TokenType::Other, mem::take(&mut string))),
                }
            })
        })
}

pub fn only_token((t, w): (TokenType, &str)) -> Option<&str> {
    if t == TokenType::Word { Some(w) } else { None }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum CharCategory {
    Chinese,
    Alphanumeric,
    Space,
    Other,
}

impl CharCategory {
    fn new(c: char) -> Self {
        if c.is_alphanumeric() {
            if is_chinese(c) { Chinese } else { Alphanumeric }
        } else {
            if c.is_whitespace() { Space } else { Other }
        }
    }
}

fn is_chinese(c: char) -> bool {
    match u32::from(c) {
          0x4E00..=0x9FEF
        | 0x3400..=0x4DBF
        | 0x20000..=0x2A6DF
        | 0x2A700..=0x2B73F
        | 0x2B740..=0x2B81F
        | 0x2B820..=0x2CEAF
        | 0x2CEB0..=0x2EBEF
        | 0x3007..=0x3007 => true,
        _ => false,
    }
}

/// Find the longest key that is prefix of the given value.
///
/// If the key exists, then `Some((value, key_len))` is returned, where
/// `value` is the value associated with the key, and `key_len` is the
/// length of the found key. Otherwise `None` is returned.
///
/// This can be used to e.g. build tokenizing functions.
// Copyright @llogiq
// https://github.com/BurntSushi/fst/pull/104
#[inline]
fn find_longest_prefix(fst: &Fst<&[u8]>, value: &[u8]) -> Option<(u64, usize)> {
    let mut node = fst.root();
    let mut out = Output::zero();
    let mut last_match = None;
    for (i, &b) in value.iter().enumerate() {
        if let Some(trans_index) = node.find_input(b) {
            let t = node.transition(trans_index);
            node = fst.node(t.addr);
            out = out.cat(t.out);
            if node.is_final() {
                last_match = Some((out.cat(node.final_output()).value(), i + 1));
            }
        } else {
            return last_match;
        }
    }
    last_match
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_chinese() {
        let mut iter = simple_tokenizer("hello world!");
        assert_eq!(iter.next(), Some((TokenType::Word, "hello")));
        assert_eq!(iter.next(), Some((TokenType::Space, " ")));
        assert_eq!(iter.next(), Some((TokenType::Word, "world")));
        assert_eq!(iter.next(), Some((TokenType::Other, "!")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn only_chinese() {
        let mut iter = simple_tokenizer("今天的天气真好");
        assert_eq!(iter.next(), Some((TokenType::Word, "今天")));
        assert_eq!(iter.next(), Some((TokenType::Word, "的")));
        assert_eq!(iter.next(), Some((TokenType::Word, "天气")));
        assert_eq!(iter.next(), Some((TokenType::Word, "真好")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn mixup_chinese_with_alphabet() {
        let mut iter = simple_tokenizer("今天的天气真好Apple is good今天的天气真好");
        assert_eq!(iter.next(), Some((TokenType::Word, "今天")));
        assert_eq!(iter.next(), Some((TokenType::Word, "的")));
        assert_eq!(iter.next(), Some((TokenType::Word, "天气")));
        assert_eq!(iter.next(), Some((TokenType::Word, "真好")));
        assert_eq!(iter.next(), Some((TokenType::Word, "Apple")));
        assert_eq!(iter.next(), Some((TokenType::Space, " ")));
        assert_eq!(iter.next(), Some((TokenType::Word, "is")));
        assert_eq!(iter.next(), Some((TokenType::Space, " ")));
        assert_eq!(iter.next(), Some((TokenType::Word, "good")));
        assert_eq!(iter.next(), Some((TokenType::Word, "今天")));
        assert_eq!(iter.next(), Some((TokenType::Word, "的")));
        assert_eq!(iter.next(), Some((TokenType::Word, "天气")));
        assert_eq!(iter.next(), Some((TokenType::Word, "真好")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn unknown_chinese() {
        let mut iter = simple_tokenizer("被虾头大讚好识𠱁女仔");
        assert_eq!(iter.next(), Some((TokenType::Word, "被")));
        assert_eq!(iter.next(), Some((TokenType::Word, "虾")));
        assert_eq!(iter.next(), Some((TokenType::Word, "头")));
        assert_eq!(iter.next(), Some((TokenType::Word, "大")));
        assert_eq!(iter.next(), Some((TokenType::Word, "讚")));
        assert_eq!(iter.next(), Some((TokenType::Word, "好")));
        assert_eq!(iter.next(), Some((TokenType::Word, "识")));
        assert_eq!(iter.next(), Some((TokenType::Word, "𠱁")));
        assert_eq!(iter.next(), Some((TokenType::Word, "女")));
        assert_eq!(iter.next(), Some((TokenType::Word, "仔")));
        assert_eq!(iter.next(), None);
    }
}
