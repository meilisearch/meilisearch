#![cfg_attr(feature = "nightly", feature(test))]

pub mod database;
pub mod tokenizer;
mod common_words;

pub use rocksdb;

pub use self::tokenizer::Tokenizer;
pub use self::common_words::CommonWords;

pub fn is_cjk(c: char) -> bool {
    (c >= '\u{2e80}' && c <= '\u{2eff}') ||
    (c >= '\u{2f00}' && c <= '\u{2fdf}') ||
    (c >= '\u{3040}' && c <= '\u{309f}') ||
    (c >= '\u{30a0}' && c <= '\u{30ff}') ||
    (c >= '\u{3100}' && c <= '\u{312f}') ||
    (c >= '\u{3200}' && c <= '\u{32ff}') ||
    (c >= '\u{3400}' && c <= '\u{4dbf}') ||
    (c >= '\u{4e00}' && c <= '\u{9fff}') ||
    (c >= '\u{f900}' && c <= '\u{faff}')
}
