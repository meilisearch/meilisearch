pub mod authentication;
pub mod meilisearch;
pub mod normalize_path;
pub mod compression;

pub use authentication::Authentication;
pub use normalize_path::NormalizePath;

pub fn is_cjk(c: char) -> bool {
    (c >= '\u{1100}' && c <= '\u{11ff}')  // Hangul Jamo
        || (c >= '\u{2e80}' && c <= '\u{2eff}')  // CJK Radicals Supplement
        || (c >= '\u{2f00}' && c <= '\u{2fdf}') // Kangxi radical
        || (c >= '\u{3000}' && c <= '\u{303f}') // Japanese-style punctuation
        || (c >= '\u{3040}' && c <= '\u{309f}') // Japanese Hiragana
        || (c >= '\u{30a0}' && c <= '\u{30ff}') // Japanese Katakana
        || (c >= '\u{3100}' && c <= '\u{312f}')
        || (c >= '\u{3130}' && c <= '\u{318F}') // Hangul Compatibility Jamo
        || (c >= '\u{3200}' && c <= '\u{32ff}') // Enclosed CJK Letters and Months
        || (c >= '\u{3400}' && c <= '\u{4dbf}') // CJK Unified Ideographs Extension A
        || (c >= '\u{4e00}' && c <= '\u{9fff}') // CJK Unified Ideographs
        || (c >= '\u{a960}' && c <= '\u{a97f}') // Hangul Jamo Extended-A
        || (c >= '\u{ac00}' && c <= '\u{d7a3}') // Hangul Syllables
        || (c >= '\u{d7b0}' && c <= '\u{d7ff}') // Hangul Jamo Extended-B
        || (c >= '\u{f900}' && c <= '\u{faff}') // CJK Compatibility Ideographs
        || (c >= '\u{ff00}' && c <= '\u{ffef}') // Full-width roman characters and half-width katakana
}
