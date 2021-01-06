pub mod authentication;
pub mod meilisearch;
pub mod normalize_path;
pub mod compression;

pub use authentication::Authentication;
pub use normalize_path::NormalizePath;

pub fn is_cjk(c: char) -> bool {
    ('\u{1100}'..'\u{11ff}').contains(&c)  // Hangul Jamo
        || ('\u{2e80}'..'\u{2eff}').contains(&c)  // CJK Radicals Supplement
        || ('\u{2f00}'..'\u{2fdf}').contains(&c) // Kangxi radical
        || ('\u{3000}'..'\u{303f}').contains(&c) // Japanese-style punctuation
        || ('\u{3040}'..'\u{309f}').contains(&c) // Japanese Hiragana
        || ('\u{30a0}'..'\u{30ff}').contains(&c) // Japanese Katakana
        || ('\u{3100}'..'\u{312f}').contains(&c)
        || ('\u{3130}'..'\u{318F}').contains(&c) // Hangul Compatibility Jamo
        || ('\u{3200}'..'\u{32ff}').contains(&c) // Enclosed CJK Letters and Months
        || ('\u{3400}'..'\u{4dbf}').contains(&c) // CJK Unified Ideographs Extension A
        || ('\u{4e00}'..'\u{9fff}').contains(&c) // CJK Unified Ideographs
        || ('\u{a960}'..'\u{a97f}').contains(&c) // Hangul Jamo Extended-A
        || ('\u{ac00}'..'\u{d7a3}').contains(&c) // Hangul Syllables
        || ('\u{d7b0}'..'\u{d7ff}').contains(&c) // Hangul Jamo Extended-B
        || ('\u{f900}'..'\u{faff}').contains(&c) // CJK Compatibility Ideographs
        || ('\u{ff00}'..'\u{ffef}').contains(&c) // Full-width roman characters and half-width katakana
}
