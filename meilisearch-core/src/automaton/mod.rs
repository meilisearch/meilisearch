mod dfa;
mod query_enhancer;

use meilisearch_tokenizer::is_cjk;

pub use self::dfa::{build_dfa, build_prefix_dfa, build_exact_dfa};
pub use self::query_enhancer::QueryEnhancer;
pub use self::query_enhancer::QueryEnhancerBuilder;

pub const NGRAMS: usize = 3;

pub fn normalize_str(string: &str) -> String {
    let mut string = string.to_lowercase();

    if !string.contains(is_cjk) {
        string = deunicode::deunicode_with_tofu(&string, "");
    }

    string
}
