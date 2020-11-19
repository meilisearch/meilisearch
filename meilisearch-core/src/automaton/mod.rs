mod dfa;


pub use self::dfa::{build_dfa, build_prefix_dfa, build_exact_dfa};

pub fn normalize_str(string: &str) -> String {
    let mut string = string.to_lowercase();

    if !string.contains(is_cjk) {
        string = deunicode::deunicode_with_tofu(&string, "");
    }

    string
}
