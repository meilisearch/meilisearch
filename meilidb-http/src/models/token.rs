use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const TOKEN_PREFIX_KEY: &str = "_token_";

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ACL {
    IndexesRead,
    IndexesWrite,
    DocumentsRead,
    DocumentsWrite,
    SettingsRead,
    SettingsWrite,
    Admin,
    #[serde(rename = "*")]
    All,
}

pub type Wildcard = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Token {
    pub key: String,
    pub description: String,
    pub acl: Vec<ACL>,
    pub indexes: Vec<Wildcard>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub revoked: bool,
}

fn cleanup_wildcard(input: &str) -> (bool, &str, bool) {
    let first = input.chars().next().filter(|&c| c == '*').is_some();
    let last = input.chars().last().filter(|&c| c == '*').is_some();
    let bound_last = std::cmp::max(input.len().saturating_sub(last as usize), first as usize);
    let output = input.get(first as usize..bound_last).unwrap();
    (first, output, last)
}

pub fn match_wildcard(pattern: &str, input: &str) -> bool {
    let (first, pattern, last) = cleanup_wildcard(pattern);

    match (first, last) {
        (false, false) => pattern == input,
        (true, false) => input.ends_with(pattern),
        (false, true) => input.starts_with(pattern),
        (true, true) => input.contains(pattern),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_wildcard() {
        assert!(match_wildcard("*", "qqq"));
        assert!(match_wildcard("*", ""));
        assert!(match_wildcard("*ab", "qqqab"));
        assert!(match_wildcard("*ab*", "qqqabqq"));
        assert!(match_wildcard("ab*", "abqqq"));
        assert!(match_wildcard("**", "ab"));
        assert!(match_wildcard("ab", "ab"));
        assert!(match_wildcard("ab*", "ab"));
        assert!(match_wildcard("*ab", "ab"));
        assert!(match_wildcard("*ab*", "ab"));
        assert!(match_wildcard("*😆*", "ab😆dsa"));
    }
}
