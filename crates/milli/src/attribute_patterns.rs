use deserr::Deserr;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Deserr, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub struct AttributePatterns {
    #[schema(value_type = Vec<String>)]
    pub patterns: Vec<String>,
}

impl From<Vec<String>> for AttributePatterns {
    fn from(patterns: Vec<String>) -> Self {
        Self { patterns }
    }
}

impl AttributePatterns {
    pub fn match_str(&self, str: &str) -> bool {
        self.patterns.iter().any(|pattern| match_pattern(pattern, str))
    }
}

fn match_pattern(pattern: &str, str: &str) -> bool {
    if pattern == "*" {
        true
    } else if pattern.starts_with('*') && pattern.ends_with('*') {
        str.contains(&pattern[1..pattern.len() - 1])
    } else if let Some(pattern) = pattern.strip_prefix('*') {
        str.ends_with(pattern)
    } else if let Some(pattern) = pattern.strip_suffix('*') {
        str.starts_with(pattern)
    } else {
        pattern == str
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_pattern() {
        assert!(match_pattern("*", "test"));
        assert!(match_pattern("test*", "test"));
        assert!(match_pattern("test*", "testa"));
        assert!(match_pattern("*test", "test"));
        assert!(match_pattern("*test", "atest"));
        assert!(match_pattern("*test*", "test"));
        assert!(match_pattern("*test*", "atesta"));
        assert!(match_pattern("*test*", "atest"));
        assert!(match_pattern("*test*", "testa"));
        assert!(!match_pattern("test*test", "test"));
        assert!(!match_pattern("*test", "testa"));
        assert!(!match_pattern("test*", "atest"));
    }
}
