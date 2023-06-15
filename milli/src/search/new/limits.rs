/// Maximum number of tokens we consider in a single search.
pub const MAX_TOKEN_COUNT: usize = 1_000;

/// Maximum number of prefixes that can be derived from a single word.
pub const MAX_PREFIX_COUNT: usize = 1_000;
/// Maximum number of words that can be derived from a single word with a distance of one to that word.
pub const MAX_ONE_TYPO_COUNT: usize = 150;
/// Maximum number of words that can be derived from a single word with a distance of two to that word.
pub const MAX_TWO_TYPOS_COUNT: usize = 50;

/// Maximum amount of synonym phrases that can be derived from a single word.
pub const MAX_SYNONYM_PHRASE_COUNT: usize = 50;

/// Maximum amount of words inside of all the synonym phrases that can be derived from a single word.
///
/// This limit is meant to gracefully handle the case where a word would have very long phrases as synonyms.
pub const MAX_SYNONYM_WORD_COUNT: usize = 100;
