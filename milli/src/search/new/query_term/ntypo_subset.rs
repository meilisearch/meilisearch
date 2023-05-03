use std::collections::BTreeSet;

use super::Phrase;
use crate::search::new::interner::Interned;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NTypoTermSubset {
    All,
    Subset {
        words: BTreeSet<Interned<String>>,
        phrases: BTreeSet<Interned<Phrase>>,
        // TODO: prefixes: BTreeSet<Interned<String>>,
    },
    Nothing,
}

impl NTypoTermSubset {
    pub fn contains_word(&self, word: Interned<String>) -> bool {
        match self {
            NTypoTermSubset::All => true,
            NTypoTermSubset::Subset { words, phrases: _ } => words.contains(&word),
            NTypoTermSubset::Nothing => false,
        }
    }
    pub fn contains_phrase(&self, phrase: Interned<Phrase>) -> bool {
        match self {
            NTypoTermSubset::All => true,
            NTypoTermSubset::Subset { words: _, phrases } => phrases.contains(&phrase),
            NTypoTermSubset::Nothing => false,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            NTypoTermSubset::All => false,
            NTypoTermSubset::Subset { words, phrases } => words.is_empty() && phrases.is_empty(),
            NTypoTermSubset::Nothing => true,
        }
    }
    pub fn union(&mut self, other: &Self) {
        match self {
            Self::All => {}
            Self::Subset { words, phrases } => match other {
                Self::All => {
                    *self = Self::All;
                }
                Self::Subset { words: w2, phrases: p2 } => {
                    words.extend(w2);
                    phrases.extend(p2);
                }
                Self::Nothing => {}
            },
            Self::Nothing => {
                *self = other.clone();
            }
        }
    }
    pub fn intersect(&mut self, other: &Self) {
        match self {
            Self::All => *self = other.clone(),
            Self::Subset { words, phrases } => match other {
                Self::All => {}
                Self::Subset { words: w2, phrases: p2 } => {
                    let mut ws = BTreeSet::new();
                    for w in words.intersection(w2) {
                        ws.insert(*w);
                    }
                    let mut ps = BTreeSet::new();
                    for p in phrases.intersection(p2) {
                        ps.insert(*p);
                    }
                    *words = ws;
                    *phrases = ps;
                }
                Self::Nothing => *self = Self::Nothing,
            },
            Self::Nothing => {}
        }
    }
}
