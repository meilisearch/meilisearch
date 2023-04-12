use itertools::Itertools;

use crate::{search::new::interner::Interned, SearchContext};

/// A phrase in the user's search query, consisting of several words
/// that must appear side-by-side in the search results.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct Phrase {
    pub words: Vec<Option<Interned<String>>>,
}
impl Interned<Phrase> {
    pub fn description(self, ctx: &SearchContext) -> String {
        let p = ctx.phrase_interner.get(self);
        p.words.iter().flatten().map(|w| ctx.word_interner.get(*w)).join(" ")
    }
    pub fn words(self, ctx: &SearchContext) -> Vec<Option<Interned<String>>> {
        let p = ctx.phrase_interner.get(self);
        p.words.clone()
    }
}
