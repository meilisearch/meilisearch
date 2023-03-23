use std::marker::PhantomData;

use fxhash::{FxHashMap, FxHashSet};
use roaring::RoaringBitmap;

use super::{RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interned;
use crate::search::new::query_term::Phrase;
use crate::search::new::SearchContext;
use crate::Result;

// TODO: give a generation to each universe, then be able to get the exact
// delta of docids between two universes of different generations!

#[derive(Default)]
pub struct ComputedCondition {
    docids: RoaringBitmap,
    universe_len: u64,
    used_words: FxHashSet<Interned<String>>,
    used_phrases: FxHashSet<Interned<Phrase>>,
}

/// A cache storing the document ids associated with each ranking rule edge
pub struct ConditionDocIdsCache<G: RankingRuleGraphTrait> {
    pub cache: FxHashMap<Interned<G::Condition>, ComputedCondition>,
    _phantom: PhantomData<G>,
}
impl<G: RankingRuleGraphTrait> Default for ConditionDocIdsCache<G> {
    fn default() -> Self {
        Self { cache: Default::default(), _phantom: Default::default() }
    }
}
impl<G: RankingRuleGraphTrait> ConditionDocIdsCache<G> {
    pub fn get_condition_used_words_and_phrases(
        &mut self,
        interned_condition: Interned<G::Condition>,
    ) -> (&FxHashSet<Interned<String>>, &FxHashSet<Interned<Phrase>>) {
        let ComputedCondition { used_words, used_phrases, .. } = &self.cache[&interned_condition];
        (used_words, used_phrases)
    }

    /// Retrieve the document ids for the given edge condition.
    ///
    /// If the cache does not yet contain these docids, they are computed
    /// and inserted in the cache.
    pub fn get_condition_docids<'s>(
        &'s mut self,
        ctx: &mut SearchContext,
        interned_condition: Interned<G::Condition>,
        graph: &mut RankingRuleGraph<G>,
        universe: &RoaringBitmap,
    ) -> Result<&'s RoaringBitmap> {
        if self.cache.contains_key(&interned_condition) {
            // TODO compare length of universe compared to the one in self
            // if it is smaller, then update the value
            let ComputedCondition { docids, universe_len, .. } =
                self.cache.entry(interned_condition).or_default();
            if *universe_len == universe.len() {
                return Ok(docids);
            } else {
                *docids &= universe;
                *universe_len = universe.len();
                return Ok(docids);
            }
        }
        let condition = graph.conditions_interner.get_mut(interned_condition);
        let (docids, used_words, used_phrases) = G::resolve_condition(ctx, condition, universe)?;
        let _ = self.cache.insert(
            interned_condition,
            ComputedCondition { docids, universe_len: universe.len(), used_words, used_phrases },
        );
        let ComputedCondition { docids, .. } = &self.cache[&interned_condition];
        Ok(docids)
    }
}
