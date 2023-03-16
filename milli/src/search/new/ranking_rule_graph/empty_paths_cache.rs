use super::{path_set::PathSet, RankingRuleGraphTrait};
use crate::search::new::{
    interner::{FixedSizeInterner, Interned, MappedInterner},
    small_bitmap::SmallBitmap,
};

/// A cache which stores sufficient conditions for a path
/// to resolve to an empty set of candidates within the current
/// universe.
pub struct DeadEndPathCache<G: RankingRuleGraphTrait> {
    /// The set of edge conditions that resolve to no documents.
    pub conditions: SmallBitmap<G::EdgeCondition>,
    /// A set of path prefixes that resolve to no documents.
    pub prefixes: PathSet<G::EdgeCondition>,
    /// A set of empty couples of edge conditions that resolve to no documents.
    pub condition_couples: MappedInterner<SmallBitmap<G::EdgeCondition>, G::EdgeCondition>,
}
impl<G: RankingRuleGraphTrait> Clone for DeadEndPathCache<G> {
    fn clone(&self) -> Self {
        Self {
            conditions: self.conditions.clone(),
            prefixes: self.prefixes.clone(),
            condition_couples: self.condition_couples.clone(),
        }
    }
}

impl<G: RankingRuleGraphTrait> DeadEndPathCache<G> {
    /// Create a new cache for a ranking rule graph containing at most `all_edges_len` edges.
    pub fn new(all_edge_conditions: &FixedSizeInterner<G::EdgeCondition>) -> Self {
        Self {
            conditions: SmallBitmap::for_interned_values_in(all_edge_conditions),
            prefixes: PathSet::default(),
            condition_couples: all_edge_conditions
                .map(|_| SmallBitmap::for_interned_values_in(all_edge_conditions)),
        }
    }

    /// Store in the cache that every path containing the given edge resolves to no documents.
    pub fn add_condition(&mut self, condition: Interned<G::EdgeCondition>) {
        self.conditions.insert(condition);
        self.condition_couples.get_mut(condition).clear();
        self.prefixes.remove_edge(condition);
        for (_, edges2) in self.condition_couples.iter_mut() {
            edges2.remove(condition);
        }
    }
    /// Store in the cache that every path containing the given prefix resolves to no documents.
    pub fn add_prefix(&mut self, prefix: &[Interned<G::EdgeCondition>]) {
        // TODO: typed PathSet
        self.prefixes.insert(prefix.iter().copied());
    }

    /// Store in the cache that every path containing the two given edges resolves to no documents.
    pub fn add_condition_couple(
        &mut self,
        edge1: Interned<G::EdgeCondition>,
        edge2: Interned<G::EdgeCondition>,
    ) {
        self.condition_couples.get_mut(edge1).insert(edge2);
    }

    /// Returns true if the cache can determine that the given path resolves to no documents.
    pub fn path_is_dead_end(
        &self,
        path: &[Interned<G::EdgeCondition>],
        path_bitmap: &SmallBitmap<G::EdgeCondition>,
    ) -> bool {
        if path_bitmap.intersects(&self.conditions) {
            return true;
        }
        for condition in path.iter() {
            // TODO: typed path
            let forbidden_other_edges = self.condition_couples.get(*condition);
            if path_bitmap.intersects(forbidden_other_edges) {
                return true;
            }
        }
        if self.prefixes.contains_prefix_of_path(path) {
            return true;
        }
        false
    }
}
