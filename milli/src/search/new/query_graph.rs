use std::cmp::{Ordering, Reverse};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use fxhash::{FxHashMap, FxHasher};
use roaring::RoaringBitmap;

use super::interner::{FixedSizeInterner, Interned};
use super::query_term::{
    self, number_of_typos_allowed, LocatedQueryTerm, LocatedQueryTermSubset, QueryTermSubset,
};
use super::small_bitmap::SmallBitmap;
use super::SearchContext;
use crate::search::new::interner::Interner;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids;
use crate::Result;

/// A node of the [`QueryGraph`].
///
/// There are four types of nodes:
/// 1. `Start` : unique, represents the start of the query
/// 2. `End` : unique, represents the end of a query
/// 3. `Deleted` : represents a node that was deleted.
/// All deleted nodes are unreachable from the start node.
/// 4. `Term` is a regular node representing a word or combination of words
/// from the user query.
#[derive(Clone)]
pub struct QueryNode {
    pub data: QueryNodeData,
    pub predecessors: SmallBitmap<QueryNode>,
    pub successors: SmallBitmap<QueryNode>,
}
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum QueryNodeData {
    Term(LocatedQueryTermSubset),
    Deleted,
    Start,
    End,
}

/**
A graph representing all the ways to interpret the user's search query.

## Example 1
For the search query `sunflower`, we need to register the following things:
- we need to look for the exact word `sunflower`
- but also any word which is 1 or 2 typos apart from `sunflower`
- and every word that contains the prefix `sunflower`
- and also the couple of adjacent words `sun flower`
- as well as all the user-defined synonyms of `sunflower`

All these derivations of a word will be stored in [`QueryTerm`].

## Example 2:
For the search query `summer house by`.

We also look for all word derivations of each term. And we also need to consider
the potential n-grams `summerhouse`, `summerhouseby`, and `houseby`.
Furthermore, we need to know which words these ngrams replace. This is done by creating the
following graph, where each node also contains a list of derivations:
```txt
                        ┌───────┐
                      ┌─│houseby│─────────┐
                      │ └───────┘         │
┌───────┐   ┌───────┐ │ ┌───────┐  ┌────┐ │ ┌───────┐
│ START │─┬─│summer │─┴─│ house │┌─│ by │─┼─│  END  │
└───────┘ │ └───────┘   └───────┘│ └────┘ │ └───────┘
          │ ┌────────────┐       │        │
          ├─│summerhouse │───────┘        │
          │ └────────────┘                │
          │         ┌─────────────┐       │
          └─────────│summerhouseby│───────┘
                    └─────────────┘
```
Note also that each node has a range of positions associated with it,
such that `summer` is known to be a word at the positions `0..=0` and `houseby`
is registered with the positions `1..=2`. When two nodes are connected by an edge,
it means that they are potentially next to each other in the user's search query
(depending on the [`TermsMatchingStrategy`](crate::search::TermsMatchingStrategy)
and the transformations that were done on the query graph).
*/
#[derive(Clone)]
pub struct QueryGraph {
    /// The index of the start node within `self.nodes`
    pub root_node: Interned<QueryNode>,
    /// The index of the end node within `self.nodes`
    pub end_node: Interned<QueryNode>,
    /// The list of all query nodes
    pub nodes: FixedSizeInterner<QueryNode>,
}

impl QueryGraph {
    /// Build the query graph from the parsed user search query, return an updated list of the located query terms
    /// which contains ngrams.
    pub fn from_query(
        ctx: &mut SearchContext,
        // The terms here must be consecutive
        terms: &[LocatedQueryTerm],
    ) -> Result<(QueryGraph, Vec<LocatedQueryTerm>)> {
        let mut new_located_query_terms = terms.to_vec();

        let nbr_typos = number_of_typos_allowed(ctx)?;

        let mut nodes_data: Vec<QueryNodeData> = vec![QueryNodeData::Start, QueryNodeData::End];
        let root_node = 0;
        let end_node = 1;

        // Ee could consider generalizing to 4,5,6,7,etc. ngrams
        let (mut prev2, mut prev1, mut prev0): (Vec<u16>, Vec<u16>, Vec<u16>) =
            (vec![], vec![], vec![root_node]);

        let original_terms_len = terms.len();
        for term_idx in 0..original_terms_len {
            let mut new_nodes = vec![];

            let new_node_idx = add_node(
                &mut nodes_data,
                QueryNodeData::Term(LocatedQueryTermSubset {
                    term_subset: QueryTermSubset::full(terms[term_idx].value),
                    positions: terms[term_idx].positions.clone(),
                    term_ids: term_idx as u8..=term_idx as u8,
                }),
            );
            new_nodes.push(new_node_idx);

            if !prev1.is_empty() {
                if let Some(ngram) =
                    query_term::make_ngram(ctx, &terms[term_idx - 1..=term_idx], &nbr_typos)?
                {
                    new_located_query_terms.push(ngram.clone());
                    let ngram_idx = add_node(
                        &mut nodes_data,
                        QueryNodeData::Term(LocatedQueryTermSubset {
                            term_subset: QueryTermSubset::full(ngram.value),
                            positions: ngram.positions,
                            term_ids: term_idx as u8 - 1..=term_idx as u8,
                        }),
                    );
                    new_nodes.push(ngram_idx);
                }
            }
            if !prev2.is_empty() {
                if let Some(ngram) =
                    query_term::make_ngram(ctx, &terms[term_idx - 2..=term_idx], &nbr_typos)?
                {
                    new_located_query_terms.push(ngram.clone());
                    let ngram_idx = add_node(
                        &mut nodes_data,
                        QueryNodeData::Term(LocatedQueryTermSubset {
                            term_subset: QueryTermSubset::full(ngram.value),
                            positions: ngram.positions,
                            term_ids: term_idx as u8 - 2..=term_idx as u8,
                        }),
                    );
                    new_nodes.push(ngram_idx);
                }
            }
            (prev0, prev1, prev2) = (new_nodes, prev0, prev1);
        }

        let root_node = Interned::from_raw(root_node);
        let end_node = Interned::from_raw(end_node);
        let mut nodes = FixedSizeInterner::new(
            nodes_data.len() as u16,
            QueryNode {
                data: QueryNodeData::Deleted,
                predecessors: SmallBitmap::new(nodes_data.len() as u16),
                successors: SmallBitmap::new(nodes_data.len() as u16),
            },
        );
        for (node_idx, node_data) in nodes_data.into_iter().enumerate() {
            let node = nodes.get_mut(Interned::from_raw(node_idx as u16));
            node.data = node_data;
        }
        let mut graph = QueryGraph { root_node, end_node, nodes };
        graph.build_initial_edges();

        Ok((graph, new_located_query_terms))
    }

    /// Remove the given nodes, connecting all their predecessors to all their successors.
    pub fn remove_nodes_keep_edges(&mut self, nodes: &[Interned<QueryNode>]) {
        for &node_id in nodes {
            let node = self.nodes.get(node_id);
            let old_node_pred = node.predecessors.clone();
            let old_node_succ = node.successors.clone();
            for pred in old_node_pred.iter() {
                let pred_successors = &mut self.nodes.get_mut(pred).successors;
                pred_successors.remove(node_id);
                pred_successors.union(&old_node_succ);
            }
            for succ in old_node_succ.iter() {
                let succ_predecessors = &mut self.nodes.get_mut(succ).predecessors;
                succ_predecessors.remove(node_id);
                succ_predecessors.union(&old_node_pred);
            }
            let node = self.nodes.get_mut(node_id);
            node.data = QueryNodeData::Deleted;
            node.predecessors.clear();
            node.successors.clear();
        }
    }

    /// Remove the given nodes and all their edges from the query graph.
    pub fn remove_nodes(&mut self, nodes: &[Interned<QueryNode>]) {
        for &node_id in nodes {
            let node = &self.nodes.get(node_id);
            let old_node_pred = node.predecessors.clone();
            let old_node_succ = node.successors.clone();

            for pred in old_node_pred.iter() {
                self.nodes.get_mut(pred).successors.remove(node_id);
            }
            for succ in old_node_succ.iter() {
                self.nodes.get_mut(succ).predecessors.remove(node_id);
            }

            let node = self.nodes.get_mut(node_id);
            node.data = QueryNodeData::Deleted;
            node.predecessors.clear();
            node.successors.clear();
        }
    }
    /// Simplify the query graph by removing all nodes that are disconnected from
    /// the start or end nodes.
    pub fn simplify(&mut self) {
        loop {
            let mut nodes_to_remove = vec![];
            for (node_idx, node) in self.nodes.iter() {
                if (!matches!(node.data, QueryNodeData::End | QueryNodeData::Deleted)
                    && node.successors.is_empty())
                    || (!matches!(node.data, QueryNodeData::Start | QueryNodeData::Deleted)
                        && node.predecessors.is_empty())
                {
                    nodes_to_remove.push(node_idx);
                }
            }
            if nodes_to_remove.is_empty() {
                break;
            } else {
                self.remove_nodes(&nodes_to_remove);
            }
        }
    }

    fn build_initial_edges(&mut self) {
        for (_, node) in self.nodes.iter_mut() {
            node.successors.clear();
            node.predecessors.clear();
        }
        for node_id in self.nodes.indexes() {
            let node = self.nodes.get(node_id);
            let end_prev_term_id = match &node.data {
                QueryNodeData::Term(term) => *term.term_ids.end() as i16,
                QueryNodeData::Start => -1,
                QueryNodeData::Deleted => continue,
                QueryNodeData::End => continue,
            };
            let successors = {
                let mut successors = SmallBitmap::for_interned_values_in(&self.nodes);
                let mut min = i16::MAX;
                for (node_id, node) in self.nodes.iter() {
                    let start_next_term_id = match &node.data {
                        QueryNodeData::Term(term) => *term.term_ids.start() as i16,
                        QueryNodeData::End => i16::MAX,
                        QueryNodeData::Start => continue,
                        QueryNodeData::Deleted => continue,
                    };
                    if start_next_term_id <= end_prev_term_id {
                        continue;
                    }
                    match start_next_term_id.cmp(&min) {
                        Ordering::Less => {
                            min = start_next_term_id;
                            successors.clear();
                            successors.insert(node_id);
                        }
                        Ordering::Equal => {
                            successors.insert(node_id);
                        }
                        Ordering::Greater => continue,
                    }
                }
                successors
            };
            let node = self.nodes.get_mut(node_id);
            node.successors = successors.clone();
            for successor in successors.iter() {
                let successor = self.nodes.get_mut(successor);
                successor.predecessors.insert(node_id);
            }
        }
    }

    pub fn removal_order_for_terms_matching_strategy_frequency(
        &self,
        ctx: &mut SearchContext,
    ) -> Result<Vec<SmallBitmap<QueryNode>>> {
        // lookup frequency for each term
        let mut term_with_frequency: Vec<(u8, u64)> = {
            let mut term_docids: BTreeMap<u8, RoaringBitmap> = Default::default();
            for (_, node) in self.nodes.iter() {
                match &node.data {
                    QueryNodeData::Term(t) => {
                        let docids = compute_query_term_subset_docids(ctx, None, &t.term_subset)?;
                        for id in t.term_ids.clone() {
                            term_docids
                                .entry(id)
                                .and_modify(|curr| *curr |= &docids)
                                .or_insert_with(|| docids.clone());
                        }
                    }
                    QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => continue,
                }
            }
            term_docids
                .into_iter()
                .map(|(idx, docids)| match docids.len() {
                    0 => (idx, u64::max_value()),
                    frequency => (idx, frequency),
                })
                .collect()
        };
        term_with_frequency.sort_by_key(|(_, frequency)| Reverse(*frequency));
        let mut term_weight = BTreeMap::new();
        let mut weight: u16 = 1;
        let mut peekable = term_with_frequency.into_iter().peekable();
        while let Some((idx, frequency)) = peekable.next() {
            term_weight.insert(idx, weight);
            if peekable.peek().map_or(false, |(_, f)| frequency != *f) {
                weight += 1;
            }
        }
        let cost_of_term_idx = move |term_idx: u8| *term_weight.get(&term_idx).unwrap();
        Ok(self.removal_order_for_terms_matching_strategy(ctx, cost_of_term_idx))
    }

    pub fn removal_order_for_terms_matching_strategy_last(
        &self,
        ctx: &SearchContext,
    ) -> Vec<SmallBitmap<QueryNode>> {
        let (first_term_idx, last_term_idx) = {
            let mut first_term_idx = u8::MAX;
            let mut last_term_idx = 0u8;
            for (_, node) in self.nodes.iter() {
                match &node.data {
                    QueryNodeData::Term(t) => {
                        if *t.term_ids.end() > last_term_idx {
                            last_term_idx = *t.term_ids.end();
                        }
                        if *t.term_ids.start() < first_term_idx {
                            first_term_idx = *t.term_ids.start();
                        }
                    }
                    QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => continue,
                }
            }
            (first_term_idx, last_term_idx)
        };
        if first_term_idx >= last_term_idx {
            return vec![];
        }

        let cost_of_term_idx = |term_idx: u8| {
            let rank = 1 + last_term_idx - term_idx;
            rank as u16
        };
        self.removal_order_for_terms_matching_strategy(ctx, cost_of_term_idx)
    }

    pub fn removal_order_for_terms_matching_strategy(
        &self,
        ctx: &SearchContext,
        order: impl Fn(u8) -> u16,
    ) -> Vec<SmallBitmap<QueryNode>> {
        let mut nodes_to_remove = BTreeMap::<u16, SmallBitmap<QueryNode>>::new();
        let mut at_least_one_mandatory_term = false;
        for (node_id, node) in self.nodes.iter() {
            let QueryNodeData::Term(t) = &node.data else { continue };
            if t.term_subset.original_phrase(ctx).is_some() || t.term_subset.is_mandatory() {
                at_least_one_mandatory_term = true;
                continue;
            }
            let mut cost = 0;
            for id in t.term_ids.clone() {
                cost = std::cmp::max(cost, order(id));
            }
            nodes_to_remove
                .entry(cost)
                .or_insert_with(|| SmallBitmap::for_interned_values_in(&self.nodes))
                .insert(node_id);
        }
        let mut res: Vec<_> = nodes_to_remove.into_values().collect();
        if !at_least_one_mandatory_term {
            res.pop();
        }
        res
    }

    /// Number of words in the phrases in this query graph
    pub(crate) fn words_in_phrases_count(&self, ctx: &SearchContext) -> usize {
        let mut word_count = 0;
        for (_, node) in self.nodes.iter() {
            match &node.data {
                QueryNodeData::Term(term) => {
                    let Some(phrase) = term.term_subset.original_phrase(ctx) else { continue };
                    let phrase = ctx.phrase_interner.get(phrase);
                    word_count += phrase.words.iter().copied().filter(|a| a.is_some()).count()
                }
                _ => continue,
            }
        }
        word_count
    }
}

fn add_node(nodes_data: &mut Vec<QueryNodeData>, node_data: QueryNodeData) -> u16 {
    let new_node_idx = nodes_data.len() as u16;
    nodes_data.push(node_data);
    new_node_idx
}

impl QueryGraph {
    /*
    Build a query graph from a list of paths

    The paths are composed of source and dest terms.

    For example, consider the following paths:
    ```txt
    PATH 1 :  a -> b1 -> c1 -> d -> e1
    PATH 2 :  a -> b2 -> c2 -> d -> e2
    ```
    Then the resulting graph will be:
    ```txt
              ┌────┐  ┌────┐   ┌────┐   ┌────┐
           ┌──│ b1 │──│ c1 │───│ d  │───│ e1 │
    ┌────┐ │  └────┘  └────┘   └────┘   └────┘
    │ a  │─┤
    └────┘ │  ┌────┐  ┌────┐   ┌────┐   ┌────┐
           └──│ b2 │──│ c2 │───│ d  │───│ e2 │
              └────┘  └────┘   └────┘   └────┘
    ```
    */
    pub fn build_from_paths(
        paths: Vec<Vec<(Option<LocatedQueryTermSubset>, LocatedQueryTermSubset)>>,
    ) -> Self {
        let mut node_data = Interner::default();
        let root_node = node_data.push(QueryNodeData::Start);
        let end_node = node_data.push(QueryNodeData::End);

        let mut paths_with_single_terms = vec![];

        for path in paths {
            let mut processed_path = vec![];
            let mut prev_dest_term: Option<LocatedQueryTermSubset> = None;
            for (start_term, dest_term) in path {
                if let Some(prev_dest_term) = prev_dest_term.take() {
                    if let Some(mut start_term) = start_term {
                        if start_term.term_ids == prev_dest_term.term_ids {
                            start_term.term_subset.intersect(&prev_dest_term.term_subset);
                            processed_path.push(start_term);
                        } else {
                            processed_path.push(prev_dest_term);
                            processed_path.push(start_term);
                        }
                    } else {
                        processed_path.push(prev_dest_term);
                    }
                } else if let Some(start_term) = start_term {
                    processed_path.push(start_term);
                }
                prev_dest_term = Some(dest_term);
            }
            if let Some(prev_dest_term) = prev_dest_term {
                processed_path.push(prev_dest_term);
            }
            paths_with_single_terms.push(processed_path);
        }

        let mut paths_with_single_terms_and_suffix_hash = vec![];
        for path in paths_with_single_terms {
            let mut hasher = FxHasher::default();
            let mut path_with_hash = vec![];
            for term in path.into_iter().rev() {
                term.hash(&mut hasher);
                path_with_hash.push((term, hasher.finish()));
            }
            path_with_hash.reverse();
            paths_with_single_terms_and_suffix_hash.push(path_with_hash);
        }

        let mut node_data_id_for_term_and_suffix_hash =
            FxHashMap::<(LocatedQueryTermSubset, u64), Interned<QueryNodeData>>::default();

        let mut paths_with_ids = vec![];
        for path in paths_with_single_terms_and_suffix_hash {
            let mut path_with_ids = vec![];
            for (term, suffix_hash) in path {
                let node_data_id = node_data_id_for_term_and_suffix_hash
                    .entry((term.clone(), suffix_hash))
                    .or_insert_with(|| node_data.push(QueryNodeData::Term(term)));
                path_with_ids.push(Interned::from_raw(node_data_id.into_raw()));
            }
            paths_with_ids.push(path_with_ids);
        }

        let nodes_data = node_data.freeze();
        let nodes_data_len = nodes_data.len();
        let mut nodes = nodes_data.map_move(|n| QueryNode {
            data: n,
            predecessors: SmallBitmap::new(nodes_data_len),
            successors: SmallBitmap::new(nodes_data_len),
        });

        let root_node = Interned::<QueryNode>::from_raw(root_node.into_raw());
        let end_node = Interned::<QueryNode>::from_raw(end_node.into_raw());

        for path in paths_with_ids {
            let mut prev_node_id = root_node;
            for node_id in path {
                let prev_node = nodes.get_mut(prev_node_id);
                prev_node.successors.insert(node_id);
                let node = nodes.get_mut(node_id);
                node.predecessors.insert(prev_node_id);
                prev_node_id = node_id;
            }
            let prev_node = nodes.get_mut(prev_node_id);
            prev_node.successors.insert(end_node);
            let node = nodes.get_mut(end_node);
            node.predecessors.insert(prev_node_id);
        }

        QueryGraph { root_node, end_node, nodes }
    }
}
