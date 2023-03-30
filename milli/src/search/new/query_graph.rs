use super::interner::{FixedSizeInterner, Interned};
use super::query_term::{
    self, number_of_typos_allowed, LocatedQueryTerm, LocatedQueryTermSubset, NTypoTermSubset,
    QueryTermSubset,
};
use super::small_bitmap::SmallBitmap;
use super::SearchContext;
use crate::search::new::interner::DedupInterner;
use crate::Result;
use std::cmp::Ordering;
use std::collections::BTreeMap;

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

## Important
At the moment, a query graph has a hardcoded limit of [`QUERY_GRAPH_NODE_LENGTH_LIMIT`] nodes.

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
    /// Build the query graph from the parsed user search query.
    pub fn from_query(
        ctx: &mut SearchContext,
        // NOTE: the terms here must be consecutive
        terms: &[LocatedQueryTerm],
    ) -> Result<QueryGraph> {
        let nbr_typos = number_of_typos_allowed(ctx)?;

        let mut nodes_data: Vec<QueryNodeData> = vec![QueryNodeData::Start, QueryNodeData::End];
        let root_node = 0;
        let end_node = 1;

        // TODO: we could consider generalizing to 4,5,6,7,etc. ngrams
        let (mut prev2, mut prev1, mut prev0): (Vec<u16>, Vec<u16>, Vec<u16>) =
            (vec![], vec![], vec![root_node]);

        let original_terms_len = terms.len();
        for term_idx in 0..original_terms_len {
            let mut new_nodes = vec![];
            let new_node_idx = add_node(
                &mut nodes_data,
                QueryNodeData::Term(LocatedQueryTermSubset {
                    term_subset: QueryTermSubset {
                        original: Interned::from_raw(term_idx as u16),
                        zero_typo_subset: NTypoTermSubset::All,
                        one_typo_subset: NTypoTermSubset::All,
                        two_typo_subset: NTypoTermSubset::All,
                    },
                    positions: terms[term_idx].positions.clone(),
                    term_ids: term_idx as u8..=term_idx as u8,
                }),
            );
            new_nodes.push(new_node_idx);

            if !prev1.is_empty() {
                if let Some(ngram) =
                    query_term::make_ngram(ctx, &terms[term_idx - 1..=term_idx], &nbr_typos)?
                {
                    let ngram_idx = add_node(
                        &mut nodes_data,
                        QueryNodeData::Term(LocatedQueryTermSubset {
                            term_subset: QueryTermSubset {
                                original: ngram.value,
                                zero_typo_subset: NTypoTermSubset::All,
                                one_typo_subset: NTypoTermSubset::All,
                                two_typo_subset: NTypoTermSubset::All,
                            },
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
                    let ngram_idx = add_node(
                        &mut nodes_data,
                        QueryNodeData::Term(LocatedQueryTermSubset {
                            term_subset: QueryTermSubset {
                                original: ngram.value,
                                zero_typo_subset: NTypoTermSubset::All,
                                one_typo_subset: NTypoTermSubset::All,
                                two_typo_subset: NTypoTermSubset::All,
                            },
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
        graph.rebuild_edges();

        Ok(graph)
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
        self.rebuild_edges();
    }

    fn rebuild_edges(&mut self) {
        for (_, node) in self.nodes.iter_mut() {
            node.successors.clear();
            node.predecessors.clear();
        }
        for node_id in self.nodes.indexes() {
            let node = self.nodes.get(node_id);
            let end_position = match &node.data {
                QueryNodeData::Term(term) => *term.positions.end(),
                QueryNodeData::Start => -1,
                QueryNodeData::Deleted => continue,
                QueryNodeData::End => continue,
            };
            let successors = {
                let mut successors = SmallBitmap::for_interned_values_in(&self.nodes);
                let mut min = i8::MAX;
                for (node_id, node) in self.nodes.iter() {
                    let start_position = match &node.data {
                        QueryNodeData::Term(term) => *term.positions.start(),
                        QueryNodeData::End => i8::MAX,
                        QueryNodeData::Start => continue,
                        QueryNodeData::Deleted => continue,
                    };
                    if start_position <= end_position {
                        continue;
                    }
                    match start_position.cmp(&min) {
                        Ordering::Less => {
                            min = start_position;
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

    /// Remove all the nodes that correspond to a word starting at the given position and rebuild
    /// the edges of the graph appropriately.
    pub fn remove_words_starting_at_position(&mut self, position: i8) -> bool {
        let mut nodes_to_remove = vec![];
        for (node_idx, node) in self.nodes.iter() {
            let QueryNodeData::Term(LocatedQueryTermSubset { term_subset: _, positions, term_ids: _  }) = &node.data else { continue };
            if positions.start() == &position {
                nodes_to_remove.push(node_idx);
            }
        }

        self.remove_nodes(&nodes_to_remove);

        !nodes_to_remove.is_empty()
    }

    pub fn removal_order_for_terms_matching_strategy_last(&self) -> Vec<SmallBitmap<QueryNode>> {
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
            if term_idx == first_term_idx {
                None
            } else {
                let rank = 1 + last_term_idx - term_idx;
                Some(rank as u16)
            }
        };
        let mut nodes_to_remove = BTreeMap::<u16, SmallBitmap<QueryNode>>::new();
        for (node_id, node) in self.nodes.iter() {
            let QueryNodeData::Term(t) = &node.data else { continue };
            let mut cost = 0;
            for id in t.term_ids.clone() {
                if let Some(t_cost) = cost_of_term_idx(id) {
                    cost += t_cost;
                } else {
                    continue;
                }
            }
            nodes_to_remove
                .entry(cost)
                .or_insert_with(|| SmallBitmap::for_interned_values_in(&self.nodes))
                .insert(node_id);
        }
        nodes_to_remove.into_values().collect()
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
    If the source term is `None`, then the last dest term is used
    as the predecessor of the dest term. If the source is Some(_),
    then an edge is built between the last dest term and the source,
    and between the source and new dest term.

    Note that the resulting graph will not correspond to a perfect
    representation of the set of paths.
    For example, consider the following paths:
    ```txt
    PATH 1 :  a -> b1 -> c1 -> d -> e1
    PATH 2 :  a -> b2 -> c2 -> d -> e2
    ```
    Then the resulting graph will be:
    ```txt
              ┌────┐  ┌────┐             ┌────┐
           ┌──│ b1 │──│ c1 │─┐        ┌──│ e1 │
    ┌────┐ │  └────┘  └────┘ │ ┌────┐ │  └────┘
    │ a  │─┤                 ├─│ d  │─┤
    └────┘ │  ┌────┐  ┌────┐ │ └────┘ │  ┌────┐
           └──│ b2 │──│ c2 │─┘        └──│ e2 │
              └────┘  └────┘             └────┘
    ```
    which is different from the fully correct representation:
    ```txt
              ┌────┐  ┌────┐   ┌────┐   ┌────┐
           ┌──│ b1 │──│ c1 │───│ d  │───│ e1 │
    ┌────┐ │  └────┘  └────┘   └────┘   └────┘
    │ a  │─┤
    └────┘ │  ┌────┐  ┌────┐   ┌────┐   ┌────┐
           └──│ b2 │──│ c2 │───│ d  │───│ e2 │
              └────┘  └────┘   └────┘   └────┘
    ```
    But we accept the first representation as it reduces the size
    of the graph and shouldn't cause much problems.
    */
    pub fn build_from_paths(
        paths: Vec<Vec<(Option<LocatedQueryTermSubset>, LocatedQueryTermSubset)>>,
    ) -> Self {
        let mut node_data = DedupInterner::default();
        let root_node = node_data.insert(QueryNodeData::Start);
        let end_node = node_data.insert(QueryNodeData::End);

        let mut paths_with_ids = vec![];
        for path in paths {
            let mut path_with_ids = vec![];
            for node in path {
                let (start_term, end_term) = node;
                let src_node_id = start_term.map(|x| node_data.insert(QueryNodeData::Term(x)));
                let dest_node_id = node_data.insert(QueryNodeData::Term(end_term));
                path_with_ids.push((src_node_id, dest_node_id));
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

        let root_node = Interned::from_raw(root_node.into_raw());
        let end_node = Interned::from_raw(end_node.into_raw());

        for path in paths_with_ids {
            let mut prev_node = root_node;
            for node in path {
                let (start_term, dest_term) = node;
                let end_term = Interned::from_raw(dest_term.into_raw());
                let src = if let Some(start_term) = start_term {
                    let start_term = Interned::from_raw(start_term.into_raw());
                    nodes.get_mut(prev_node).successors.insert(start_term);
                    nodes.get_mut(start_term).predecessors.insert(prev_node);
                    start_term
                } else {
                    prev_node
                };
                nodes.get_mut(src).successors.insert(end_term);
                nodes.get_mut(end_term).predecessors.insert(src);
                prev_node = end_term;
            }
            nodes.get_mut(prev_node).successors.insert(end_node);
            nodes.get_mut(end_node).predecessors.insert(prev_node);
        }

        QueryGraph { root_node, end_node, nodes }
    }
}
