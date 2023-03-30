use std::collections::HashSet;

use super::interner::{FixedSizeInterner, Interned};
use super::query_term::{self, number_of_typos_allowed, LocatedQueryTerm};
use super::small_bitmap::SmallBitmap;
use super::SearchContext;
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
#[derive(Clone)]
pub enum QueryNodeData {
    Term(LocatedQueryTerm),
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

// impl Default for QueryGraph {
//     /// Create a new QueryGraph with two disconnected nodes: the root and end nodes.
//     fn default() -> Self {
//         let nodes = vec![
//             QueryNode {
//                 data: QueryNodeData::Start,
//                 predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
//                 successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
//             },
//             QueryNode {
//                 data: QueryNodeData::End,
//                 predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
//                 successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
//             },
//         ];

//         Self { root_node: 0, end_node: 1, nodes }
//     }
// }

impl QueryGraph {
    /// Connect all the given predecessor nodes to the given successor node
    fn connect_to_node(
        &mut self,
        from_nodes: &[Interned<QueryNode>],
        to_node: Interned<QueryNode>,
    ) {
        for &from_node in from_nodes {
            self.nodes.get_mut(from_node).successors.insert(to_node);
            self.nodes.get_mut(to_node).predecessors.insert(from_node);
        }
    }
}

impl QueryGraph {
    /// Build the query graph from the parsed user search query.
    ///
    /// The ngrams are made at this point.
    pub fn from_query(ctx: &mut SearchContext, terms: Vec<LocatedQueryTerm>) -> Result<QueryGraph> {
        let nbr_typos = number_of_typos_allowed(ctx)?;

        let mut empty_nodes = vec![];

        let mut predecessors: Vec<HashSet<u16>> = vec![HashSet::new(), HashSet::new()];
        let mut successors: Vec<HashSet<u16>> = vec![HashSet::new(), HashSet::new()];
        let mut nodes_data: Vec<QueryNodeData> = vec![QueryNodeData::Start, QueryNodeData::End];
        let root_node = 0;
        let end_node = 1;

        // TODO: we could consider generalizing to 4,5,6,7,etc. ngrams
        let (mut prev2, mut prev1, mut prev0): (Vec<u16>, Vec<u16>, Vec<u16>) =
            (vec![], vec![], vec![root_node]);

        for term_idx in 0..terms.len() {
            let term0 = &terms[term_idx];

            let mut new_nodes = vec![];
            let new_node_idx = add_node(
                &mut nodes_data,
                QueryNodeData::Term(term0.clone()),
                &prev0,
                &mut successors,
                &mut predecessors,
            );
            new_nodes.push(new_node_idx);
            if term0.is_empty(&ctx.term_interner) {
                empty_nodes.push(new_node_idx);
            }

            if !prev1.is_empty() {
                if let Some(ngram) =
                    query_term::make_ngram(ctx, &terms[term_idx - 1..=term_idx], &nbr_typos)?
                {
                    let ngram_idx = add_node(
                        &mut nodes_data,
                        QueryNodeData::Term(ngram),
                        &prev1,
                        &mut successors,
                        &mut predecessors,
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
                        QueryNodeData::Term(ngram),
                        &prev2,
                        &mut successors,
                        &mut predecessors,
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
        for (node_idx, ((node_data, predecessors), successors)) in nodes_data
            .into_iter()
            .zip(predecessors.into_iter())
            .zip(successors.into_iter())
            .enumerate()
        {
            let node = nodes.get_mut(Interned::from_raw(node_idx as u16));
            node.data = node_data;
            for x in predecessors {
                node.predecessors.insert(Interned::from_raw(x));
            }
            for x in successors {
                node.successors.insert(Interned::from_raw(x));
            }
        }
        let mut graph = QueryGraph { root_node, end_node, nodes };

        graph.connect_to_node(
            prev0.into_iter().map(Interned::from_raw).collect::<Vec<_>>().as_slice(),
            end_node,
        );
        let empty_nodes = empty_nodes.into_iter().map(Interned::from_raw).collect::<Vec<_>>();
        graph.remove_nodes_keep_edges(&empty_nodes);

        Ok(graph)
    }

    /// Remove the given nodes and all their edges from the query graph.
    /// TODO: need to check where this is used, and if this is correct.
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

    /// Remove all the nodes that correspond to a word starting at the given position, and connect
    /// the predecessors of these nodes to their successors.
    /// Return `true` if any node was removed.
    pub fn remove_words_starting_at_position(&mut self, position: u16) -> bool {
        let mut nodes_to_remove_keeping_edges = vec![];
        for (node_idx, node) in self.nodes.iter() {
            let QueryNodeData::Term(LocatedQueryTerm { value: _, positions }) = &node.data else { continue };
            if positions.start() == &position {
                nodes_to_remove_keeping_edges.push(node_idx);
            }
        }

        self.remove_nodes_keep_edges(&nodes_to_remove_keeping_edges);

        self.simplify();
        !nodes_to_remove_keeping_edges.is_empty()
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
}

fn add_node(
    nodes_data: &mut Vec<QueryNodeData>,
    node_data: QueryNodeData,
    from_nodes: &Vec<u16>,
    successors: &mut Vec<HashSet<u16>>,
    predecessors: &mut Vec<HashSet<u16>>,
) -> u16 {
    successors.push(HashSet::new());
    predecessors.push(HashSet::new());
    let new_node_idx = nodes_data.len() as u16;
    nodes_data.push(node_data);
    for &from_node in from_nodes {
        successors[from_node as usize].insert(new_node_idx);
        predecessors[new_node_idx as usize].insert(from_node);
    }
    new_node_idx
}
