use super::query_term::{self, LocatedQueryTerm, QueryTerm, WordDerivations};
use super::small_bitmap::SmallBitmap;
use super::SearchContext;
use crate::Result;

pub const QUERY_GRAPH_NODE_LENGTH_LIMIT: u16 = 64;

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
pub enum QueryNode {
    Term(LocatedQueryTerm),
    Deleted,
    Start,
    End,
}

/// The edges associated with a node in the query graph.
#[derive(Clone)]
pub struct Edges {
    /// Set of nodes which have an edge going to the current node
    pub predecessors: SmallBitmap,
    /// Set of nodes which are reached by an edge from the current node
    pub successors: SmallBitmap,
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

All these derivations of a word will be stored in [`WordDerivations`].

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
    pub root_node: u16,
    /// The index of the end node within `self.nodes`
    pub end_node: u16,
    /// The list of all query nodes
    pub nodes: Vec<QueryNode>,
    /// The list of all node edges
    pub edges: Vec<Edges>,
}

impl Default for QueryGraph {
    /// Create a new QueryGraph with two disconnected nodes: the root and end nodes.
    fn default() -> Self {
        let nodes = vec![QueryNode::Start, QueryNode::End];
        let edges = vec![
            Edges {
                predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
                successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
            },
            Edges {
                predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
                successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
            },
        ];

        Self { root_node: 0, end_node: 1, nodes, edges }
    }
}

impl QueryGraph {
    /// Connect all the given predecessor nodes to the given successor node
    fn connect_to_node(&mut self, from_nodes: &[u16], to_node: u16) {
        for &from_node in from_nodes {
            self.edges[from_node as usize].successors.insert(to_node);
            self.edges[to_node as usize].predecessors.insert(from_node);
        }
    }
    /// Add the given node to the graph and connect it to all the given predecessor nodes
    fn add_node(&mut self, from_nodes: &[u16], node: QueryNode) -> u16 {
        let new_node_idx = self.nodes.len() as u16;
        assert!(new_node_idx <= QUERY_GRAPH_NODE_LENGTH_LIMIT);
        self.nodes.push(node);
        self.edges.push(Edges {
            predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
            successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
        });
        self.connect_to_node(from_nodes, new_node_idx);

        new_node_idx
    }
}

impl QueryGraph {
    /// Build the query graph from the parsed user search query.
    pub fn from_query(ctx: &mut SearchContext, terms: Vec<LocatedQueryTerm>) -> Result<QueryGraph> {
        let mut empty_nodes = vec![];

        let word_set = ctx.index.words_fst(ctx.txn)?;
        let mut graph = QueryGraph::default();

        let (mut prev2, mut prev1, mut prev0): (Vec<u16>, Vec<u16>, Vec<u16>) =
            (vec![], vec![], vec![graph.root_node]);

        for length in 1..=terms.len() {
            let query = &terms[..length];

            let term0 = query.last().unwrap();

            let mut new_nodes = vec![];
            let new_node_idx = graph.add_node(&prev0, QueryNode::Term(term0.clone()));
            new_nodes.push(new_node_idx);
            if term0.is_empty(&ctx.derivations_interner) {
                empty_nodes.push(new_node_idx);
            }

            if !prev1.is_empty() {
                if let Some((ngram2_str, ngram2_pos)) =
                    query_term::ngram2(ctx, &query[length - 2], &query[length - 1])
                {
                    if word_set.contains(ctx.word_interner.get(ngram2_str)) {
                        let ngram2 = LocatedQueryTerm {
                            value: QueryTerm::Word {
                                derivations: ctx.derivations_interner.insert(WordDerivations {
                                    original: ngram2_str,
                                    // TODO: could add a typo if it's an ngram?
                                    zero_typo: Box::new([ngram2_str]),
                                    one_typo: Box::new([]),
                                    two_typos: Box::new([]),
                                    use_prefix_db: false,
                                    synonyms: Box::new([]), // TODO: ngram synonyms
                                    split_words: None,      // TODO: maybe ngram split words?
                                }),
                            },
                            positions: ngram2_pos,
                        };
                        let ngram2_idx = graph.add_node(&prev1, QueryNode::Term(ngram2));
                        new_nodes.push(ngram2_idx);
                    }
                }
            }
            if !prev2.is_empty() {
                if let Some((ngram3_str, ngram3_pos)) = query_term::ngram3(
                    ctx,
                    &query[length - 3],
                    &query[length - 2],
                    &query[length - 1],
                ) {
                    if word_set.contains(ctx.word_interner.get(ngram3_str)) {
                        let ngram3 = LocatedQueryTerm {
                            value: QueryTerm::Word {
                                derivations: ctx.derivations_interner.insert(WordDerivations {
                                    original: ngram3_str,
                                    // TODO: could add a typo if it's an ngram?
                                    zero_typo: Box::new([ngram3_str]),
                                    one_typo: Box::new([]),
                                    two_typos: Box::new([]),
                                    use_prefix_db: false,
                                    synonyms: Box::new([]), // TODO: ngram synonyms
                                    split_words: None,      // TODO: maybe ngram split words?
                                                            // would be nice for typos like su nflower
                                }),
                            },
                            positions: ngram3_pos,
                        };
                        let ngram3_idx = graph.add_node(&prev2, QueryNode::Term(ngram3));
                        new_nodes.push(ngram3_idx);
                    }
                }
            }
            (prev0, prev1, prev2) = (new_nodes, prev0, prev1);
        }
        graph.connect_to_node(&prev0, graph.end_node);

        graph.remove_nodes_keep_edges(&empty_nodes);

        Ok(graph)
    }

    /// Remove the given nodes and all their edges from the query graph.
    pub fn remove_nodes(&mut self, nodes: &[u16]) {
        for &node in nodes {
            self.nodes[node as usize] = QueryNode::Deleted;
            let edges = self.edges[node as usize].clone();
            for pred in edges.predecessors.iter() {
                self.edges[pred as usize].successors.remove(node);
            }
            for succ in edges.successors.iter() {
                self.edges[succ as usize].predecessors.remove(node);
            }
            self.edges[node as usize] = Edges {
                predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
                successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
            };
        }
    }
    /// Remove the given nodes, connecting all their predecessors to all their successors.
    pub fn remove_nodes_keep_edges(&mut self, nodes: &[u16]) {
        for &node in nodes {
            self.nodes[node as usize] = QueryNode::Deleted;
            let edges = self.edges[node as usize].clone();
            for pred in edges.predecessors.iter() {
                self.edges[pred as usize].successors.remove(node);
                self.edges[pred as usize].successors.union(&edges.successors);
            }
            for succ in edges.successors.iter() {
                self.edges[succ as usize].predecessors.remove(node);
                self.edges[succ as usize].predecessors.union(&edges.predecessors);
            }
            self.edges[node as usize] = Edges {
                predecessors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
                successors: SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT),
            };
        }
    }

    /// Remove all the nodes that correspond to a word starting at the given position, and connect
    /// the predecessors of these nodes to their successors.
    /// Return `true` if any node was removed.
    pub fn remove_words_starting_at_position(&mut self, position: i8) -> bool {
        let mut nodes_to_remove_keeping_edges = vec![];
        for (node_idx, node) in self.nodes.iter().enumerate() {
            let node_idx = node_idx as u16;
            let QueryNode::Term(LocatedQueryTerm { value: _, positions }) = node else { continue };
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
    fn simplify(&mut self) {
        loop {
            let mut nodes_to_remove = vec![];
            for (node_idx, node) in self.nodes.iter().enumerate() {
                if (!matches!(node, QueryNode::End | QueryNode::Deleted)
                    && self.edges[node_idx].successors.is_empty())
                    || (!matches!(node, QueryNode::Start | QueryNode::Deleted)
                        && self.edges[node_idx].predecessors.is_empty())
                {
                    nodes_to_remove.push(node_idx as u16);
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
