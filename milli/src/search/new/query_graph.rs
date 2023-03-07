use super::query_term::{self, LocatedQueryTerm, QueryTerm, WordDerivations};
use super::small_bitmap::SmallBitmap;
use super::SearchContext;
use crate::Result;

#[derive(Clone)]
pub enum QueryNode {
    Term(LocatedQueryTerm),
    Deleted,
    Start,
    End,
}

#[derive(Clone)]
pub struct Edges {
    // TODO: use a tiny bitset instead, something like a simple Vec<u8> where most queries will see a vector of one element
    pub predecessors: SmallBitmap,
    pub successors: SmallBitmap,
}

#[derive(Clone)]
pub struct QueryGraph {
    pub root_node: u16,
    pub end_node: u16,
    pub nodes: Vec<QueryNode>,
    pub edges: Vec<Edges>,
}

fn _assert_sizes() {
    // TODO: QueryNodes are too big now, 88B is a bit too big
    let _: [u8; 88] = [0; std::mem::size_of::<QueryNode>()];
    let _: [u8; 32] = [0; std::mem::size_of::<Edges>()];
}

impl Default for QueryGraph {
    /// Create a new QueryGraph with two disconnected nodes: the root and end nodes.
    fn default() -> Self {
        let nodes = vec![QueryNode::Start, QueryNode::End];
        let edges = vec![
            Edges { predecessors: SmallBitmap::new(64), successors: SmallBitmap::new(64) },
            Edges { predecessors: SmallBitmap::new(64), successors: SmallBitmap::new(64) },
        ];

        Self { root_node: 0, end_node: 1, nodes, edges }
    }
}

impl QueryGraph {
    fn connect_to_node(&mut self, from_nodes: &[u16], to_node: u16) {
        for &from_node in from_nodes {
            self.edges[from_node as usize].successors.insert(to_node);
            self.edges[to_node as usize].predecessors.insert(from_node);
        }
    }
    fn add_node(&mut self, from_nodes: &[u16], node: QueryNode) -> u16 {
        let new_node_idx = self.nodes.len() as u16;
        self.nodes.push(node);
        self.edges.push(Edges {
            predecessors: SmallBitmap::from_array(from_nodes, 64),
            successors: SmallBitmap::new(64),
        });
        for from_node in from_nodes {
            self.edges[*from_node as usize].successors.insert(new_node_idx);
        }
        new_node_idx
    }
}

impl QueryGraph {
    // TODO: return the list of all matching words here as well
    pub fn from_query(ctx: &mut SearchContext, terms: Vec<LocatedQueryTerm>) -> Result<QueryGraph> {
        // TODO: maybe empty nodes should not be removed here, to compute
        // the score of the `words` ranking rule correctly
        // it is very easy to traverse the graph and remove afterwards anyway
        // Still, I'm keeping this here as a demo
        let mut empty_nodes = vec![];

        let word_set = ctx.index.words_fst(ctx.txn)?;
        let mut graph = QueryGraph::default();

        let (mut prev2, mut prev1, mut prev0): (Vec<u16>, Vec<u16>, Vec<u16>) =
            (vec![], vec![], vec![graph.root_node]);

        // TODO: split words / synonyms
        for length in 1..=terms.len() {
            let query = &terms[..length];

            let term0 = query.last().unwrap();

            let mut new_nodes = vec![];
            let new_node_idx = graph.add_node(&prev0, QueryNode::Term(term0.clone()));
            new_nodes.push(new_node_idx);
            if term0.is_empty() {
                empty_nodes.push(new_node_idx);
            }

            if !prev1.is_empty() {
                if let Some((ngram2_str, ngram2_pos)) =
                    query_term::ngram2(ctx, &query[length - 2], &query[length - 1])
                {
                    if word_set.contains(ctx.word_interner.get(ngram2_str)) {
                        let ngram2 = LocatedQueryTerm {
                            value: QueryTerm::Word {
                                derivations: WordDerivations {
                                    original: ngram2_str,
                                    // TODO: could add a typo if it's an ngram?
                                    zero_typo: Box::new([ngram2_str]),
                                    one_typo: Box::new([]),
                                    two_typos: Box::new([]),
                                    use_prefix_db: false,
                                    synonyms: Box::new([]), // TODO: ngram synonyms
                                    split_words: None,      // TODO: maybe ngram split words?
                                },
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
                                derivations: WordDerivations {
                                    original: ngram3_str,
                                    // TODO: could add a typo if it's an ngram?
                                    zero_typo: Box::new([ngram3_str]),
                                    one_typo: Box::new([]),
                                    two_typos: Box::new([]),
                                    use_prefix_db: false,
                                    synonyms: Box::new([]), // TODO: ngram synonyms
                                    split_words: None,      // TODO: maybe ngram split words?
                                                            // would be nice for typos like su nflower
                                },
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
            self.edges[node as usize] =
                Edges { predecessors: SmallBitmap::new(64), successors: SmallBitmap::new(64) };
        }
    }
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
            self.edges[node as usize] =
                Edges { predecessors: SmallBitmap::new(64), successors: SmallBitmap::new(64) };
        }
    }
    pub fn remove_words_at_position(&mut self, position: i8) -> bool {
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
