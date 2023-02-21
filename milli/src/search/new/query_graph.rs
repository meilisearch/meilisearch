use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::db_cache::DatabaseCache;
use super::query_term::{LocatedQueryTerm, QueryTerm, WordDerivations};
use crate::{Index, Result};

#[derive(Clone)]
pub enum QueryNode {
    Term(LocatedQueryTerm),
    Deleted,
    Start,
    End,
}

#[derive(Debug, Clone)]
pub struct Edges {
    // TODO: use a tiny bitset instead, something like a simple Vec<u8> where most queries will see a vector of one element
    pub predecessors: RoaringBitmap,
    pub successors: RoaringBitmap,
}

#[derive(Debug, Clone)]
pub struct QueryGraph {
    pub root_node: u32,
    pub end_node: u32,
    pub nodes: Vec<QueryNode>,
    pub edges: Vec<Edges>,
}

fn _assert_sizes() {
    let _: [u8; 112] = [0; std::mem::size_of::<QueryNode>()];
    let _: [u8; 48] = [0; std::mem::size_of::<Edges>()];
}

impl Default for QueryGraph {
    /// Create a new QueryGraph with two disconnected nodes: the root and end nodes.
    fn default() -> Self {
        let nodes = vec![QueryNode::Start, QueryNode::End];
        let edges = vec![
            Edges { predecessors: RoaringBitmap::new(), successors: RoaringBitmap::new() },
            Edges { predecessors: RoaringBitmap::new(), successors: RoaringBitmap::new() },
        ];

        Self { root_node: 0, end_node: 1, nodes, edges }
    }
}

impl QueryGraph {
    fn connect_to_node(&mut self, from_nodes: &[u32], to_node: u32) {
        for &from_node in from_nodes {
            self.edges[from_node as usize].successors.insert(to_node);
            self.edges[to_node as usize].predecessors.insert(from_node);
        }
    }
    fn add_node(&mut self, from_nodes: &[u32], node: QueryNode) -> u32 {
        let new_node_idx = self.nodes.len() as u32;
        self.nodes.push(node);
        self.edges.push(Edges {
            predecessors: from_nodes.iter().collect(),
            successors: RoaringBitmap::new(),
        });
        for from_node in from_nodes {
            self.edges[*from_node as usize].successors.insert(new_node_idx);
        }
        new_node_idx
    }
}

impl QueryGraph {
    // TODO: return the list of all matching words here as well
    pub fn from_query<'transaction>(
        index: &Index,
        txn: &RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
        query: Vec<LocatedQueryTerm>,
    ) -> Result<QueryGraph> {
        // TODO: maybe empty nodes should not be removed here, to compute
        // the score of the `words` ranking rule correctly
        // it is very easy to traverse the graph and remove afterwards anyway
        // Still, I'm keeping this here as a demo
        let mut empty_nodes = vec![];

        let word_set = index.words_fst(txn)?;
        let mut graph = QueryGraph::default();

        let (mut prev2, mut prev1, mut prev0): (Vec<u32>, Vec<u32>, Vec<u32>) =
            (vec![], vec![], vec![graph.root_node]);

        // TODO: split words / synonyms
        for length in 1..=query.len() {
            let query = &query[..length];

            let term0 = query.last().unwrap();

            let mut new_nodes = vec![];
            let new_node_idx = graph.add_node(&prev0, QueryNode::Term(term0.clone()));
            new_nodes.push(new_node_idx);
            if term0.is_empty() {
                empty_nodes.push(new_node_idx);
            }

            if !prev1.is_empty() {
                if let Some((ngram2_str, ngram2_pos)) =
                    LocatedQueryTerm::ngram2(&query[length - 2], &query[length - 1])
                {
                    if word_set.contains(ngram2_str.as_bytes()) {
                        let ngram2 = LocatedQueryTerm {
                            value: QueryTerm::Word {
                                derivations: WordDerivations {
                                    original: ngram2_str.clone(),
                                    // TODO: could add a typo if it's an ngram?
                                    zero_typo: vec![ngram2_str],
                                    one_typo: vec![],
                                    two_typos: vec![],
                                    use_prefix_db: false,
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
                if let Some((ngram3_str, ngram3_pos)) = LocatedQueryTerm::ngram3(
                    &query[length - 3],
                    &query[length - 2],
                    &query[length - 1],
                ) {
                    if word_set.contains(ngram3_str.as_bytes()) {
                        let ngram3 = LocatedQueryTerm {
                            value: QueryTerm::Word {
                                derivations: WordDerivations {
                                    original: ngram3_str.clone(),
                                    // TODO: could add a typo if it's an ngram?
                                    zero_typo: vec![ngram3_str],
                                    one_typo: vec![],
                                    two_typos: vec![],
                                    use_prefix_db: false,
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
    pub fn remove_nodes(&mut self, nodes: &[u32]) {
        for &node in nodes {
            self.nodes[node as usize] = QueryNode::Deleted;
            let edges = self.edges[node as usize].clone();
            for pred in edges.predecessors.iter() {
                self.edges[pred as usize].successors.remove(node);
            }
            for succ in edges.successors {
                self.edges[succ as usize].predecessors.remove(node);
            }
            self.edges[node as usize] =
                Edges { predecessors: RoaringBitmap::new(), successors: RoaringBitmap::new() };
        }
    }
    pub fn remove_nodes_keep_edges(&mut self, nodes: &[u32]) {
        for &node in nodes {
            self.nodes[node as usize] = QueryNode::Deleted;
            let edges = self.edges[node as usize].clone();
            for pred in edges.predecessors.iter() {
                self.edges[pred as usize].successors.remove(node);
                self.edges[pred as usize].successors |= &edges.successors;
            }
            for succ in edges.successors {
                self.edges[succ as usize].predecessors.remove(node);
                self.edges[succ as usize].predecessors |= &edges.predecessors;
            }
            self.edges[node as usize] =
                Edges { predecessors: RoaringBitmap::new(), successors: RoaringBitmap::new() };
        }
    }
    pub fn remove_words_at_position(&mut self, position: i8) {
        let mut nodes_to_remove_keeping_edges = vec![];
        let mut nodes_to_remove = vec![];
        for (node_idx, node) in self.nodes.iter().enumerate() {
            let node_idx = node_idx as u32;
            let QueryNode::Term(LocatedQueryTerm { value: _, positions }) = node else { continue };
            if positions.contains(&position) {
                nodes_to_remove_keeping_edges.push(node_idx)
            } else if positions.contains(&position) {
                nodes_to_remove.push(node_idx)
            }
        }

        self.remove_nodes(&nodes_to_remove);
        self.remove_nodes_keep_edges(&nodes_to_remove_keeping_edges);

        self.simplify();
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
                    nodes_to_remove.push(node_idx as u32);
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
impl Debug for QueryNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryNode::Term(term @ LocatedQueryTerm { value, positions: _ }) => match value {
                QueryTerm::Word {
                    derivations:
                        WordDerivations { original, zero_typo, one_typo, two_typos, use_prefix_db },
                } => {
                    if term.is_empty() {
                        write!(f, "\"{original} (∅)\"")
                    } else {
                        let derivations = std::iter::once(original.clone())
                            .chain(zero_typo.iter().map(|s| format!("T0 .. {s}")))
                            .chain(one_typo.iter().map(|s| format!("T1 .. {s}")))
                            .chain(two_typos.iter().map(|s| format!("T2 .. {s}")))
                            .collect::<Vec<String>>()
                            .join(" | ");

                        write!(f, "\"{derivations}")?;
                        if *use_prefix_db {
                            write!(f, " | +prefix_db")?;
                        }
                        write!(f, " | pos:{}..={}", term.positions.start(), term.positions.end())?;
                        write!(f, "\"")?;
                        /*
                        "beautiful" [label = "<f0> beautiful | beauiful | beautifol"]
                        */
                        Ok(())
                    }
                }
                QueryTerm::Phrase(ws) => {
                    let joined =
                        ws.iter().filter_map(|x| x.clone()).collect::<Vec<String>>().join(" ");
                    let in_quotes = format!("\"{joined}\"");
                    let escaped = in_quotes.escape_default().collect::<String>();
                    write!(f, "\"{escaped}\"")
                }
            },
            QueryNode::Start => write!(f, "\"START\""),
            QueryNode::End => write!(f, "\"END\""),
            QueryNode::Deleted => write!(f, "\"_deleted_\""),
        }
    }
}

impl QueryGraph {
    pub fn graphviz(&self) -> String {
        let mut desc = String::new();
        desc.push_str(
            r#"
digraph G {
rankdir = LR;
node [shape = "record"]
"#,
        );

        for node in 0..self.nodes.len() {
            if matches!(self.nodes[node], QueryNode::Deleted) {
                continue;
            }
            desc.push_str(&format!("{node} [label = {:?}]", &self.nodes[node],));
            if node == self.root_node as usize {
                desc.push_str("[color = blue]");
            } else if node == self.end_node as usize {
                desc.push_str("[color = red]");
            }
            desc.push_str(";\n");

            for edge in self.edges[node].successors.iter() {
                desc.push_str(&format!("{node} -> {edge};\n"));
            }
        }

        desc.push('}');
        desc
    }
}
