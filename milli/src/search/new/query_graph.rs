use std::fmt::Debug;
use std::{collections::HashSet, fmt};

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::{
    db_cache::DatabaseCache,
    query_term::{LocatedQueryTerm, QueryTerm, WordDerivations},
};
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
    // TODO: use a tiny bitset instead
    // something like a simple Vec<u8> where most queries will see a vector of one element
    pub predecessors: RoaringBitmap,
    pub successors: RoaringBitmap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeIndex(pub u32);
impl fmt::Display for NodeIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone)]
pub struct QueryGraph {
    pub root_node: NodeIndex,
    pub end_node: NodeIndex,
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

        Self { root_node: NodeIndex(0), end_node: NodeIndex(1), nodes, edges }
    }
}

impl QueryGraph {
    fn connect_to_node(&mut self, from_nodes: &[NodeIndex], to_node: NodeIndex) {
        for &from_node in from_nodes {
            self.edges[from_node.0 as usize].successors.insert(to_node.0);
            self.edges[to_node.0 as usize].predecessors.insert(from_node.0);
        }
    }
    fn add_node(&mut self, from_nodes: &[NodeIndex], node: QueryNode) -> NodeIndex {
        let new_node_idx = self.nodes.len() as u32;
        self.nodes.push(node);
        self.edges.push(Edges {
            predecessors: from_nodes.iter().map(|x| x.0).collect(),
            successors: RoaringBitmap::new(),
        });
        for from_node in from_nodes {
            self.edges[from_node.0 as usize].successors.insert(new_node_idx);
        }
        NodeIndex(new_node_idx)
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

        let (mut prev2, mut prev1, mut prev0): (Vec<NodeIndex>, Vec<NodeIndex>, Vec<NodeIndex>) =
            (vec![], vec![], vec![graph.root_node]);

        // TODO: add all the word derivations found in the fst
        // and add split words / support phrases

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
    pub fn remove_nodes(&mut self, nodes: &[NodeIndex]) {
        for &node in nodes {
            self.nodes[node.0 as usize] = QueryNode::Deleted;
            let edges = self.edges[node.0 as usize].clone();
            for pred in edges.predecessors.iter() {
                self.edges[pred as usize].successors.remove(node.0);
            }
            for succ in edges.successors {
                self.edges[succ as usize].predecessors.remove(node.0);
            }
            self.edges[node.0 as usize] =
                Edges { predecessors: RoaringBitmap::new(), successors: RoaringBitmap::new() };
        }
    }
    pub fn remove_nodes_keep_edges(&mut self, nodes: &[NodeIndex]) {
        for &node in nodes {
            self.nodes[node.0 as usize] = QueryNode::Deleted;
            let edges = self.edges[node.0 as usize].clone();
            for pred in edges.predecessors.iter() {
                self.edges[pred as usize].successors.remove(node.0);
                self.edges[pred as usize].successors |= &edges.successors;
            }
            for succ in edges.successors {
                self.edges[succ as usize].predecessors.remove(node.0);
                self.edges[succ as usize].predecessors |= &edges.predecessors;
            }
            self.edges[node.0 as usize] =
                Edges { predecessors: RoaringBitmap::new(), successors: RoaringBitmap::new() };
        }
    }
    pub fn remove_words_at_position(&mut self, position: i8) {
        let mut nodes_to_remove_keeping_edges = vec![];
        let mut nodes_to_remove = vec![];
        for (node_idx, node) in self.nodes.iter().enumerate() {
            let node_idx = NodeIndex(node_idx as u32);
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
                    nodes_to_remove.push(NodeIndex(node_idx as u32));
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

/*
TODO:

1. Find the minimum number of words to check to resolve the 10 query trees at once.
    (e.g. just 0 | 01 | 012 )
2. Simplify the query tree after removal of a node ✅
3. Create the proximity graph ✅
4. Assign different proximities for the ngrams ✅
5. Walk the proximity graph, finding all the potential paths of weight N from START to END ✅
(without checking the bitmaps)

*/
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
            if node == self.root_node.0 as usize {
                desc.push_str("[color = blue]");
            } else if node == self.end_node.0 as usize {
                desc.push_str("[color = red]");
            }
            desc.push_str(";\n");

            for edge in self.edges[node].successors.iter() {
                desc.push_str(&format!("{node} -> {edge};\n"));
            }
            // for edge in self.edges[node].incoming.iter() {
            //     desc.push_str(&format!("{node} -> {edge} [color = grey];\n"));
            // }
        }

        desc.push('}');
        desc
    }
}

#[cfg(test)]
mod tests {
    use charabia::Tokenize;

    use super::{LocatedQueryTerm, QueryGraph, QueryNode};
    use crate::index::tests::TempIndex;
    use crate::new::db_cache::DatabaseCache;
    use crate::search::new::query_term::word_derivations;

    #[test]
    fn build_graph() {
        let mut index = TempIndex::new();
        index.index_documents_config.autogenerate_docids = true;
        index
            .update_settings(|s| {
                s.set_searchable_fields(vec!["text".to_owned()]);
            })
            .unwrap();
        index
            .add_documents(documents!({
                "text": "0 1 2 3 4 5 6 7 01 23 234 56 79 709 7356",
            }))
            .unwrap();

        // let fst = fst::Set::from_iter(["01", "23", "234", "56"]).unwrap();
        let txn = index.read_txn().unwrap();
        let mut db_cache = DatabaseCache::default();

        let fst = index.words_fst(&txn).unwrap();
        let query = LocatedQueryTerm::from_query(
            "0 no 1 2 3 4 5 6 7".tokenize(),
            None,
            |word, is_prefix| {
                word_derivations(
                    &index,
                    &txn,
                    word,
                    if word.len() < 3 {
                        0
                    } else if word.len() < 6 {
                        1
                    } else {
                        2
                    },
                    is_prefix,
                    &fst,
                )
            },
        )
        .unwrap();

        let graph = QueryGraph::from_query(&index, &txn, &mut db_cache, query).unwrap();
        println!("{}", graph.graphviz());

        // let positions_to_remove = vec![3, 6, 0, 4];
        // for p in positions_to_remove {
        //     graph.remove_words_at_position(p);
        //     println!("{}", graph.graphviz());
        // }

        // let proximities = |w1: &str, w2: &str| -> Vec<i8> {
        //     if matches!((w1, w2), ("56", "7")) {
        //         vec![]
        //     } else {
        //         vec![1, 2]
        //     }
        // };

        // let prox_graph = ProximityGraph::from_query_graph(graph, proximities);

        // println!("{}", prox_graph.graphviz());
    }
}

// fn remove_element_from_vector(v: &mut Vec<usize>, el: usize) {
//     let position = v.iter().position(|&x| x == el).unwrap();
//     v.swap_remove(position);
// }
