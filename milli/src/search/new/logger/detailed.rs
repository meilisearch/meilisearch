use rand::random;
use roaring::RoaringBitmap;
use std::fs::File;
use std::path::Path;
use std::{io::Write, path::PathBuf};

use crate::new::QueryNode;
use crate::new::query_term::{LocatedQueryTerm, QueryTerm, WordDerivations};
use crate::new::ranking_rule_graph::{Edge, EdgeDetails, RankingRuleGraphTrait};
use crate::new::ranking_rule_graph::{
    paths_map::PathsMap, proximity::ProximityGraph, RankingRuleGraph,
};

use super::{QueryGraph, RankingRule, RankingRuleQueryTrait, SearchLogger};

pub enum SearchEvents {
    RankingRuleStartIteration {
        ranking_rule_idx: usize,
        query: QueryGraph,
        universe: RoaringBitmap,
    },
    RankingRuleNextBucket {
        ranking_rule_idx: usize,
        universe: RoaringBitmap,
    },
    RankingRuleEndIteration {
        ranking_rule_idx: usize,
        universe: RoaringBitmap,
    },
    ExtendResults {
        new: RoaringBitmap,
    },
    WordsState {
        query_graph: QueryGraph,
    },
    ProximityState {
        graph: RankingRuleGraph<ProximityGraph>,
        paths: PathsMap<u64>,
    },
}

pub struct DetailedSearchLogger {
    folder_path: PathBuf,
    initial_query: Option<QueryGraph>,
    initial_universe: Option<RoaringBitmap>,
    ranking_rules_ids: Option<Vec<String>>,
    events: Vec<SearchEvents>,
}
impl DetailedSearchLogger {
    pub fn new(folder_path: &str) -> Self {
        Self {
            folder_path: PathBuf::new().join(folder_path),
            initial_query: <_>::default(),
            initial_universe: <_>::default(),
            ranking_rules_ids: <_>::default(),
            events: <_>::default(),
        }
    }
}

impl SearchLogger<QueryGraph> for DetailedSearchLogger {
    fn initial_query(&mut self, query: &QueryGraph) {
        self.initial_query = Some(query.clone());
    }

    fn initial_universe(&mut self, universe: &RoaringBitmap) {
        self.initial_universe = Some(universe.clone());
    }
    fn ranking_rules(&mut self, rr: &[Box<dyn RankingRule<QueryGraph>>]) {
        self.ranking_rules_ids = Some(rr.iter().map(|rr| rr.id()).collect());
    }

    fn start_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, QueryGraph>,
        query: &QueryGraph,
        universe: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleStartIteration {
            ranking_rule_idx,
            query: query.clone(),
            universe: universe.clone(),
        })
    }

    fn next_bucket_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, QueryGraph>,
        universe: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleNextBucket {
            ranking_rule_idx,
            universe: universe.clone(),
        })
    }

    fn end_iteration_ranking_rule<'transaction>(
        &mut self,
        ranking_rule_idx: usize,
        ranking_rule: &dyn RankingRule<'transaction, QueryGraph>,
        universe: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleEndIteration {
            ranking_rule_idx,
            universe: universe.clone(),
        })
    }
    fn add_to_results(&mut self, docids: &RoaringBitmap) {
        self.events.push(SearchEvents::ExtendResults { new: docids.clone() });
    }

    fn log_words_state(&mut self, query_graph: &QueryGraph) {
        self.events.push(SearchEvents::WordsState { query_graph: query_graph.clone() });
    }

    fn log_proximity_state(&mut self, query_graph: &RankingRuleGraph<ProximityGraph>, paths_map: &PathsMap<u64>,) {
        self.events.push(SearchEvents::ProximityState { graph: query_graph.clone(), paths: paths_map.clone() })
    }
    
    
}

impl DetailedSearchLogger {
    pub fn write_d2_description(&self) {
        let mut timestamp_idx = 0;
        let mut timestamp = vec![];
        fn activated_id(timestamp: &[usize]) -> String {
            let mut s = String::new();
            s.push('0');
            for t in timestamp.iter() {
                s.push_str(&format!("{t}"));
            }
            s
        }

        let index_path = self.folder_path.join("index.d2");
        let mut file = std::fs::File::create(index_path).unwrap();
        writeln!(&mut file, "Control Flow Between Ranking Rules: {{").unwrap();
        writeln!(&mut file, "shape: sequence_diagram");
        for (idx, rr_id) in self.ranking_rules_ids.as_ref().unwrap().iter().enumerate() {
            writeln!(&mut file, "{idx}: {rr_id}").unwrap();
        }
        writeln!(&mut file, "results");
        for event in self.events.iter() {
            match event {
                SearchEvents::RankingRuleStartIteration { query, universe, ranking_rule_idx } => {

                    let parent_activated_id = activated_id(&timestamp);
                    timestamp.push(0);
                    let self_activated_id = activated_id(&timestamp);
                    if *ranking_rule_idx != 0 {
                        let parent_ranking_rule_idx = ranking_rule_idx - 1;
                        writeln!(
                            &mut file,
                            "{parent_ranking_rule_idx}.{parent_activated_id} -> {ranking_rule_idx}.{self_activated_id} : start iteration",
                        )
                        .unwrap();
                    }
                    writeln!(&mut file, 
                    "{ranking_rule_idx}.{self_activated_id} {{
    style {{
        fill: \"#D8A7B1\"
    }}
}}").unwrap();
                }
                SearchEvents::RankingRuleNextBucket { universe, ranking_rule_idx } => {
                    let old_activated_id = activated_id(&timestamp);
                    *timestamp.last_mut().unwrap() += 1;
                    let next_activated_id = activated_id(&timestamp);
                    writeln!(&mut file, 
                        "{ranking_rule_idx}.{old_activated_id} -> {ranking_rule_idx}.{next_activated_id} : next bucket",)
                        .unwrap();
                }
                SearchEvents::RankingRuleEndIteration { universe, ranking_rule_idx } => {
                    let cur_activated_id = activated_id(&timestamp);
                    timestamp.pop();
                    let parent_activated_id = activated_id(&timestamp);
                    let parent_ranking_rule = if *ranking_rule_idx == 0 {
                        "start".to_owned()
                    } else {
                        format!("{}.{parent_activated_id}", ranking_rule_idx - 1)
                    };
                    writeln!(
                        &mut file,
                        "{ranking_rule_idx}.{cur_activated_id} -> {parent_ranking_rule} : end iteration",
                    )
                    .unwrap();
                }
                SearchEvents::ExtendResults { new } => {
                    if new.is_empty() {
                        continue
                    }
                    let cur_ranking_rule = timestamp.len() - 1;
                    let cur_activated_id = activated_id(&timestamp);
                    let docids = new.iter().collect::<Vec<_>>();
                    let len = new.len();
                    let random = random::<u64>();
                    
                    writeln!(
                        &mut file,
                        "{cur_ranking_rule}.{cur_activated_id} -> results.{random} : \"add {len}\"
results.{random} {{
    tooltip: \"{docids:?}\"
    style {{
        fill: \"#B6E2D3\"
    }}
}}
"
                    )
                    .unwrap();
                },
                SearchEvents::WordsState { query_graph } => {
                    let cur_ranking_rule = timestamp.len() - 1;
                    let cur_activated_id = activated_id(&timestamp);
                    let id = format!("{cur_ranking_rule}.{cur_activated_id}");
                    let mut new_file_path = self.folder_path.join(format!("{id}.d2"));
                    let mut new_file = std::fs::File::create(new_file_path).unwrap();
                    Self::query_graph_d2_description(query_graph, &mut new_file);
                    writeln!(
                        &mut file,
                        "{id} {{
    link: \"{id}.d2.svg\"
}}").unwrap();
                },
                SearchEvents::ProximityState { graph, paths } => {
                    let cur_ranking_rule = timestamp.len() - 1;
                    let cur_activated_id = activated_id(&timestamp);
                    let id = format!("{cur_ranking_rule}.{cur_activated_id}");
                    let mut new_file_path = self.folder_path.join(format!("{id}.d2"));
                    let mut new_file = std::fs::File::create(new_file_path).unwrap();
                    Self::proximity_graph_d2_description(graph, paths, &mut new_file);
                    writeln!(
                        &mut file,
                        "{id} {{
    link: \"{id}.d2.svg\"
}}").unwrap();
                },
            }
        }
        writeln!(&mut file, "}}");
    }
    
    fn query_node_d2_desc(node_idx: usize, node: &QueryNode, file: &mut File) {
        match &node {
            QueryNode::Term(LocatedQueryTerm { value, positions }) => {
                match value {
                    QueryTerm::Phrase(_) => todo!(),
                    QueryTerm::Word { derivations: WordDerivations { original, zero_typo, one_typo, two_typos, use_prefix_db } } => {
                        writeln!(file,"{node_idx} : \"{original}\" {{
shape: class").unwrap();
                        for w in zero_typo {
                            writeln!(file, "\"{w}\" : 0").unwrap();
                        }
                        for w in one_typo {
                            writeln!(file, "\"{w}\" : 1").unwrap();
                        }
                        for w in two_typos {
                            writeln!(file, "\"{w}\" : 2").unwrap();
                        }
                        if *use_prefix_db {
                            writeln!(file, "use prefix DB : true").unwrap();
                        }
                        writeln!(file, "}}").unwrap();
                    },
                }
            },
            QueryNode::Deleted => panic!(),
            QueryNode::Start => {
                writeln!(file,"{node_idx} : START").unwrap();
            },
            QueryNode::End => {
                writeln!(file,"{node_idx} : END").unwrap();
            },
        }
    }
    fn query_graph_d2_description(query_graph: &QueryGraph, file: &mut File) {
        writeln!(file,"direction: right");
        for node in 0..query_graph.nodes.len() {
            if matches!(query_graph.nodes[node], QueryNode::Deleted) {
                continue;
            }
            Self::query_node_d2_desc(node, &query_graph.nodes[node], file);
            
            for edge in query_graph.edges[node].successors.iter() {
                writeln!(file, "{node} -> {edge};\n").unwrap();
            }
        }        
    }
    fn proximity_graph_d2_description(graph: &RankingRuleGraph<ProximityGraph>, paths: &PathsMap<u64>, file: &mut File) {
        writeln!(file,"direction: right").unwrap();

        writeln!(file, "Proximity Graph {{").unwrap();
        for (node_idx, node) in graph.query_graph.nodes.iter().enumerate() {
            if matches!(node, QueryNode::Deleted) {
                continue;
            }
            Self::query_node_d2_desc(node_idx, node, file);
        }
        for edge in graph.all_edges.iter().flatten() {
            let Edge { from_node, to_node, cost, details } = edge;

            match &details {
                EdgeDetails::Unconditional => {
                    writeln!(file, 
                        "{from_node} -> {to_node} : \"always cost {cost}\"",
                        cost = edge.cost,
                    );
                }
                EdgeDetails::Data(details) => {
                    writeln!(file, 
                        "{from_node} -> {to_node} : \"cost {cost} {edge_label}\"",
                        cost = edge.cost,
                        edge_label = ProximityGraph::graphviz_edge_details_label(details)
                    );
                }
            }
        }
        writeln!(file, "}}").unwrap();

        writeln!(file, "Shortest Paths {{").unwrap();
        Self::paths_d2_description(graph, "", paths, file);
        writeln!(file, "}}").unwrap();
    }
    fn paths_d2_description(graph: &RankingRuleGraph<ProximityGraph>, paths_idx: &str, paths: &PathsMap<u64>, file: &mut File) { 

        for (edge_idx, rest) in paths.nodes.iter() {
            let Edge { from_node, to_node, cost, .. } = graph.all_edges[*edge_idx as usize].as_ref().unwrap() ;
            let from_node = &graph.query_graph.nodes[*from_node as usize];
            let from_node_desc = match from_node {
                QueryNode::Term(term) => match &term.value {
                    QueryTerm::Phrase(_) => todo!(),
                    QueryTerm::Word { derivations } => derivations.original.clone(),
                },
                QueryNode::Deleted => panic!(),
                QueryNode::Start => "START".to_owned(),
                QueryNode::End => "END".to_owned(),
            };
            let to_node = &graph.query_graph.nodes[*to_node as usize];
            let to_node_desc = match to_node {
                QueryNode::Term(term) => match &term.value {
                    QueryTerm::Phrase(_) => todo!(),
                    QueryTerm::Word { derivations } => derivations.original.clone(),
                },
                QueryNode::Deleted => panic!(),
                QueryNode::Start => "START".to_owned(),
                QueryNode::End => "END".to_owned(),
            };
            let edge_id = format!("{paths_idx}{edge_idx}");
            writeln!(file, "{edge_id}: \"{from_node_desc}->{to_node_desc} [{cost}]\" {{
                shape: class
            }}").unwrap();
            for (dest_edge_idx, _) in rest.nodes.iter() {
                let dest_edge_id = format!("{paths_idx}{edge_idx}{dest_edge_idx}");
                writeln!(file, "{edge_id} -> {dest_edge_id}").unwrap();
            }
            Self::paths_d2_description(graph, &format!("{paths_idx}{edge_idx}"), rest, file);
        }
    }
}
