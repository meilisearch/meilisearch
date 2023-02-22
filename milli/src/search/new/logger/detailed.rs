use rand::random;
use roaring::RoaringBitmap;
use std::fs::File;
use std::path::Path;
use std::{io::Write, path::PathBuf};

use crate::new::QueryNode;
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
        let mut file = std::fs::File::create(&index_path).unwrap();
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
                    Self::query_graph_d2_description(&query_graph, &mut new_file);
                    writeln!(
                        &mut file,
                        "{id} {{
    link: \"{id}.d2.svg\"
}}").unwrap();
                },
                SearchEvents::ProximityState { graph, paths } => todo!(),
            }
        }
        writeln!(&mut file, "}}");
    }
    fn query_graph_d2_description(query_graph: &QueryGraph, file: &mut File) {
        writeln!(file,"direction: right");
        for node in 0..query_graph.nodes.len() {
            if matches!(query_graph.nodes[node], QueryNode::Deleted) {
                continue;
            }
            writeln!(file,"{node}");

            for edge in query_graph.edges[node].successors.iter() {
                writeln!(file, "{node} -> {edge};\n").unwrap();
            }
        }        
    }
}
