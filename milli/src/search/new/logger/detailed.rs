use std::any::Any;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

// use rand::random;
use roaring::RoaringBitmap;

use crate::search::new::graph_based_ranking_rule::Typo;
use crate::search::new::interner::{Interned, MappedInterner};
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::ranking_rule_graph::{
    DeadEndsCache, Edge, ProximityCondition, ProximityGraph, RankingRuleGraph,
    RankingRuleGraphTrait, TypoCondition, TypoGraph,
};
use crate::search::new::ranking_rules::BoxRankingRule;
use crate::search::new::sort::Sort;
use crate::search::new::words::Words;
use crate::search::new::{QueryGraph, QueryNode, RankingRule, SearchContext, SearchLogger};

pub enum SearchEvents {
    RankingRuleStartIteration {
        ranking_rule_idx: usize,
        query: QueryGraph,
        universe: RoaringBitmap,
        time: Instant,
    },
    RankingRuleNextBucket {
        ranking_rule_idx: usize,
        universe: RoaringBitmap,
        candidates: RoaringBitmap,
        time: Instant,
    },
    RankingRuleEndIteration {
        ranking_rule_idx: usize,
        universe: RoaringBitmap,
        time: Instant,
    },
    ExtendResults {
        new: Vec<u32>,
    },
    WordsState {
        query_graph: QueryGraph,
    },
    ProximityState {
        graph: RankingRuleGraph<ProximityGraph>,
        paths: Vec<Vec<Interned<ProximityCondition>>>,
        dead_ends_cache: DeadEndsCache<ProximityCondition>,
        universe: RoaringBitmap,
        costs: MappedInterner<QueryNode, Vec<u64>>,
        cost: u64,
    },
    TypoState {
        graph: RankingRuleGraph<TypoGraph>,
        paths: Vec<Vec<Interned<TypoCondition>>>,
        dead_ends_cache: DeadEndsCache<TypoCondition>,
        universe: RoaringBitmap,
        costs: MappedInterner<QueryNode, Vec<u64>>,
        cost: u64,
    },
    RankingRuleSkipBucket {
        ranking_rule_idx: usize,
        candidates: RoaringBitmap,
        time: Instant,
    },
}

pub struct DetailedSearchLogger {
    folder_path: PathBuf,
    initial_query: Option<QueryGraph>,
    initial_query_time: Option<Instant>,
    query_for_universe: Option<QueryGraph>,
    initial_universe: Option<RoaringBitmap>,
    ranking_rules_ids: Option<Vec<String>>,
    events: Vec<SearchEvents>,
}
impl DetailedSearchLogger {
    pub fn new(folder_path: &str) -> Self {
        Self {
            folder_path: PathBuf::new().join(folder_path),
            initial_query: None,
            initial_query_time: None,
            query_for_universe: None,
            initial_universe: None,
            ranking_rules_ids: None,
            events: vec![],
        }
    }
}

impl SearchLogger<QueryGraph> for DetailedSearchLogger {
    fn initial_query(&mut self, query: &QueryGraph) {
        self.initial_query = Some(query.clone());
        self.initial_query_time = Some(Instant::now());
    }

    fn query_for_initial_universe(&mut self, query: &QueryGraph) {
        self.query_for_universe = Some(query.clone());
    }

    fn initial_universe(&mut self, universe: &RoaringBitmap) {
        self.initial_universe = Some(universe.clone());
    }
    fn ranking_rules(&mut self, rr: &[BoxRankingRule<QueryGraph>]) {
        self.ranking_rules_ids = Some(rr.iter().map(|rr| rr.id()).collect());
    }

    fn start_iteration_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<QueryGraph>,
        query: &QueryGraph,
        universe: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleStartIteration {
            ranking_rule_idx,
            query: query.clone(),
            universe: universe.clone(),
            time: Instant::now(),
        })
    }

    fn next_bucket_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<QueryGraph>,
        universe: &RoaringBitmap,
        candidates: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleNextBucket {
            ranking_rule_idx,
            universe: universe.clone(),
            candidates: candidates.clone(),
            time: Instant::now(),
        })
    }
    fn skip_bucket_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<QueryGraph>,
        candidates: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleSkipBucket {
            ranking_rule_idx,
            candidates: candidates.clone(),
            time: Instant::now(),
        })
    }

    fn end_iteration_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<QueryGraph>,
        universe: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleEndIteration {
            ranking_rule_idx,
            universe: universe.clone(),
            time: Instant::now(),
        })
    }
    fn add_to_results(&mut self, docids: &[u32]) {
        self.events.push(SearchEvents::ExtendResults { new: docids.to_vec() });
    }

    /// Logs the internal state of the ranking rule
    fn log_ranking_rule_state<'ctx>(&mut self, state: &(dyn Any + 'ctx)) {
        if let Some(_words) = state.downcast_ref::<Words>() {
        } else if let Some(_sort) = state.downcast_ref::<Sort<'ctx, QueryGraph>>() {
        } else if let Some(_typo) = state.downcast_ref::<Typo>() {
        }
    }
}

impl DetailedSearchLogger {
    pub fn write_d2_description(&self, ctx: &mut SearchContext) {
        let mut prev_time = self.initial_query_time.unwrap();
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
        writeln!(&mut file, "direction: right").unwrap();
        writeln!(&mut file, "Initial Query Graph: {{").unwrap();
        let initial_query_graph = self.initial_query.as_ref().unwrap();
        Self::query_graph_d2_description(ctx, initial_query_graph, &mut file);
        writeln!(&mut file, "}}").unwrap();

        writeln!(&mut file, "Query Graph Used To Compute Universe: {{").unwrap();
        let query_graph_for_universe = self.query_for_universe.as_ref().unwrap();
        Self::query_graph_d2_description(ctx, query_graph_for_universe, &mut file);
        writeln!(&mut file, "}}").unwrap();

        let initial_universe = self.initial_universe.as_ref().unwrap();
        writeln!(&mut file, "Initial Universe Length {}", initial_universe.len()).unwrap();

        writeln!(&mut file, "Control Flow Between Ranking Rules: {{").unwrap();
        writeln!(&mut file, "shape: sequence_diagram").unwrap();
        for (idx, rr_id) in self.ranking_rules_ids.as_ref().unwrap().iter().enumerate() {
            writeln!(&mut file, "{idx}: {rr_id}").unwrap();
        }
        writeln!(&mut file, "results").unwrap();
        // writeln!(&mut file, "time").unwrap();
        for event in self.events.iter() {
            match event {
                SearchEvents::RankingRuleStartIteration { ranking_rule_idx, time, .. } => {
                    let _elapsed = time.duration_since(prev_time);
                    prev_time = *time;
                    let parent_activated_id = activated_id(&timestamp);
                    timestamp.push(0);
                    let self_activated_id = activated_id(&timestamp);
                    // writeln!(&mut file, "time.{self_activated_id}: {:.2}", elapsed.as_micros() as f64 / 1000.0).unwrap();
                    if *ranking_rule_idx != 0 {
                        let parent_ranking_rule_idx = ranking_rule_idx - 1;
                        writeln!(
                            &mut file,
                            "{parent_ranking_rule_idx}.{parent_activated_id} -> {ranking_rule_idx}.{self_activated_id} : start iteration",
                        )
                        .unwrap();
                    }
                    writeln!(
                        &mut file,
                        "{ranking_rule_idx}.{self_activated_id} {{
    style {{
        fill: \"#D8A7B1\"
    }}
}}"
                    )
                    .unwrap();
                }
                SearchEvents::RankingRuleNextBucket {
                    ranking_rule_idx,
                    time,
                    universe,
                    candidates,
                } => {
                    let _elapsed = time.duration_since(prev_time);
                    prev_time = *time;
                    let old_activated_id = activated_id(&timestamp);
                    // writeln!(&mut file, "time.{old_activated_id}: {:.2}", elapsed.as_micros() as f64 / 1000.0).unwrap();
                    *timestamp.last_mut().unwrap() += 1;
                    let next_activated_id = activated_id(&timestamp);
                    writeln!(&mut file,
                        "{ranking_rule_idx}.{old_activated_id} -> {ranking_rule_idx}.{next_activated_id} : next bucket {}/{}", candidates.len(), universe.len())
                        .unwrap();
                }
                SearchEvents::RankingRuleSkipBucket { ranking_rule_idx, candidates, time } => {
                    let _elapsed = time.duration_since(prev_time);
                    prev_time = *time;
                    let old_activated_id = activated_id(&timestamp);
                    // writeln!(&mut file, "time.{old_activated_id}: {:.2}", elapsed.as_micros() as f64 / 1000.0).unwrap();
                    *timestamp.last_mut().unwrap() += 1;
                    let next_activated_id = activated_id(&timestamp);
                    let len = candidates.len();
                    writeln!(&mut file,
                        "{ranking_rule_idx}.{old_activated_id} -> {ranking_rule_idx}.{next_activated_id} : skip bucket ({len})",)
                        .unwrap();
                }
                SearchEvents::RankingRuleEndIteration { ranking_rule_idx, time, .. } => {
                    let _elapsed = time.duration_since(prev_time);
                    prev_time = *time;
                    let cur_activated_id = activated_id(&timestamp);
                    // writeln!(&mut file, "time.{cur_activated_id}: {:.2}", elapsed.as_micros() as f64 / 1000.0).unwrap();

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
                        continue;
                    }
                    let cur_ranking_rule = timestamp.len() - 1;
                    let cur_activated_id = activated_id(&timestamp);
                    let docids = new.iter().collect::<Vec<_>>();
                    let len = new.len();

                    writeln!(
                        &mut file,
                        "{cur_ranking_rule}.{cur_activated_id} -> results.{cur_ranking_rule}{cur_activated_id} : \"add {len}\"
results.{cur_ranking_rule}{cur_activated_id} {{
    tooltip: \"{docids:?}\"
    style {{
        fill: \"#B6E2D3\"
    }}
}}
"
                    )
                    .unwrap();
                }
                SearchEvents::WordsState { query_graph } => {
                    let cur_ranking_rule = timestamp.len() - 1;
                    *timestamp.last_mut().unwrap() += 1;
                    let cur_activated_id = activated_id(&timestamp);
                    *timestamp.last_mut().unwrap() -= 1;
                    let id = format!("{cur_ranking_rule}.{cur_activated_id}");
                    let new_file_path = self.folder_path.join(format!("{id}.d2"));
                    let mut new_file = std::fs::File::create(new_file_path).unwrap();
                    Self::query_graph_d2_description(ctx, query_graph, &mut new_file);
                    writeln!(
                        &mut file,
                        "{id} {{
    link: \"{id}.d2.svg\"
}}"
                    )
                    .unwrap();
                }
                SearchEvents::ProximityState {
                    graph,
                    paths,
                    dead_ends_cache,
                    universe,
                    costs,
                    cost,
                } => {
                    let cur_ranking_rule = timestamp.len() - 1;
                    *timestamp.last_mut().unwrap() += 1;
                    let cur_activated_id = activated_id(&timestamp);
                    *timestamp.last_mut().unwrap() -= 1;
                    let id = format!("{cur_ranking_rule}.{cur_activated_id}");
                    let new_file_path = self.folder_path.join(format!("{id}.d2"));
                    let mut new_file = std::fs::File::create(new_file_path).unwrap();
                    Self::ranking_rule_graph_d2_description(
                        ctx,
                        graph,
                        paths,
                        dead_ends_cache,
                        costs.clone(),
                        &mut new_file,
                    );
                    writeln!(
                        &mut file,
                        "{id} {{
                    link: \"{id}.d2.svg\"
                    tooltip: \"cost {cost}, universe len: {}\"
                }}",
                        universe.len()
                    )
                    .unwrap();
                }
                SearchEvents::TypoState {
                    graph,
                    paths,
                    dead_ends_cache,
                    universe,
                    costs,
                    cost,
                } => {
                    let cur_ranking_rule = timestamp.len() - 1;
                    *timestamp.last_mut().unwrap() += 1;
                    let cur_activated_id = activated_id(&timestamp);
                    *timestamp.last_mut().unwrap() -= 1;
                    let id = format!("{cur_ranking_rule}.{cur_activated_id}");
                    let new_file_path = self.folder_path.join(format!("{id}.d2"));
                    let mut new_file = std::fs::File::create(new_file_path).unwrap();
                    Self::ranking_rule_graph_d2_description(
                        ctx,
                        graph,
                        paths,
                        dead_ends_cache,
                        costs.clone(),
                        &mut new_file,
                    );
                    writeln!(
                        &mut file,
                        "{id} {{
    link: \"{id}.d2.svg\"
    tooltip: \"cost {cost}, universe len: {}\"
}}",
                        universe.len()
                    )
                    .unwrap();
                }
            }
        }
        writeln!(&mut file, "}}").unwrap();
    }

    fn query_node_d2_desc(
        ctx: &mut SearchContext,
        node_idx: Interned<QueryNode>,
        node: &QueryNode,
        _costs: &[u64],
        file: &mut File,
    ) {
        match &node.data {
            QueryNodeData::Term(LocatedQueryTermSubset {
                term_subset,
                positions: _,
                term_ids: _,
            }) => {
                writeln!(
                    file,
                    "{node_idx} : \"{}\" {{
                shape: class
                max_nbr_typo: {}",
                    term_subset.description(ctx),
                    term_subset.max_nbr_typos(ctx)
                )
                .unwrap();

                for w in term_subset.all_single_words_except_prefix_db(ctx).unwrap() {
                    let w = ctx.word_interner.get(w);
                    writeln!(file, "{w}: word").unwrap();
                }
                for p in term_subset.all_phrases(ctx).unwrap() {
                    writeln!(file, "{}: phrase", p.description(ctx)).unwrap();
                }
                if let Some(w) = term_subset.use_prefix_db(ctx) {
                    let w = ctx.word_interner.get(w);
                    writeln!(file, "{w}: prefix db").unwrap();
                }

                writeln!(file, "}}").unwrap();
            }
            QueryNodeData::Deleted => panic!(),
            QueryNodeData::Start => {
                writeln!(file, "{node_idx} : START").unwrap();
            }
            QueryNodeData::End => {
                writeln!(file, "{node_idx} : END").unwrap();
            }
        }
    }
    fn query_graph_d2_description(
        ctx: &mut SearchContext,
        query_graph: &QueryGraph,
        file: &mut File,
    ) {
        writeln!(file, "direction: right").unwrap();
        for (node_id, node) in query_graph.nodes.iter() {
            if matches!(node.data, QueryNodeData::Deleted) {
                continue;
            }
            Self::query_node_d2_desc(ctx, node_id, node, &[], file);

            for edge in node.successors.iter() {
                writeln!(file, "{node_id} -> {edge};\n").unwrap();
            }
        }
    }
    fn ranking_rule_graph_d2_description<R: RankingRuleGraphTrait>(
        ctx: &mut SearchContext,
        graph: &RankingRuleGraph<R>,
        paths: &[Vec<Interned<R::Condition>>],
        _dead_ends_cache: &DeadEndsCache<R::Condition>,
        costs: MappedInterner<QueryNode, Vec<u64>>,
        file: &mut File,
    ) {
        writeln!(file, "direction: right").unwrap();

        writeln!(file, "Proximity Graph {{").unwrap();
        for (node_idx, node) in graph.query_graph.nodes.iter() {
            if matches!(&node.data, QueryNodeData::Deleted) {
                continue;
            }
            let costs = &costs.get(node_idx);
            Self::query_node_d2_desc(ctx, node_idx, node, costs, file);
        }
        for (_edge_id, edge) in graph.edges_store.iter() {
            let Some(edge) = edge else { continue };
            let Edge { source_node, dest_node, condition: details, cost, nodes_to_skip: _ } = edge;

            match &details {
                None => {
                    writeln!(file, "{source_node} -> {dest_node} : \"always cost {cost}\"",)
                        .unwrap();
                }
                Some(condition) => {
                    // let condition = graph.conditions_interner.get(*condition);
                    writeln!(
                        file,
                        "{source_node} -> {dest_node} : \"{condition} cost {cost}\"",
                        cost = edge.cost,
                    )
                    .unwrap();
                }
            }
        }
        writeln!(file, "}}").unwrap();

        // writeln!(file, "costs {{").unwrap();
        // Self::paths_d2_description(graph, paths, file);
        // writeln!(file, "}}").unwrap();

        writeln!(file, "Paths {{").unwrap();
        Self::paths_d2_description(ctx, graph, paths, file);
        writeln!(file, "}}").unwrap();

        // writeln!(file, "Dead-end couples of conditions {{").unwrap();
        // for (i, (e1, e2)) in dead_end_paths_cache.condition_couples.iter().enumerate() {
        //     writeln!(file, "{i} : \"\" {{").unwrap();
        //     Self::condition_d2_description(ctx, graph, e1, file);
        //     for e2 in e2.iter() {
        //         Self::condition_d2_description(ctx, graph, e2, file);
        //         writeln!(file, "{e1} -- {e2}").unwrap();
        //     }
        //     writeln!(file, "}}").unwrap();
        // }
        // writeln!(file, "}}").unwrap();

        // writeln!(file, "Dead-end edges {{").unwrap();
        // for condition in dead_end_paths_cache.conditions.iter() {
        //     writeln!(file, "{condition}").unwrap();
        // }
        // writeln!(file, "}}").unwrap();

        // writeln!(file, "Dead-end prefixes {{").unwrap();
        // writeln!(file, "}}").unwrap();
    }
    fn condition_d2_description<R: RankingRuleGraphTrait>(
        ctx: &mut SearchContext,
        graph: &RankingRuleGraph<R>,
        condition_id: Interned<R::Condition>,
        file: &mut File,
    ) {
        let condition = graph.conditions_interner.get(condition_id);
        writeln!(
            file,
            "{condition_id} {{
shape: class
label
}}",
        )
        .unwrap();
    }
    fn paths_d2_description<R: RankingRuleGraphTrait>(
        ctx: &mut SearchContext,
        graph: &RankingRuleGraph<R>,
        paths: &[Vec<Interned<R::Condition>>],
        file: &mut File,
    ) {
        for (path_idx, condition_indexes) in paths.iter().enumerate() {
            writeln!(file, "{path_idx} {{").unwrap();
            for condition in condition_indexes.iter() {
                Self::condition_d2_description(ctx, graph, *condition, file);
            }
            for couple_edges in condition_indexes.windows(2) {
                let [src_edge_idx, dest_edge_idx] = couple_edges else { panic!() };
                writeln!(file, "{src_edge_idx} -> {dest_edge_idx}").unwrap();
            }
            writeln!(file, "}}").unwrap();
        }
    }
}
