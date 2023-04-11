use std::any::Any;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

// use rand::random;
use roaring::RoaringBitmap;

use crate::search::new::interner::Interned;
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::search::new::ranking_rule_graph::{
    Edge, ProximityCondition, ProximityGraph, RankingRuleGraph, RankingRuleGraphTrait,
    TypoCondition, TypoGraph,
};
use crate::search::new::ranking_rules::BoxRankingRule;
use crate::search::new::{QueryGraph, QueryNode, RankingRule, SearchContext, SearchLogger};
use crate::Result;

pub enum SearchEvents {
    RankingRuleStartIteration { ranking_rule_idx: usize, universe_len: u64 },
    RankingRuleNextBucket { ranking_rule_idx: usize, universe_len: u64, bucket_len: u64 },
    RankingRuleSkipBucket { ranking_rule_idx: usize, bucket_len: u64 },
    RankingRuleEndIteration { ranking_rule_idx: usize, universe_len: u64 },
    ExtendResults { new: Vec<u32> },
    WordsGraph { query_graph: QueryGraph },
    ProximityGraph { graph: RankingRuleGraph<ProximityGraph> },
    ProximityPaths { paths: Vec<Vec<Interned<ProximityCondition>>> },
    TypoGraph { graph: RankingRuleGraph<TypoGraph> },
    TypoPaths { paths: Vec<Vec<Interned<TypoCondition>>> },
}

enum Location {
    Words,
    Typo,
    Proximity,
    Other,
}

#[derive(Default)]
pub struct VisualSearchLogger {
    initial_query: Option<QueryGraph>,
    initial_query_time: Option<Instant>,
    query_for_universe: Option<QueryGraph>,
    initial_universe: Option<RoaringBitmap>,
    ranking_rules_ids: Option<Vec<String>>,
    events: Vec<SearchEvents>,
    location: Vec<Location>,
}

impl SearchLogger<QueryGraph> for VisualSearchLogger {
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
        ranking_rule: &dyn RankingRule<QueryGraph>,
        _query: &QueryGraph,
        universe: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleStartIteration {
            ranking_rule_idx,
            universe_len: universe.len(),
        });
        self.location.push(match ranking_rule.id().as_str() {
            "words" => Location::Words,
            "typo" => Location::Typo,
            "proximity" => Location::Proximity,
            _ => Location::Other,
        });
    }

    fn next_bucket_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<QueryGraph>,
        universe: &RoaringBitmap,
        bucket: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleNextBucket {
            ranking_rule_idx,
            universe_len: universe.len(),
            bucket_len: bucket.len(),
        });
    }
    fn skip_bucket_ranking_rule(
        &mut self,
        ranking_rule_idx: usize,
        _ranking_rule: &dyn RankingRule<QueryGraph>,
        bucket: &RoaringBitmap,
    ) {
        self.events.push(SearchEvents::RankingRuleSkipBucket {
            ranking_rule_idx,
            bucket_len: bucket.len(),
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
            universe_len: universe.len(),
        });
        self.location.pop();
    }
    fn add_to_results(&mut self, docids: &[u32]) {
        self.events.push(SearchEvents::ExtendResults { new: docids.to_vec() });
    }

    /// Logs the internal state of the ranking rule
    fn log_internal_state(&mut self, state: &dyn Any) {
        let Some(location) = self.location.last() else { return };
        match location {
            Location::Words => {
                if let Some(query_graph) = state.downcast_ref::<QueryGraph>() {
                    self.events.push(SearchEvents::WordsGraph { query_graph: query_graph.clone() });
                }
            }
            Location::Typo => {
                if let Some(graph) = state.downcast_ref::<RankingRuleGraph<TypoGraph>>() {
                    self.events.push(SearchEvents::TypoGraph { graph: graph.clone() });
                }
                if let Some(paths) = state.downcast_ref::<Vec<Vec<Interned<TypoCondition>>>>() {
                    self.events.push(SearchEvents::TypoPaths { paths: paths.clone() });
                }
            }
            Location::Proximity => {
                if let Some(graph) = state.downcast_ref::<RankingRuleGraph<ProximityGraph>>() {
                    self.events.push(SearchEvents::ProximityGraph { graph: graph.clone() });
                }
                if let Some(paths) = state.downcast_ref::<Vec<Vec<Interned<ProximityCondition>>>>()
                {
                    self.events.push(SearchEvents::ProximityPaths { paths: paths.clone() });
                }
            }
            Location::Other => {}
        }
    }
}

impl VisualSearchLogger {
    pub fn finish<'ctx>(self, ctx: &'ctx mut SearchContext<'ctx>, folder: &Path) -> Result<()> {
        let mut f = DetailedLoggerFinish::new(ctx, folder)?;
        f.finish(self)?;
        Ok(())
    }
}

struct DetailedLoggerFinish<'ctx> {
    ctx: &'ctx mut SearchContext<'ctx>,
    /// The folder where all the files should be printed
    folder_path: PathBuf,
    /// The main file visualising the search request
    index_file: BufWriter<File>,
    /// A vector of counters where each counter at index i represents the number of times
    /// that the ranking rule at idx i-1 was called since its last call to `start_iteration`.
    /// This is used to uniquely identify a point in the sequence diagram.
    rr_action_counter: Vec<usize>,
    /// The file storing information about the internal state of the latest active ranking rule
    file_for_internal_state: Option<BufWriter<File>>,
}

impl<'ctx> DetailedLoggerFinish<'ctx> {
    fn cur_file(&mut self) -> &mut BufWriter<File> {
        if let Some(file) = self.file_for_internal_state.as_mut() {
            file
        } else {
            &mut self.index_file
        }
    }
    fn pop_rr_action(&mut self) {
        self.file_for_internal_state = None;
        self.rr_action_counter.pop();
    }
    fn push_new_rr_action(&mut self) {
        self.file_for_internal_state = None;
        self.rr_action_counter.push(0);
    }
    fn increment_cur_rr_action(&mut self) {
        self.file_for_internal_state = None;
        if let Some(c) = self.rr_action_counter.last_mut() {
            *c += 1;
        }
    }
    fn id_of_timestamp(&self) -> String {
        let mut s = String::new();
        for t in self.rr_action_counter.iter() {
            s.push_str(&format!("{t}_"));
        }
        s
    }
    fn id_of_extend_results(&self) -> String {
        let mut s = String::new();
        s.push_str("results.\"");
        s.push_str(&self.id_of_timestamp());
        s.push('"');
        s
    }
    fn id_of_last_rr_action(&self) -> String {
        let mut s = String::new();
        let rr_id = if self.rr_action_counter.is_empty() {
            "start.\"".to_owned()
        } else {
            format!("{}.\"", self.rr_action_counter.len() - 1)
        };
        s.push_str(&rr_id);
        s.push_str(&self.id_of_timestamp());
        s.push('"');
        s
    }
    fn make_new_file_for_internal_state_if_needed(&mut self) -> Result<()> {
        if self.file_for_internal_state.is_some() {
            return Ok(());
        }
        let timestamp = self.id_of_timestamp();
        let id = self.id_of_last_rr_action();
        let new_file_path = self.folder_path.join(format!("{timestamp}.d2"));
        self.file_for_internal_state = Some(BufWriter::new(File::create(new_file_path)?));

        writeln!(
            &mut self.index_file,
            "{id} {{
    link: \"{timestamp}.d2.svg\"
}}"
        )?;
        Ok(())
    }
    fn new(ctx: &'ctx mut SearchContext<'ctx>, folder_path: &Path) -> Result<Self> {
        let index_path = folder_path.join("index.d2");
        let index_file = BufWriter::new(File::create(index_path)?);

        Ok(Self {
            ctx,
            folder_path: folder_path.to_owned(),
            index_file,
            rr_action_counter: vec![],
            file_for_internal_state: None,
        })
    }

    fn finish(&mut self, logger: VisualSearchLogger) -> Result<()> {
        writeln!(&mut self.index_file, "direction: right")?;
        if let Some(qg) = logger.initial_query {
            writeln!(&mut self.index_file, "Initial Query Graph: {{")?;
            self.write_query_graph(&qg)?;
            writeln!(&mut self.index_file, "}}")?;
        }
        if let Some(qg) = logger.query_for_universe {
            writeln!(&mut self.index_file, "Query Graph Used To Compute Universe: {{")?;
            self.write_query_graph(&qg)?;
            writeln!(&mut self.index_file, "}}")?;
        }
        let Some(ranking_rules_ids) = logger.ranking_rules_ids else { return Ok(()) };
        writeln!(&mut self.index_file, "Control Flow Between Ranking Rules: {{")?;
        writeln!(&mut self.index_file, "shape: sequence_diagram")?;
        writeln!(&mut self.index_file, "start")?;
        for (idx, rr_id) in ranking_rules_ids.iter().enumerate() {
            writeln!(&mut self.index_file, "{idx}: {rr_id}")?;
        }
        writeln!(&mut self.index_file, "results")?;
        for event in logger.events {
            self.write_event(event)?;
        }
        writeln!(&mut self.index_file, "}}")?;
        Ok(())
    }

    fn write_event(&mut self, e: SearchEvents) -> Result<()> {
        match e {
            SearchEvents::RankingRuleStartIteration { ranking_rule_idx, universe_len } => {
                assert!(ranking_rule_idx == self.rr_action_counter.len());
                self.write_start_iteration(universe_len)?;
            }
            SearchEvents::RankingRuleNextBucket { ranking_rule_idx, universe_len, bucket_len } => {
                assert!(ranking_rule_idx == self.rr_action_counter.len() - 1);
                self.write_next_bucket(bucket_len, universe_len)?;
            }
            SearchEvents::RankingRuleSkipBucket { ranking_rule_idx, bucket_len } => {
                assert!(ranking_rule_idx == self.rr_action_counter.len() - 1);
                self.write_skip_bucket(bucket_len)?;
            }
            SearchEvents::RankingRuleEndIteration { ranking_rule_idx, universe_len: _ } => {
                assert!(ranking_rule_idx == self.rr_action_counter.len() - 1);
                self.write_end_iteration()?;
            }
            SearchEvents::ExtendResults { new } => {
                self.write_extend_results(new)?;
            }
            SearchEvents::WordsGraph { query_graph } => self.write_words_graph(query_graph)?,
            SearchEvents::ProximityGraph { graph } => self.write_rr_graph(&graph)?,
            SearchEvents::ProximityPaths { paths } => {
                self.write_rr_graph_paths::<ProximityGraph>(paths)?;
            }
            SearchEvents::TypoGraph { graph } => self.write_rr_graph(&graph)?,
            SearchEvents::TypoPaths { paths } => {
                self.write_rr_graph_paths::<TypoGraph>(paths)?;
            }
        }
        Ok(())
    }
    fn write_query_graph(&mut self, qg: &QueryGraph) -> Result<()> {
        writeln!(self.cur_file(), "direction: right")?;
        for (node_id, node) in qg.nodes.iter() {
            if matches!(node.data, QueryNodeData::Deleted) {
                continue;
            }
            self.write_query_node(node_id, node)?;

            for edge in node.successors.iter() {
                writeln!(self.cur_file(), "{node_id} -> {edge};\n").unwrap();
            }
        }
        Ok(())
    }

    fn write_start_iteration(&mut self, _universe_len: u64) -> Result<()> {
        let parent_action_id = self.id_of_last_rr_action();
        self.push_new_rr_action();
        let self_action_id = self.id_of_last_rr_action();
        writeln!(&mut self.index_file, "{parent_action_id} -> {self_action_id} : start iteration")?;
        writeln!(
            &mut self.index_file,
            "{self_action_id} {{
style {{
fill: \"#D8A7B1\"
}}
}}"
        )?;

        Ok(())
    }
    fn write_next_bucket(&mut self, bucket_len: u64, universe_len: u64) -> Result<()> {
        let cur_action_id = self.id_of_last_rr_action();
        self.increment_cur_rr_action();
        let next_action_id = self.id_of_last_rr_action();
        writeln!(
            &mut self.index_file,
            "{cur_action_id} -> {next_action_id} : next bucket {bucket_len}/{universe_len}"
        )?;

        Ok(())
    }
    fn write_skip_bucket(&mut self, bucket_len: u64) -> Result<()> {
        let cur_action_id = self.id_of_last_rr_action();
        self.increment_cur_rr_action();
        let next_action_id = self.id_of_last_rr_action();
        writeln!(
            &mut self.index_file,
            "{cur_action_id} -> {next_action_id} : skip bucket ({bucket_len})"
        )?;

        Ok(())
    }
    fn write_end_iteration(&mut self) -> Result<()> {
        let cur_action_id = self.id_of_last_rr_action();
        self.pop_rr_action();
        let parent_action_id = self.id_of_last_rr_action();

        writeln!(&mut self.index_file, "{cur_action_id} -> {parent_action_id} : end iteration",)?;
        Ok(())
    }
    fn write_extend_results(&mut self, new: Vec<u32>) -> Result<()> {
        if new.is_empty() {
            return Ok(());
        }

        let cur_action_id = self.id_of_last_rr_action();
        let results_id = self.id_of_extend_results();
        let docids = new.iter().collect::<Vec<_>>();
        let len = new.len();

        writeln!(
            &mut self.index_file,
            "{cur_action_id} -> {results_id} : \"add {len}\"
{results_id} {{
tooltip: \"{docids:?}\"
style {{
fill: \"#B6E2D3\"
}}
}}
"
        )?;
        Ok(())
    }

    fn write_query_node(&mut self, node_idx: Interned<QueryNode>, node: &QueryNode) -> Result<()> {
        let Self {
            ctx, index_file, file_for_internal_state: active_ranking_rule_state_file, ..
        } = self;
        let file = if let Some(file) = active_ranking_rule_state_file.as_mut() {
            file
        } else {
            index_file
        };
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
                )?;

                for w in term_subset.all_single_words_except_prefix_db(ctx)? {
                    let w = ctx.word_interner.get(w);
                    writeln!(file, "{w}: word")?;
                }
                for p in term_subset.all_phrases(ctx)? {
                    writeln!(file, "{}: phrase", p.description(ctx))?;
                }
                if let Some(w) = term_subset.use_prefix_db(ctx) {
                    let w = ctx.word_interner.get(w);
                    writeln!(file, "{w}: prefix db")?;
                }

                writeln!(file, "}}")?;
            }
            QueryNodeData::Deleted => panic!(),
            QueryNodeData::Start => {
                writeln!(file, "{node_idx} : START")?;
            }
            QueryNodeData::End => {
                writeln!(file, "{node_idx} : END")?;
            }
        }
        Ok(())
    }
    fn write_words_graph(&mut self, qg: QueryGraph) -> Result<()> {
        self.make_new_file_for_internal_state_if_needed()?;

        self.write_query_graph(&qg)?;

        Ok(())
    }
    fn write_rr_graph<R: RankingRuleGraphTrait>(
        &mut self,
        graph: &RankingRuleGraph<R>,
    ) -> Result<()> {
        self.make_new_file_for_internal_state_if_needed()?;

        writeln!(self.cur_file(), "direction: right")?;

        writeln!(self.cur_file(), "Graph {{")?;
        for (node_idx, node) in graph.query_graph.nodes.iter() {
            if matches!(&node.data, QueryNodeData::Deleted) {
                continue;
            }
            self.write_query_node(node_idx, node)?;
        }
        for (_edge_id, edge) in graph.edges_store.iter() {
            let Some(edge) = edge else { continue };
            let Edge { source_node, dest_node, condition: details, cost, nodes_to_skip: _ } = edge;

            match &details {
                None => {
                    writeln!(
                        self.cur_file(),
                        "{source_node} -> {dest_node} : \"always cost {cost}\"",
                    )?;
                }
                Some(condition) => {
                    writeln!(
                        self.cur_file(),
                        "{source_node} -> {dest_node} : \"{condition} cost {cost}\"",
                        cost = edge.cost,
                    )?;
                }
            }
        }
        writeln!(self.cur_file(), "}}")?;

        Ok(())
    }

    fn write_rr_graph_paths<R: RankingRuleGraphTrait>(
        &mut self,
        paths: Vec<Vec<Interned<R::Condition>>>,
    ) -> Result<()> {
        self.make_new_file_for_internal_state_if_needed()?;
        let file = if let Some(file) = self.file_for_internal_state.as_mut() {
            file
        } else {
            &mut self.index_file
        };
        writeln!(file, "Path {{")?;
        for (path_idx, condition_indexes) in paths.iter().enumerate() {
            writeln!(file, "{path_idx} {{")?;
            for condition in condition_indexes.iter() {
                writeln!(file, "{condition}")?;
            }
            for couple_edges in condition_indexes.windows(2) {
                let [src_edge_idx, dest_edge_idx] = couple_edges else { panic!() };
                writeln!(file, "{src_edge_idx} -> {dest_edge_idx}")?;
            }
            writeln!(file, "}}")?;
        }
        writeln!(file, "}}")?;
        Ok(())
    }
}
