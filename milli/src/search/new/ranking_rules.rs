use std::time::Instant;

use heed::RoTxn;
use roaring::RoaringBitmap;

use super::db_cache::DatabaseCache;
use super::logger::SearchLogger;

use super::QueryGraph;
use crate::new::graph_based_ranking_rule::GraphBasedRankingRule;
use crate::new::ranking_rule_graph::proximity::ProximityGraph;
use crate::new::ranking_rule_graph::typo::TypoGraph;
use crate::new::words::Words;
// use crate::search::new::sort::Sort;
use crate::{Filter, Index, Result, TermsMatchingStrategy};

pub trait RankingRuleOutputIter<'transaction, Query> {
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput<Query>>>;
}

pub struct RankingRuleOutputIterWrapper<'transaction, Query> {
    iter: Box<dyn Iterator<Item = Result<RankingRuleOutput<Query>>> + 'transaction>,
}
impl<'transaction, Query> RankingRuleOutputIterWrapper<'transaction, Query> {
    pub fn new(
        iter: Box<dyn Iterator<Item = Result<RankingRuleOutput<Query>>> + 'transaction>,
    ) -> Self {
        Self { iter }
    }
}
impl<'transaction, Query> RankingRuleOutputIter<'transaction, Query>
    for RankingRuleOutputIterWrapper<'transaction, Query>
{
    fn next_bucket(&mut self) -> Result<Option<RankingRuleOutput<Query>>> {
        match self.iter.next() {
            Some(x) => x.map(Some),
            None => Ok(None),
        }
    }
}

pub trait RankingRuleQueryTrait: Sized + Clone + 'static {}

#[derive(Clone)]
pub struct PlaceholderQuery;
impl RankingRuleQueryTrait for PlaceholderQuery {}
impl RankingRuleQueryTrait for QueryGraph {}

pub trait RankingRule<'transaction, Query: RankingRuleQueryTrait> {
    fn id(&self) -> String;

    /// Prepare the ranking rule such that it can start iterating over its
    /// buckets using [`next_bucket`](RankingRule::next_bucket).
    ///
    /// The given universe is the universe that will be given to [`next_bucket`](RankingRule::next_bucket).
    fn start_iteration(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
        query: &Query,
    ) -> Result<()>;

    /// Return the next bucket of this ranking rule.
    ///
    /// The returned candidates MUST be a subset of the given universe.
    ///
    /// The universe given as argument is either:
    /// - a subset of the universe given to the previous call to [`next_bucket`](RankingRule::next_bucket); OR
    /// - the universe given to [`start_iteration`](RankingRule::start_iteration)
    fn next_bucket(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<Query>,
        universe: &RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<Query>>>;

    /// Finish iterating over the buckets, which yields control to the parent ranking rule
    /// The next call to this ranking rule, if any, will be [`start_iteration`](RankingRule::start_iteration).
    fn end_iteration(
        &mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        logger: &mut dyn SearchLogger<Query>,
    );
}

#[derive(Debug)]
pub struct RankingRuleOutput<Q> {
    /// The query corresponding to the current bucket for the child ranking rule
    pub query: Q,
    /// The allowed candidates for the child ranking rule
    pub candidates: RoaringBitmap,
}

// TODO: can make it generic over the query type (either query graph or placeholder) fairly easily
#[allow(clippy::too_many_arguments)]
pub fn execute_search<'transaction>(
    index: &Index,
    txn: &'transaction heed::RoTxn,
    // TODO: ranking rules parameter
    db_cache: &mut DatabaseCache<'transaction>,
    query_graph: &QueryGraph,
    filters: Option<Filter>,
    from: usize,
    length: usize,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<Vec<u32>> {
    logger.initial_query(query_graph, Instant::now());
    let words = &mut Words::new(TermsMatchingStrategy::Last);
    // let sort = &mut Sort::new(index, txn, "release_date".to_owned(), true)?;
    let proximity = &mut GraphBasedRankingRule::<ProximityGraph>::new("proximity".to_owned());
    let typo = &mut GraphBasedRankingRule::<TypoGraph>::new("typo".to_owned());
    // TODO: ranking rules given as argument
    let mut ranking_rules: Vec<&mut dyn RankingRule<'transaction, QueryGraph>> =
        vec![words, typo, proximity /*sort*/];

    logger.ranking_rules(&ranking_rules);

    let universe = if let Some(filters) = filters {
        filters.evaluate(txn, index)?
    } else {
        index.documents_ids(txn)?
    };

    if universe.len() < from as u64 {
        return Ok(vec![]);
    }

    let ranking_rules_len = ranking_rules.len();
    logger.start_iteration_ranking_rule(
        0,
        ranking_rules[0],
        query_graph,
        &universe,
        Instant::now(),
    );
    ranking_rules[0].start_iteration(index, txn, db_cache, logger, &universe, query_graph)?;

    let mut candidates = vec![RoaringBitmap::default(); ranking_rules_len];
    candidates[0] = universe.clone();

    let mut cur_ranking_rule_index = 0;

    macro_rules! back {
        () => {
            assert!(candidates[cur_ranking_rule_index].is_empty());
            logger.end_iteration_ranking_rule(
                cur_ranking_rule_index,
                ranking_rules[cur_ranking_rule_index],
                &candidates[cur_ranking_rule_index],
                Instant::now(),
            );
            candidates[cur_ranking_rule_index].clear();
            ranking_rules[cur_ranking_rule_index].end_iteration(index, txn, db_cache, logger);
            if cur_ranking_rule_index == 0 {
                break;
            } else {
                cur_ranking_rule_index -= 1;
            }
        };
    }

    let mut results = vec![];
    let mut cur_offset = 0usize;

    // Add the candidates to the results. Take the `from`, `limit`, and `cur_offset`
    // into account and inform the logger.
    macro_rules! maybe_add_to_results {
        ($candidates:expr) => {
            let candidates = $candidates;
            let len = candidates.len();
            // if the candidates are empty, there is nothing to do;
            if !candidates.is_empty() {
                if cur_offset < from {
                    if cur_offset + (candidates.len() as usize) < from {
                        logger.skip_bucket_ranking_rule(
                            cur_ranking_rule_index,
                            ranking_rules[cur_ranking_rule_index],
                            &candidates,
                            Instant::now(),
                        );
                    } else {
                        let all_candidates = candidates.iter().collect::<Vec<_>>();
                        let (skipped_candidates, candidates) =
                            all_candidates.split_at(from - cur_offset);
                        logger.skip_bucket_ranking_rule(
                            cur_ranking_rule_index,
                            ranking_rules[cur_ranking_rule_index],
                            &skipped_candidates.into_iter().collect(),
                            Instant::now(),
                        );
                        let candidates = candidates
                            .iter()
                            .take(length - results.len())
                            .copied()
                            .collect::<Vec<_>>();
                        logger.add_to_results(&candidates);
                        results.extend(&candidates);
                    }
                } else {
                    let candidates =
                        candidates.iter().take(length - results.len()).collect::<Vec<_>>();
                    logger.add_to_results(&candidates);
                    results.extend(&candidates);
                }
            }
            cur_offset += len as usize;
        };
    }
    while results.len() < length {
        // The universe for this bucket is zero or one element, so we don't need to sort
        // anything, just extend the results and go back to the parent ranking rule.
        if candidates[cur_ranking_rule_index].len() <= 1 {
            maybe_add_to_results!(&candidates[cur_ranking_rule_index]);
            candidates[cur_ranking_rule_index].clear();
            back!();
            continue;
        }

        let Some(next_bucket) = ranking_rules[cur_ranking_rule_index].next_bucket(index, txn, db_cache, logger, &candidates[cur_ranking_rule_index])? else {
            // TODO: add remaining candidates automatically here?
            back!();
            continue;
        };

        logger.next_bucket_ranking_rule(
            cur_ranking_rule_index,
            ranking_rules[cur_ranking_rule_index],
            &candidates[cur_ranking_rule_index],
            &next_bucket.candidates,
            Instant::now(),
        );

        assert!(candidates[cur_ranking_rule_index].is_superset(&next_bucket.candidates));
        candidates[cur_ranking_rule_index] -= &next_bucket.candidates;

        if cur_ranking_rule_index == ranking_rules_len - 1
            || next_bucket.candidates.len() <= 1
            || cur_offset + (next_bucket.candidates.len() as usize) < from
        {
            maybe_add_to_results!(&next_bucket.candidates);
            continue;
        }

        cur_ranking_rule_index += 1;
        candidates[cur_ranking_rule_index] = next_bucket.candidates.clone();
        logger.start_iteration_ranking_rule(
            cur_ranking_rule_index,
            ranking_rules[cur_ranking_rule_index],
            &next_bucket.query,
            &candidates[cur_ranking_rule_index],
            Instant::now(),
        );
        ranking_rules[cur_ranking_rule_index].start_iteration(
            index,
            txn,
            db_cache,
            logger,
            &next_bucket.candidates,
            &next_bucket.query,
        )?;
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::execute_search;
    // use crate::allocator::ALLOC;
    use crate::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
    use crate::index::tests::TempIndex;
    use crate::new::db_cache::DatabaseCache;
    use big_s::S;
    use heed::EnvOpenOptions;
    use maplit::hashset;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Cursor, Seek};
    use std::time::Instant;
    // use crate::new::logger::detailed::DetailedSearchLogger;
    use crate::new::logger::{DefaultSearchLogger, SearchLogger};
    use crate::new::make_query_graph;
    use crate::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
    use crate::{Criterion, Index, Object, Search, TermsMatchingStrategy};

    #[test]
    fn execute_new_search() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                {
                    "id": 7,
                    "text": "the super quick super brown fox jumps over",
                },
                {
                    "id": 8,
                    "text": "the super quick brown fox jumps over",
                },
                {
                    "id": 9,
                    "text": "the quick super brown fox jumps over",
                },
                {
                    "id": 10,
                    "text": "the quick brown fox jumps over",
                },
                {
                    "id": 11,
                    "text": "the quick brown fox jumps over the lazy dog",
                },
                {
                    "id": 12,
                    "text": "the quick brown cat jumps over the lazy dog",
                },
            ]))
            .unwrap();
        let txn = index.read_txn().unwrap();
        let mut logger = DefaultSearchLogger;
        let mut db_cache = DatabaseCache::default();

        let query_graph =
            make_query_graph(&index, &txn, &mut db_cache, "releases from poison by the government")
                .unwrap();
        logger.initial_query(&query_graph, Instant::now());

        let results =
            execute_search(&index, &txn, &mut db_cache, &query_graph, None, 0, 50, &mut logger)
                .unwrap();
        println!("{results:?}")
    }

    #[test]
    fn search_wiki_new() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_wiki").unwrap();
        let txn = index.read_txn().unwrap();

        println!("nbr docids: {}", index.documents_ids(&txn).unwrap().len());

        let primary_key = index.primary_key(&txn).unwrap().unwrap();
        let primary_key = index.fields_ids_map(&txn).unwrap().id(primary_key).unwrap();
        // loop {
        let start = Instant::now();

        let mut db_cache = DatabaseCache::default();

        let query_graph = make_query_graph(
            &index,
            &txn,
            &mut db_cache,
            "which a the releases from poison by the government",
        )
        .unwrap();

        // let mut logger = crate::new::logger::detailed::DetailedSearchLogger::new("log");

        let results = execute_search(
            &index,
            &txn,
            &mut db_cache,
            &query_graph,
            None,
            0,
            20,
            &mut DefaultSearchLogger,
            // &mut logger,
        )
        .unwrap();

        // logger.write_d2_description();

        let elapsed = start.elapsed();

        let ids = index
            .documents(&txn, results.iter().copied())
            .unwrap()
            .into_iter()
            .map(|x| {
                let obkv = &x.1;
                let id = obkv.get(primary_key).unwrap();
                let id: serde_json::Value = serde_json::from_slice(id).unwrap();
                id.as_str().unwrap().to_owned()
            })
            .collect::<Vec<_>>();

        println!("{}us: {results:?}", elapsed.as_micros());
        println!("external ids: {ids:?}");
        // println!("max_resident: {}", ALLOC.max_resident.load(std::sync::atomic::Ordering::SeqCst));
        // println!("allocated: {}", ALLOC.allocated.load(std::sync::atomic::Ordering::SeqCst));
        // }
    }

    #[test]
    fn search_wiki_old() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_wiki").unwrap();

        let txn = index.read_txn().unwrap();

        let rr = index.criteria(&txn).unwrap();
        println!("{rr:?}");

        let primary_key = index.primary_key(&txn).unwrap().unwrap();
        let primary_key = index.fields_ids_map(&txn).unwrap().id(primary_key).unwrap();

        let start = Instant::now();

        let mut s = Search::new(&txn, &index);
        s.query("releases from poison by the government");
        s.terms_matching_strategy(TermsMatchingStrategy::Last);
        s.criterion_implementation_strategy(crate::CriterionImplementationStrategy::OnlySetBased);
        let docs = s.execute().unwrap();

        let elapsed = start.elapsed();

        let ids = index
            .documents(&txn, docs.documents_ids.iter().copied())
            .unwrap()
            .into_iter()
            .map(|x| {
                let obkv = &x.1;
                let id = obkv.get(primary_key).unwrap();
                let id: serde_json::Value = serde_json::from_slice(id).unwrap();
                id.as_str().unwrap().to_owned()
            })
            .collect::<Vec<_>>();

        println!("{}us: {:?}", elapsed.as_micros(), docs.documents_ids);
        println!("external ids: {ids:?}");
    }
    #[test]
    fn search_movies_new() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_movies").unwrap();
        let txn = index.read_txn().unwrap();

        let primary_key = index.primary_key(&txn).unwrap().unwrap();
        let primary_key = index.fields_ids_map(&txn).unwrap().id(primary_key).unwrap();
        // loop {
        let start = Instant::now();

        let mut db_cache = DatabaseCache::default();

        let query_graph =
            make_query_graph(&index, &txn, &mut db_cache, "releases from poison by the government")
                .unwrap();

        let mut logger = crate::new::logger::detailed::DetailedSearchLogger::new("log");

        let results = execute_search(
            &index,
            &txn,
            &mut db_cache,
            &query_graph,
            None,
            0,
            20,
            // &mut DefaultSearchLogger,
            &mut logger,
        )
        .unwrap();

        logger.write_d2_description();

        let elapsed = start.elapsed();

        let ids = index
            .documents(&txn, results.iter().copied())
            .unwrap()
            .into_iter()
            .map(|x| {
                let obkv = &x.1;
                let id = obkv.get(primary_key).unwrap();
                let id: serde_json::Value = serde_json::from_slice(id).unwrap();
                id.as_str().unwrap().to_owned()
            })
            .collect::<Vec<_>>();

        println!("{}us: {results:?}", elapsed.as_micros());
        println!("external ids: {ids:?}");
        // }
    }

    #[test]
    fn search_movies_old() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_movies").unwrap();

        let txn = index.read_txn().unwrap();

        let rr = index.criteria(&txn).unwrap();
        println!("{rr:?}");

        let primary_key = index.primary_key(&txn).unwrap().unwrap();
        let primary_key = index.fields_ids_map(&txn).unwrap().id(primary_key).unwrap();

        let start = Instant::now();

        let mut s = Search::new(&txn, &index);
        s.query("releases from poison by the government");
        s.terms_matching_strategy(TermsMatchingStrategy::Last);
        s.criterion_implementation_strategy(crate::CriterionImplementationStrategy::OnlySetBased);
        let docs = s.execute().unwrap();

        let elapsed = start.elapsed();

        let ids = index
            .documents(&txn, docs.documents_ids.iter().copied())
            .unwrap()
            .into_iter()
            .map(|x| {
                let obkv = &x.1;
                let id = obkv.get(primary_key).unwrap();
                let id: serde_json::Value = serde_json::from_slice(id).unwrap();
                id.as_str().unwrap().to_owned()
            })
            .collect::<Vec<_>>();

        println!("{}us: {:?}", elapsed.as_micros(), docs.documents_ids);
        println!("external ids: {ids:?}");
    }

    #[test]
    fn _settings_movies() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_movies").unwrap();
        let mut wtxn = index.write_txn().unwrap();

        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);

        builder.set_min_word_len_one_typo(5);
        builder.set_min_word_len_two_typos(100);
        builder.set_sortable_fields(hashset! { S("release_date") });
        builder.set_criteria(vec![
            Criterion::Words,
            Criterion::Typo,
            Criterion::Proximity,
            Criterion::Asc("release_date".to_owned()),
        ]);

        builder.execute(|_| (), || false).unwrap();
        wtxn.commit().unwrap();
    }

    #[test]
    fn _index_movies() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_movies").unwrap();
        let mut wtxn = index.write_txn().unwrap();

        let primary_key = "id";
        let searchable_fields = vec!["title", "overview"];
        let filterable_fields = vec!["release_date", "genres"];

        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_primary_key(primary_key.to_owned());
        let searchable_fields = searchable_fields.iter().map(|s| s.to_string()).collect();
        builder.set_searchable_fields(searchable_fields);
        let filterable_fields = filterable_fields.iter().map(|s| s.to_string()).collect();
        builder.set_filterable_fields(filterable_fields);

        builder.set_min_word_len_one_typo(5);
        builder.set_min_word_len_two_typos(100);
        builder.set_criteria(vec![Criterion::Words, Criterion::Proximity]);
        builder.execute(|_| (), || false).unwrap();

        let config = IndexerConfig::default();
        let indexing_config = IndexDocumentsConfig::default();
        let builder =
            IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false)
                .unwrap();

        let documents = documents_from(
            "/Users/meilisearch/Documents/milli2/benchmarks/datasets/movies.json",
            "json",
        );
        let (builder, user_error) = builder.add_documents(documents).unwrap();
        user_error.unwrap();
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        index.prepare_for_closing().wait();
    }
    #[test]
    fn _index_wiki() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_wiki").unwrap();
        let mut wtxn = index.write_txn().unwrap();

        // let primary_key = "id";
        let searchable_fields = vec!["body", "title", "url"];
        // let filterable_fields = vec![];
        let config = IndexerConfig::default();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        // builder.set_primary_key(primary_key.to_owned());
        let searchable_fields = searchable_fields.iter().map(|s| s.to_string()).collect();
        builder.set_searchable_fields(searchable_fields);
        // let filterable_fields = filterable_fields.iter().map(|s| s.to_string()).collect();
        // builder.set_filterable_fields(filterable_fields);

        // builder.set_min_word_len_one_typo(5);
        // builder.set_min_word_len_two_typos(100);
        builder.set_criteria(vec![Criterion::Words, Criterion::Typo, Criterion::Proximity]);
        builder.execute(|_| (), || false).unwrap();

        let config = IndexerConfig::default();
        let indexing_config =
            IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
        let builder =
            IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| (), || false)
                .unwrap();

        let documents = documents_from(
            "/Users/meilisearch/Documents/milli2/benchmarks/datasets/smol-wiki-articles.csv",
            "csv",
        );
        let (builder, user_error) = builder.add_documents(documents).unwrap();
        user_error.unwrap();
        builder.execute().unwrap();
        wtxn.commit().unwrap();

        index.prepare_for_closing().wait();
    }

    fn documents_from(filename: &str, filetype: &str) -> DocumentsBatchReader<impl BufRead + Seek> {
        let reader = File::open(filename)
            .unwrap_or_else(|_| panic!("could not find the dataset in: {}", filename));
        let reader = BufReader::new(reader);
        let documents = match filetype {
            "csv" => documents_from_csv(reader).unwrap(),
            "json" => documents_from_json(reader).unwrap(),
            "jsonl" => documents_from_jsonl(reader).unwrap(),
            otherwise => panic!("invalid update format {:?}", otherwise),
        };
        DocumentsBatchReader::from_reader(Cursor::new(documents)).unwrap()
    }

    fn documents_from_jsonl(reader: impl BufRead) -> crate::Result<Vec<u8>> {
        let mut documents = DocumentsBatchBuilder::new(Vec::new());

        for result in serde_json::Deserializer::from_reader(reader).into_iter::<Object>() {
            let object = result.unwrap();
            documents.append_json_object(&object)?;
        }

        documents.into_inner().map_err(Into::into)
    }

    fn documents_from_json(reader: impl BufRead) -> crate::Result<Vec<u8>> {
        let mut documents = DocumentsBatchBuilder::new(Vec::new());

        documents.append_json_array(reader)?;

        documents.into_inner().map_err(Into::into)
    }

    fn documents_from_csv(reader: impl BufRead) -> crate::Result<Vec<u8>> {
        let csv = csv::Reader::from_reader(reader);

        let mut documents = DocumentsBatchBuilder::new(Vec::new());
        documents.append_csv(csv)?;

        documents.into_inner().map_err(Into::into)
    }
}
