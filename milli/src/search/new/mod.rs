mod db_cache;
mod distinct;
mod graph_based_ranking_rule;
mod interner;
mod logger;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
// TODO: documentation + comments
mod small_bitmap;
// TODO: documentation + comments
mod sort;
// TODO: documentation + comments
mod words;

// #[cfg(test)]
pub use logger::detailed::DetailedSearchLogger;
pub use logger::{DefaultSearchLogger, SearchLogger};

use std::collections::{BTreeSet, HashSet};

use crate::{Filter, Index, MatchingWords, Result, Search, SearchResult, TermsMatchingStrategy};
use charabia::Tokenize;
use db_cache::DatabaseCache;
use graph_based_ranking_rule::{Proximity, Typo};
use heed::RoTxn;
use interner::DedupInterner;
use query_graph::{QueryGraph, QueryNode, QueryNodeData};
use query_term::{located_query_terms_from_string, Phrase, QueryTerm};
use ranking_rules::{bucket_sort, PlaceholderQuery, RankingRuleOutput, RankingRuleQueryTrait};
use resolve_query_graph::{resolve_query_graph, QueryTermDocIdsCache};
use roaring::RoaringBitmap;
use words::Words;

use self::ranking_rules::RankingRule;

/// A structure used throughout the execution of a search query.
pub struct SearchContext<'ctx> {
    pub index: &'ctx Index,
    pub txn: &'ctx RoTxn<'ctx>,
    pub db_cache: DatabaseCache<'ctx>,
    pub word_interner: DedupInterner<String>,
    pub phrase_interner: DedupInterner<Phrase>,
    pub term_interner: DedupInterner<QueryTerm>,
    pub term_docids: QueryTermDocIdsCache,
}
impl<'ctx> SearchContext<'ctx> {
    pub fn new(index: &'ctx Index, txn: &'ctx RoTxn<'ctx>) -> Self {
        Self {
            index,
            txn,
            db_cache: <_>::default(),
            word_interner: <_>::default(),
            phrase_interner: <_>::default(),
            term_interner: <_>::default(),
            term_docids: <_>::default(),
        }
    }
}

/// Apply the [`TermsMatchingStrategy`] to the query graph and resolve it.
#[allow(clippy::too_many_arguments)]
fn resolve_maximally_reduced_query_graph<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    let mut graph = query_graph.clone();
    let mut positions_to_remove = match matching_strategy {
        TermsMatchingStrategy::Last => {
            let mut all_positions = BTreeSet::new();
            for (_, n) in query_graph.nodes.iter() {
                match &n.data {
                    QueryNodeData::Term(term) => {
                        all_positions.extend(term.positions.clone().into_iter());
                    }
                    QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => {}
                }
            }
            all_positions.into_iter().collect()
        }
        TermsMatchingStrategy::All => vec![],
    };
    // don't remove the first term
    if !positions_to_remove.is_empty() {
        positions_to_remove.remove(0);
    }
    loop {
        if positions_to_remove.is_empty() {
            break;
        } else {
            let position_to_remove = positions_to_remove.pop().unwrap();
            let _ = graph.remove_words_starting_at_position(position_to_remove);
        }
    }
    logger.query_for_universe(&graph);
    let docids = resolve_query_graph(ctx, &graph, universe)?;

    Ok(docids)
}

/// Return the list of initialised ranking rules to be used for a placeholder search.
fn get_ranking_rules_for_placeholder_search<'ctx>(
    ctx: &SearchContext<'ctx>,
) -> Result<Vec<Box<dyn RankingRule<'ctx, PlaceholderQuery>>>> {
    // let sort = false;
    // let mut asc = HashSet::new();
    // let mut desc = HashSet::new();
    let /*mut*/ ranking_rules: Vec<Box<dyn RankingRule<PlaceholderQuery>>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        // Add Words before any of: typo, proximity, attribute, exactness
        match rr {
            crate::Criterion::Words
            | crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
            | crate::Criterion::Exactness => continue,
            crate::Criterion::Sort => todo!(),
            crate::Criterion::Asc(_) => todo!(),
            crate::Criterion::Desc(_) => todo!(),
        }
    }
    Ok(ranking_rules)
}

/// Return the list of initialised ranking rules to be used for a query graph search.
fn get_ranking_rules_for_query_graph_search<'ctx>(
    ctx: &SearchContext<'ctx>,
    terms_matching_strategy: TermsMatchingStrategy,
) -> Result<Vec<Box<dyn RankingRule<'ctx, QueryGraph>>>> {
    // query graph search
    let mut words = false;
    let mut typo = false;
    let mut proximity = false;
    let sort = false;
    let attribute = false;
    let exactness = false;
    let mut asc = HashSet::new();
    let mut desc = HashSet::new();

    let mut ranking_rules: Vec<Box<dyn RankingRule<QueryGraph>>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        // Add Words before any of: typo, proximity, attribute, exactness
        match rr {
            crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
            | crate::Criterion::Exactness => {
                if !words {
                    ranking_rules.push(Box::new(Words::new(terms_matching_strategy)));
                    words = true;
                }
            }
            _ => {}
        }
        match rr {
            crate::Criterion::Words => {
                if words {
                    continue;
                }
                ranking_rules.push(Box::new(Words::new(terms_matching_strategy)));
                words = true;
            }
            crate::Criterion::Typo => {
                if typo {
                    continue;
                }
                typo = true;
                ranking_rules.push(Box::<Typo>::default());
            }
            crate::Criterion::Proximity => {
                if proximity {
                    continue;
                }
                proximity = true;
                ranking_rules.push(Box::<Proximity>::default());
            }
            crate::Criterion::Attribute => {
                if attribute {
                    continue;
                }
                todo!();
                // attribute = false;
            }
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                todo!();
                // sort = false;
            }
            crate::Criterion::Exactness => {
                if exactness {
                    continue;
                }
                todo!();
                // exactness = false;
            }
            crate::Criterion::Asc(field) => {
                if asc.contains(&field) {
                    continue;
                }
                asc.insert(field);
                // TODO
            }
            crate::Criterion::Desc(field) => {
                if desc.contains(&field) {
                    continue;
                }
                desc.insert(field);
                todo!();
            }
        }
    }
    Ok(ranking_rules)
}

#[allow(clippy::too_many_arguments)]
pub fn execute_search<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    query: &str,
    terms_matching_strategy: TermsMatchingStrategy,
    filters: Option<Filter>,
    from: usize,
    length: usize,
    placeholder_search_logger: &mut dyn SearchLogger<PlaceholderQuery>,
    query_graph_logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<SearchResult> {
    assert!(!query.is_empty());
    let query_terms = located_query_terms_from_string(ctx, query.tokenize(), None)?;
    let graph = QueryGraph::from_query(ctx, query_terms)?;

    let mut universe = if let Some(filters) = filters {
        filters.evaluate(ctx.txn, ctx.index)?
    } else {
        ctx.index.documents_ids(ctx.txn)?
    };

    // TODO: other way to tell whether it is a placeholder search
    // This way of doing things is not correct because if someone searches
    // for a word that does not appear in any document, the word will be removed
    // from the graph and thus its number of nodes will be == 2
    // But in that case, we should return no results.
    //
    // The search is a placeholder search only if there are no tokens?
    let documents_ids = if graph.nodes.len() > 2 {
        universe = resolve_maximally_reduced_query_graph(
            ctx,
            &universe,
            &graph,
            terms_matching_strategy,
            query_graph_logger,
        )?;

        let ranking_rules = get_ranking_rules_for_query_graph_search(ctx, terms_matching_strategy)?;
        bucket_sort(ctx, ranking_rules, &graph, &universe, from, length, query_graph_logger)?
    } else {
        let ranking_rules = get_ranking_rules_for_placeholder_search(ctx)?;
        bucket_sort(
            ctx,
            ranking_rules,
            &PlaceholderQuery,
            &universe,
            from,
            length,
            placeholder_search_logger,
        )?
    };

    Ok(SearchResult {
        // TODO: correct matching words
        matching_words: MatchingWords::default(),
        // TODO: candidates with distinct
        candidates: universe,
        documents_ids,
    })
}

impl<'a> Search<'a> {
    // TODO
    pub fn execute_new(&self) -> Result<SearchResult> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    // use crate::allocator::ALLOC;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Cursor, Seek};
    use std::time::Instant;

    use big_s::S;
    use heed::EnvOpenOptions;
    use maplit::hashset;

    use crate::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
    // use crate::search::new::logger::detailed::DetailedSearchLogger;
    use crate::search::new::logger::DefaultSearchLogger;
    use crate::search::new::{execute_search, SearchContext};
    use crate::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
    use crate::{Criterion, Index, Object, Search, TermsMatchingStrategy};

    #[test]
    fn search_wiki_new() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_wiki").unwrap();
        let txn = index.read_txn().unwrap();

        println!("nbr docids: {}", index.documents_ids(&txn).unwrap().len());

        loop {
            let start = Instant::now();

            // let mut logger = crate::search::new::logger::detailed::DetailedSearchLogger::new("log");
            let mut ctx = SearchContext::new(&index, &txn);
            let results = execute_search(
                &mut ctx,
                "released from prison by the government",
                // "which a the releases from poison by the government",
                // "sun flower s are the best",
                // "zero config",
                TermsMatchingStrategy::Last,
                None,
                0,
                20,
                &mut DefaultSearchLogger,
                &mut DefaultSearchLogger,
                // &mut logger,
            )
            .unwrap();

            // logger.write_d2_description(&mut ctx);

            let elapsed = start.elapsed();
            println!("{}us", elapsed.as_micros());

            let _documents = index
                .documents(&txn, results.documents_ids.iter().copied())
                .unwrap()
                .into_iter()
                .map(|(id, obkv)| {
                    let mut object = serde_json::Map::default();
                    for (fid, fid_name) in index.fields_ids_map(&txn).unwrap().iter() {
                        let value = obkv.get(fid).unwrap();
                        let value: serde_json::Value = serde_json::from_slice(value).unwrap();
                        object.insert(fid_name.to_owned(), value);
                    }
                    (id, serde_json::to_string_pretty(&object).unwrap())
                })
                .collect::<Vec<_>>();

            println!("{}us: {:?}", elapsed.as_micros(), results);
        }
        // for (id, document) in documents {
        //     println!("{id}:");
        //     // println!("{document}");
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

        let start = Instant::now();

        let mut s = Search::new(&txn, &index);
        s.query(
            // "which a the releases from poison by the government",
            // "sun flower s are the best",
            "zero config",
        );
        s.terms_matching_strategy(TermsMatchingStrategy::Last);
        // s.criterion_implementation_strategy(crate::CriterionImplementationStrategy::OnlyIterative);
        let docs = s.execute().unwrap();

        let elapsed = start.elapsed();

        let documents = index
            .documents(&txn, docs.documents_ids.iter().copied())
            .unwrap()
            .into_iter()
            .map(|(id, obkv)| {
                let mut object = serde_json::Map::default();
                for (fid, fid_name) in index.fields_ids_map(&txn).unwrap().iter() {
                    let value = obkv.get(fid).unwrap();
                    let value: serde_json::Value = serde_json::from_slice(value).unwrap();
                    object.insert(fid_name.to_owned(), value);
                }
                (id, serde_json::to_string_pretty(&object).unwrap())
            })
            .collect::<Vec<_>>();

        println!("{}us: {:?}", elapsed.as_micros(), docs.documents_ids);
        for (id, _document) in documents {
            println!("{id}:");
            // println!("{document}");
        }
    }
    #[test]
    fn search_movies_new() {
        let mut options = EnvOpenOptions::new();
        options.map_size(100 * 1024 * 1024 * 1024); // 100 GB

        let index = Index::new(options, "data_movies").unwrap();
        let txn = index.read_txn().unwrap();

        // let primary_key = index.primary_key(&txn).unwrap().unwrap();
        // let primary_key = index.fields_ids_map(&txn).unwrap().id(primary_key).unwrap();
        // loop {
        let start = Instant::now();

        let mut logger = crate::search::new::logger::detailed::DetailedSearchLogger::new("log");
        let mut ctx = SearchContext::new(&index, &txn);
        let results = execute_search(
            &mut ctx,
            "releases from poison by the government",
            TermsMatchingStrategy::Last,
            None,
            0,
            20,
            &mut DefaultSearchLogger,
            &mut logger,
        )
        .unwrap();

        logger.write_d2_description(&mut ctx);

        let elapsed = start.elapsed();

        // let ids = index
        //     .documents(&txn, results.iter().copied())
        //     .unwrap()
        //     .into_iter()
        //     .map(|x| {
        //         let obkv = &x.1;
        //         let id = obkv.get(primary_key).unwrap();
        //         let id: serde_json::Value = serde_json::from_slice(id).unwrap();
        //         id.as_str().unwrap().to_owned()
        //     })
        //     .collect::<Vec<_>>();

        println!("{}us: {results:?}", elapsed.as_micros());
        // println!("external ids: {ids:?}");
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
        s.query("which a the releases from poison by the government");
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
