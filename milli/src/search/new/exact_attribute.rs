use heed::BytesDecode;
use roaring::MultiOps;

use super::query_graph::QueryGraph;
use super::ranking_rules::{RankingRule, RankingRuleOutput};
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::ExactTerm;
use crate::{CboRoaringBitmapCodec, Result, SearchContext, SearchLogger};

/// FIXME:
///
/// - A lot of work done in next_bucket that start_iteration could do.
/// - Consider calling the graph based rule directly from this one.
/// - currently we did exact term, don't forget about prefix
/// - some tests
pub struct ExactAttribute {
    query_graph: Option<QueryGraph>,
}

impl ExactAttribute {
    pub fn new() -> Self {
        Self { query_graph: None }
    }
}

impl<'ctx> RankingRule<'ctx, QueryGraph> for ExactAttribute {
    fn id(&self) -> String {
        "exact_attribute".to_owned()
    }

    fn start_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        _universe: &roaring::RoaringBitmap,
        query: &QueryGraph,
    ) -> Result<()> {
        self.query_graph = Some(query.clone());
        Ok(())
    }

    fn next_bucket(
        &mut self,
        ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
        universe: &roaring::RoaringBitmap,
    ) -> Result<Option<RankingRuleOutput<QueryGraph>>> {
        // iterate on the nodes of the graph, retain LocatedQueryTermSubset
        let query_graph = self.query_graph.as_ref().unwrap();
        let mut exact_term_position_ids: Vec<(ExactTerm, u16, u8)> =
            Vec::with_capacity(query_graph.nodes.len() as usize);
        for (_, node) in query_graph.nodes.iter() {
            match &node.data {
                QueryNodeData::Term(term) => {
                    let exact_term = if let Some(exact_term) = term.term_subset.exact_term(ctx) {
                        exact_term
                    } else {
                        // FIXME: Use `None` or some function indicating that we're passing down the bucket to our child rules
                        return Ok(Some(RankingRuleOutput {
                            query: query_graph.clone(),
                            candidates: universe.clone(),
                        }));
                    };
                    exact_term_position_ids.push((
                        exact_term,
                        *term.positions.start(),
                        *term.term_ids.start(),
                    ))
                }
                QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => continue,
            }
        }

        exact_term_position_ids.sort_by_key(|(_, _, id)| *id);
        // bail if there is a "hole" (missing word) in remaining query graph
        let mut previous_id = 0;
        for (_, _, id) in exact_term_position_ids.iter().copied() {
            if id < previous_id || id - previous_id > 1 {
                // FIXME: Use `None` or some function indicating that we're passing down the bucket to our child rules
                return Ok(Some(RankingRuleOutput {
                    query: query_graph.clone(),
                    candidates: universe.clone(),
                }));
            } else {
                previous_id = id;
            }
        }

        // sample query: "sunflower are pretty"
        // sunflower at pos 0 in attr A
        // are at pos 1 in attr B
        // pretty at pos 2 in attr C
        // We want to eliminate such document

        // first check that for each term, there exists some attribute that has this term at the correct position
        //"word-position-docids";
        let mut candidates = universe.clone();
        let words_positions: Vec<(Vec<_>, _)> = exact_term_position_ids
            .iter()
            .copied()
            .map(|(term, position, _)| (term.interned_words(ctx).collect(), position))
            .collect();
        for (words, position) in &words_positions {
            if candidates.is_empty() {
                // FIXME: Use `None` or some function indicating that we're passing down the bucket to our child rules
                return Ok(Some(RankingRuleOutput {
                    query: query_graph.clone(),
                    candidates: universe.clone(),
                }));
            }

            'words: for (offset, word) in words.iter().enumerate() {
                let offset = offset as u16;
                let word = if let Some(word) = word {
                    word
                } else {
                    continue 'words;
                };
                let word_position_docids = CboRoaringBitmapCodec::bytes_decode(
                    ctx.get_db_word_position_docids(*word, position + offset)?.unwrap_or_default(),
                )
                .unwrap_or_default();
                candidates &= word_position_docids;
            }
        }

        let candidates = candidates;

        if candidates.is_empty() {
            // FIXME: Use `None` or some function indicating that we're passing down the bucket to our child rules
            return Ok(Some(RankingRuleOutput {
                query: query_graph.clone(),
                candidates: universe.clone(),
            }));
        }

        let searchable_fields_ids = ctx.index.searchable_fields_ids(ctx.txn)?.unwrap_or_default();

        let mut candidates_per_attributes = Vec::with_capacity(searchable_fields_ids.len());

        // then check that there exists at least one attribute that has all of the terms
        for fid in searchable_fields_ids {
            let mut intersection = MultiOps::intersection(
                words_positions
                    .iter()
                    .flat_map(|(words, ..)| words.iter())
                    // ignore stop words words in phrases
                    .flatten()
                    .map(|word| -> Result<_> {
                        Ok(ctx
                            .get_db_word_fid_docids(*word, fid)?
                            .map(CboRoaringBitmapCodec::bytes_decode)
                            .unwrap_or_default()
                            .unwrap_or_default())
                    }),
            )?;
            intersection &= &candidates;
            if !intersection.is_empty() {
                candidates_per_attributes.push(intersection);
            }
        }
        // note we could have "false positives" where there both exist different attributes that collectively
        // have the terms in the correct order and a single attribute that have all the terms, but in the incorrect order.

        let candidates = MultiOps::union(candidates_per_attributes.into_iter());
        Ok(Some(RankingRuleOutput { query: query_graph.clone(), candidates }))
    }

    fn end_iteration(
        &mut self,
        _ctx: &mut SearchContext<'ctx>,
        _logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
    }
}
