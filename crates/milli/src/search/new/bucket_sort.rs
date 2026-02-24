use roaring::RoaringBitmap;

use super::logger::SearchLogger;
use super::ranking_rules::{BoxRankingRule, RankingRuleQueryTrait};
use super::SearchContext;
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::search::new::distinct::{
    apply_distinct_rule, distinct_fid, distinct_single_docid, DistinctOutput,
};
use crate::{Deadline, Result};

pub struct BucketSortOutput {
    pub docids: Vec<u32>,
    pub scores: Vec<Vec<ScoreDetails>>,
    pub all_candidates: RoaringBitmap,

    pub degraded: bool,
}

// TODO: would probably be good to regroup some of these inside of a struct?
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "trace", skip_all, target = "search::bucket_sort")]
pub fn bucket_sort<'ctx, Q: RankingRuleQueryTrait>(
    ctx: &mut SearchContext<'ctx>,
    mut ranking_rules: Vec<BoxRankingRule<'ctx, Q>>,
    query: &Q,
    distinct: Option<&str>,
    universe: &RoaringBitmap,
    from: usize,
    length: usize,
    scoring_strategy: ScoringStrategy,
    logger: &mut dyn SearchLogger<Q>,
    deadline: Deadline,
    ranking_score_threshold: Option<f64>,
    exhaustive_number_hits: bool,
    max_total_hits: Option<usize>,
    pins: &[(u32, u32)],
) -> Result<BucketSortOutput> {
    logger.initial_query(query);
    logger.ranking_rules(&ranking_rules);
    logger.initial_universe(universe);

    let distinct_fid = distinct_fid(distinct, ctx.index, ctx.txn)?;

    // Pinned documents are excluded from the ranking process so they don't appear
    // in the normal candidate set. They will be injected at their exact target
    // positions by `inject_pins` at the end.
    let mut pinned_bitmap = RoaringBitmap::new();
    for &(_, docid) in pins {
        pinned_bitmap.insert(docid);
    }
    let adjusted_universe = universe - &pinned_bitmap;

    // Adjust pagination to reserve slots for pinned documents. The final result list interleaves
    // ranked docs and pinned docs. To get the right ranked docs for the current page, we adjust
    // `from` and `length`:
    //
    //   - Each pin before the page inserts a slot into the combined list, pushing every ranked
    //     doc after its one position to the right. So the ranked doc that would have been at
    //     position `from` is now at `from + pins_before`, and we need to start fetching ranked
    //     docs earlier: `ranked_from = from - pins_before`.
    //
    //   - Each pin on the page takes one of the `length` slots, leaving fewer slots for ranked
    //     docs: `ranked_length = length - pins_on_page`.
    //
    // Example: from=10, length=10, pin at position 5
    //   The pin at 5 pushes ranked[5..] one slot right, so the page [10,20) now starts at
    //   ranked[9] instead of ranked[10].
    //   pins_before=1, pins_on_page=0 means ranked_from=9, ranked_length=10
    //
    // Example: from=0, length=10, pins at positions 2 and 7
    //   No pins before the page, but 2 slots on the page are taken by pins,
    //   so we only need 8 ranked docs to fill the remaining slots.
    //   pins_before=0, pins_on_page=2 means ranked_from=0, ranked_length=8
    let pins_before = pins.iter().filter(|&&(pos, _)| (pos as usize) < from).count();
    let pins_on_page = pins
        .iter()
        .filter(|&&(pos, _)| {
            let pos = pos as usize;
            pos >= from && pos < from + length
        })
        .count();
    let ranked_from = from.saturating_sub(pins_before);
    let ranked_length = length.saturating_sub(pins_on_page);

    if adjusted_universe.len() < ranked_from as u64 {
        return Ok(inject_pins(
            pins,
            from,
            length,
            &pinned_bitmap,
            BucketSortOutput {
                docids: vec![],
                scores: vec![],
                all_candidates: adjusted_universe,
                degraded: false,
            },
        ));
    }
    if ranking_rules.is_empty() {
        if let Some(distinct_fid) = distinct_fid {
            let mut excluded = RoaringBitmap::new();
            let mut results = vec![];
            for docid in adjusted_universe.iter() {
                if results.len() >= ranked_from + ranked_length {
                    break;
                }
                if excluded.contains(docid) {
                    continue;
                }

                distinct_single_docid(ctx.index, ctx.txn, distinct_fid, docid, &mut excluded)?;
                results.push(docid);
            }

            let mut all_candidates = &adjusted_universe - &excluded;
            all_candidates.extend(results.iter().copied());
            // drain the results of the skipped elements
            // this **must** be done **after** writing the entire results in `all_candidates` to ensure
            // e.g. estimatedTotalHits is correct.
            if results.len() >= ranked_from {
                results.drain(..ranked_from);
            } else {
                results.clear();
            }

            return Ok(inject_pins(
                pins,
                from,
                length,
                &pinned_bitmap,
                BucketSortOutput {
                    scores: vec![Default::default(); results.len()],
                    docids: results,
                    all_candidates,
                    degraded: false,
                },
            ));
        } else {
            let docids: Vec<u32> =
                adjusted_universe.iter().skip(ranked_from).take(ranked_length).collect();
            return Ok(inject_pins(
                pins,
                from,
                length,
                &pinned_bitmap,
                BucketSortOutput {
                    scores: vec![Default::default(); docids.len()],
                    docids,
                    all_candidates: adjusted_universe.clone(),
                    degraded: false,
                },
            ));
        };
    }

    let ranking_rules_len = ranking_rules.len();

    logger.start_iteration_ranking_rule(0, ranking_rules[0].as_ref(), query, &adjusted_universe);

    ranking_rules[0].start_iteration(ctx, logger, &adjusted_universe, query, &deadline)?;

    let mut ranking_rule_scores: Vec<ScoreDetails> = vec![];

    let mut ranking_rule_universes: Vec<RoaringBitmap> =
        vec![RoaringBitmap::default(); ranking_rules_len];
    ranking_rule_universes[0].clone_from(&adjusted_universe);
    let mut cur_ranking_rule_index = 0;

    /// Finish iterating over the current ranking rule, yielding
    /// control to the parent (or finishing the search if not possible).
    /// Update the universes accordingly and inform the logger.
    macro_rules! back {
        () => {
            // FIXME: temporarily disabled assert: see <https://github.com/meilisearch/meilisearch/pull/4013>
            // assert!(
            //     ranking_rule_universes[cur_ranking_rule_index].is_empty(),
            //     "The ranking rule {} did not sort its bucket exhaustively",
            //     ranking_rules[cur_ranking_rule_index].id()
            // );
            logger.end_iteration_ranking_rule(
                cur_ranking_rule_index,
                ranking_rules[cur_ranking_rule_index].as_ref(),
                &ranking_rule_universes[cur_ranking_rule_index],
            );
            ranking_rule_universes[cur_ranking_rule_index].clear();
            ranking_rules[cur_ranking_rule_index].end_iteration(ctx, logger);
            if cur_ranking_rule_index == 0 {
                break;
            } else {
                cur_ranking_rule_index -= 1;
            }
            if ranking_rule_scores.len() > cur_ranking_rule_index {
                ranking_rule_scores.pop();
            }
        };
    }

    let mut all_candidates = adjusted_universe.clone();
    let mut valid_docids = vec![];
    let mut valid_scores = vec![];
    let mut cur_offset = 0usize;

    macro_rules! maybe_add_to_results {
        ($candidates:expr) => {
            maybe_add_to_results(
                ctx,
                ranked_from,
                ranked_length,
                logger,
                &mut valid_docids,
                &mut valid_scores,
                &mut all_candidates,
                &mut ranking_rule_universes,
                &mut ranking_rules,
                cur_ranking_rule_index,
                &mut cur_offset,
                distinct_fid,
                &ranking_rule_scores,
                $candidates,
            )?;
        };
    }

    let max_len_to_evaluate =
        match (max_total_hits, exhaustive_number_hits && ranking_score_threshold.is_some()) {
            (Some(max_total_hits), true) => max_total_hits,
            _ => ranked_length,
        };

    while valid_docids.len() < max_len_to_evaluate {
        // The universe for this bucket is zero, so we don't need to sort
        // anything, just go back to the parent ranking rule.
        if ranking_rule_universes[cur_ranking_rule_index].is_empty()
            || (scoring_strategy == ScoringStrategy::Skip
                && ranking_rule_universes[cur_ranking_rule_index].len() == 1)
        {
            let bucket = std::mem::take(&mut ranking_rule_universes[cur_ranking_rule_index]);
            maybe_add_to_results!(bucket);
            back!();
            continue;
        }

        let next_bucket = if deadline.exceeded() {
            loop {
                match ranking_rules[cur_ranking_rule_index].non_blocking_next_bucket(
                    ctx,
                    logger,
                    &ranking_rule_universes[cur_ranking_rule_index],
                )? {
                    std::task::Poll::Ready(bucket) => break bucket,
                    std::task::Poll::Pending => {
                        let bucket =
                            std::mem::take(&mut ranking_rule_universes[cur_ranking_rule_index]);
                        ranking_rule_scores.push(ScoreDetails::Skipped);

                        // remove candidates from the universe without adding them to result if their score is below the threshold
                        let is_below_threshold =
                            ranking_score_threshold.is_some_and(|ranking_score_threshold| {
                                let current_score =
                                    ScoreDetails::global_score(ranking_rule_scores.iter());
                                current_score < ranking_score_threshold
                            });

                        if is_below_threshold {
                            all_candidates -= &bucket;
                            all_candidates -= &ranking_rule_universes[cur_ranking_rule_index];
                        } else {
                            maybe_add_to_results!(bucket);
                        }

                        ranking_rule_scores.pop();

                        if cur_ranking_rule_index == 0 {
                            return Ok(inject_pins(
                                pins,
                                from,
                                length,
                                &pinned_bitmap,
                                BucketSortOutput {
                                    scores: valid_scores,
                                    docids: valid_docids,
                                    all_candidates,
                                    degraded: true,
                                },
                            ));
                        }

                        // This is a copy/paste/adapted of the ugly back!() macro
                        logger.end_iteration_ranking_rule(
                            cur_ranking_rule_index,
                            ranking_rules[cur_ranking_rule_index].as_ref(),
                            &ranking_rule_universes[cur_ranking_rule_index],
                        );
                        ranking_rule_universes[cur_ranking_rule_index].clear();
                        ranking_rules[cur_ranking_rule_index].end_iteration(ctx, logger);
                        cur_ranking_rule_index -= 1;
                        if ranking_rule_scores.len() > cur_ranking_rule_index {
                            ranking_rule_scores.pop();
                        }
                    }
                }
            }
        } else {
            let Some(next_bucket) = ranking_rules[cur_ranking_rule_index].next_bucket(
                ctx,
                logger,
                &ranking_rule_universes[cur_ranking_rule_index],
                &deadline,
            )?
            else {
                back!();
                continue;
            };
            next_bucket
        };

        ranking_rule_scores.push(next_bucket.score);

        logger.next_bucket_ranking_rule(
            cur_ranking_rule_index,
            ranking_rules[cur_ranking_rule_index].as_ref(),
            &ranking_rule_universes[cur_ranking_rule_index],
            &next_bucket.candidates,
        );

        debug_assert!(
            ranking_rule_universes[cur_ranking_rule_index].is_superset(&next_bucket.candidates)
        );

        // remove candidates from the universe without adding them to result if their score is below the threshold
        let is_below_threshold = ranking_score_threshold.is_some_and(|ranking_score_threshold| {
            let current_score = ScoreDetails::global_score(ranking_rule_scores.iter());
            current_score < ranking_score_threshold
        });

        ranking_rule_universes[cur_ranking_rule_index] -= &next_bucket.candidates;

        if cur_ranking_rule_index == ranking_rules_len - 1
            || (scoring_strategy == ScoringStrategy::Skip && next_bucket.candidates.len() <= 1)
            || cur_offset + (next_bucket.candidates.len() as usize) < ranked_from
            || is_below_threshold
        {
            if is_below_threshold {
                all_candidates -= &next_bucket.candidates;
                all_candidates -= &ranking_rule_universes[cur_ranking_rule_index];
            } else {
                maybe_add_to_results!(next_bucket.candidates);
            }
            ranking_rule_scores.pop();
            continue;
        }

        cur_ranking_rule_index += 1;
        ranking_rule_universes[cur_ranking_rule_index].clone_from(&next_bucket.candidates);
        logger.start_iteration_ranking_rule(
            cur_ranking_rule_index,
            ranking_rules[cur_ranking_rule_index].as_ref(),
            &next_bucket.query,
            &ranking_rule_universes[cur_ranking_rule_index],
        );
        ranking_rules[cur_ranking_rule_index].start_iteration(
            ctx,
            logger,
            &next_bucket.candidates,
            &next_bucket.query,
            &deadline,
        )?;
    }

    Ok(inject_pins(
        pins,
        from,
        length,
        &pinned_bitmap,
        BucketSortOutput {
            docids: valid_docids,
            scores: valid_scores,
            all_candidates,
            degraded: false,
        },
    ))
}

/// Inject pinned documents into the ranked output. For each pin whose absolute position falls on the
/// current page [from, from+length), insert the document at the corresponding page-relative offset.
/// Truncate to `length` in case the insertions "push" over and add pinned docids back into
/// `all_candidates` so that `estimatedTotalHits` accounts for them.
fn inject_pins(
    pins: &[(u32, u32)],
    from: usize,
    length: usize,
    pinned_bitmap: &RoaringBitmap,
    mut output: BucketSortOutput,
) -> BucketSortOutput {
    if pins.is_empty() {
        return output;
    }

    for &(pos, doc_id) in pins {
        let pos = pos as usize;
        if pos >= from && pos < from + length {
            let insert_at = (pos - from).min(output.docids.len());
            output.docids.insert(insert_at, doc_id);
            output.scores.insert(insert_at, vec![]);
        }
    }

    output.docids.truncate(length);
    output.scores.truncate(length);
    output.all_candidates |= pinned_bitmap;

    output
}

/// Add the candidates to the results. Take `distinct`, `from`, `length`, and `cur_offset`
/// into account and inform the logger.
#[allow(clippy::too_many_arguments)]
fn maybe_add_to_results<'ctx, Q: RankingRuleQueryTrait>(
    ctx: &mut SearchContext<'ctx>,
    from: usize,
    length: usize,
    logger: &mut dyn SearchLogger<Q>,

    valid_docids: &mut Vec<u32>,
    valid_scores: &mut Vec<Vec<ScoreDetails>>,
    all_candidates: &mut RoaringBitmap,

    ranking_rule_universes: &mut [RoaringBitmap],
    ranking_rules: &mut [BoxRankingRule<'ctx, Q>],

    cur_ranking_rule_index: usize,

    cur_offset: &mut usize,

    distinct_fid: Option<u16>,
    ranking_rule_scores: &[ScoreDetails],
    candidates: RoaringBitmap,
) -> Result<()> {
    // First apply the distinct rule on the candidates, reducing the universes if necessary
    let candidates = if let Some(distinct_fid) = distinct_fid {
        let DistinctOutput { remaining, excluded } =
            apply_distinct_rule(ctx, distinct_fid, &candidates)?;
        for universe in ranking_rule_universes.iter_mut() {
            *universe -= &excluded;
            *all_candidates -= &excluded;
        }
        remaining
    } else {
        candidates.clone()
    };

    *all_candidates |= &candidates;

    // if the candidates are empty, there is nothing to do;
    if candidates.is_empty() {
        return Ok(());
    }

    // if we still haven't reached the first document to return
    if *cur_offset < from {
        // and if no document from this bucket can be returned
        if *cur_offset + (candidates.len() as usize) < from {
            // then just skip the bucket
            logger.skip_bucket_ranking_rule(
                cur_ranking_rule_index,
                ranking_rules[cur_ranking_rule_index].as_ref(),
                &candidates,
            );
        } else {
            // otherwise, skip some of the documents and add some of the rest, in order of ids
            let candidates_vec = candidates.iter().collect::<Vec<_>>();
            let (skipped_candidates, candidates) = candidates_vec.split_at(from - *cur_offset);

            logger.skip_bucket_ranking_rule(
                cur_ranking_rule_index,
                ranking_rules[cur_ranking_rule_index].as_ref(),
                &skipped_candidates.iter().collect(),
            );
            let candidates =
                candidates.iter().take(length - valid_docids.len()).copied().collect::<Vec<_>>();
            logger.add_to_results(&candidates);
            valid_docids.extend_from_slice(&candidates);
            valid_scores
                .extend(std::iter::repeat_n(ranking_rule_scores.to_owned(), candidates.len()));
        }
    } else {
        // if we have passed the offset already, add some of the documents (up to the limit)
        let candidates = candidates.iter().take(length - valid_docids.len()).collect::<Vec<u32>>();
        logger.add_to_results(&candidates);
        valid_docids.extend_from_slice(&candidates);
        valid_scores.extend(std::iter::repeat_n(ranking_rule_scores.to_owned(), candidates.len()));
    }

    *cur_offset += candidates.len() as usize;
    Ok(())
}
