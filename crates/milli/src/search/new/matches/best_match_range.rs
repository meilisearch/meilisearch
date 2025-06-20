use std::cell::Cell;

use crate::search::new::matches::matching_words::QueryPosition;

use super::r#match::{Match, MatchPosition};

struct MatchesIndexRangeWithScore {
    matches_index_range: [usize; 2],
    score: [i16; 3],
}

/// Compute the score of a match interval:
/// 1) count unique matches
/// 2) calculate distance between matches
/// 3) count ordered matches
fn get_score(
    matches: &[Match],
    query_positions: &[QueryPosition],
    index_first: usize,
    index_last: usize,
) -> [i16; 3] {
    let order_score = Cell::new(0);
    let distance_score = Cell::new(0);

    let mut iter = (index_first..=index_last)
        .filter_map(|index| {
            query_positions.iter().find_map(move |v| (v.index == index).then(|| v.range[0]))
        })
        .peekable();
    while let (Some(range_first), Some(next_range_first)) = (iter.next(), iter.peek()) {
        if range_first < *next_range_first {
            order_score.set(order_score.get() + 1);
        }
    }

    // count score for phrases
    let tally_phrase_scores = |fwp, lwp| {
        let words_in_phrase_minus_one = (lwp - fwp) as i16;
        // will always be in the order of query, so +1 for each space between words
        order_score.set(order_score.get() + words_in_phrase_minus_one);
        // distance will always be 1, so -1 for each space between words
        distance_score.set(distance_score.get() - words_in_phrase_minus_one);
    };

    let mut iter = matches[index_first..=index_last].iter().peekable();
    while let Some(r#match) = iter.next() {
        if let Some(next_match) = iter.peek() {
            let match_last_word_pos = match r#match.position {
                MatchPosition::Word { word_position, .. } => word_position,
                MatchPosition::Phrase { word_position_range: [fwp, lwp], .. } => {
                    tally_phrase_scores(fwp, lwp);
                    lwp
                }
            };
            let next_match_first_word_pos = next_match.get_first_word_pos();

            // compute distance between matches
            distance_score.set(
                distance_score.get()
                    - (next_match_first_word_pos - match_last_word_pos).min(7) as i16,
            );
        } else if let MatchPosition::Phrase { word_position_range: [fwp, lwp], .. } =
            r#match.position
        {
            // in case last match is a phrase, count score for its words
            tally_phrase_scores(fwp, lwp);
        }
    }

    let mut uniqueness_score = 0i16;
    let mut current_range: Option<super::matching_words::UserQueryPositionRange> = None;

    for qp in query_positions.iter().filter(|v| v.index >= index_first && v.index <= index_last) {
        match current_range.as_mut() {
            Some([saved_range_start, saved_range_end]) => {
                let [range_start, range_end] = qp.range;

                if range_start > *saved_range_start {
                    uniqueness_score += (*saved_range_end - *saved_range_start) as i16 + 1;

                    *saved_range_start = range_start;
                    *saved_range_end = range_end;
                } else if range_end > *saved_range_end {
                    *saved_range_end = range_end;
                }
            }
            None => current_range = Some(qp.range),
        }
    }

    if let Some([saved_range_start, saved_range_end]) = current_range {
        uniqueness_score += (saved_range_end - saved_range_start) as i16 + 1;
    }

    // rank by unique match count, then by distance between matches, then by ordered match count.
    [uniqueness_score, distance_score.into_inner(), order_score.into_inner()]
}

/// Returns the first and last match where the score computed by match_interval_score is the best.
pub fn get_best_match_index_range(
    matches: &[Match],
    query_positions: &[QueryPosition],
    crop_size: usize,
) -> [usize; 2] {
    // positions of the first and the last match of the best matches index range in `matches`.
    let mut best_matches_index_range: Option<MatchesIndexRangeWithScore> = None;

    let mut save_best_matches_index_range = |index_first, index_last| {
        let score = get_score(matches, query_positions, index_first, index_last);
        let is_score_better = best_matches_index_range.as_ref().is_none_or(|v| score > v.score);

        if is_score_better {
            best_matches_index_range = Some(MatchesIndexRangeWithScore {
                matches_index_range: [index_first, index_last],
                score,
            });
        }
    };

    // we compute the matches index range if we have at least 2 matches.
    let mut index_first = 0;
    let mut first_match_first_word_pos = matches[index_first].get_first_word_pos();

    for (index, next_match) in matches.iter().enumerate() {
        // if next match would make index range gross more than crop_size,
        // we compare the current index range with the best one,
        // then we increase `index_first` until next match can be added.
        let next_match_last_word_pos = next_match.get_last_word_pos();

        // if the next match would mean that we pass the crop size window,
        // we take the last valid match, that didn't pass this boundry, which is `index` - 1,
        // and calculate a score for it, and check if it's better than our best so far
        if next_match_last_word_pos - first_match_first_word_pos + 1 > crop_size {
            // if index is 0 there is no previous viable match
            if index != 0 {
                // keep index range if it's the best
                save_best_matches_index_range(index_first, index - 1);
            }

            // advance `index_first` while index range is longer than crop_size.
            loop {
                if index_first == matches.len() - 1 {
                    break;
                }

                index_first += 1;
                first_match_first_word_pos = matches[index_first].get_first_word_pos();

                // also make sure that subtracting won't cause a panic
                if next_match_last_word_pos < first_match_first_word_pos
                    || next_match_last_word_pos - first_match_first_word_pos + 1 < crop_size
                {
                    break;
                }
            }
        }
    }

    // compute the last index range score and compare it to the best one.
    let index_last = matches.len() - 1;
    // if it's the last match with itself, we need to make sure it's
    // not a phrase longer than the crop window
    if index_first != index_last || matches[index_first].get_word_count() < crop_size {
        save_best_matches_index_range(index_first, index_last);
    }

    // if none of the matches fit the criteria above, default to the first one
    best_matches_index_range.map_or([0, 0], |v| v.matches_index_range)
}
