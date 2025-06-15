use std::cmp::{max, min};

use super::{
    matching_words::QueryPosition,
    r#match::{Match, MatchPosition},
};

use super::adjust_indices::{
    get_adjusted_index_forward_for_crop_size, get_adjusted_indices_for_highlights_and_crop_size,
};
use charabia::Token;
use serde::Serialize;
use utoipa::ToSchema;

use super::FormatOptions;

// TODO: Differentiate if full match do not return None, instead return match bounds with full length
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MatchBounds {
    pub highlight_toggle: bool,
    pub indices: Vec<usize>,
}

struct MatchBoundsHelper<'a> {
    tokens: &'a [Token<'a>],
    matches: &'a [Match],
    query_positions: &'a [QueryPosition],
}

struct MatchesAndCropIndices {
    matches_first_index: usize,
    matches_last_index: usize,
    crop_byte_start: usize,
    crop_byte_end: usize,
}

enum CropThing {
    Last(usize),
    First(usize),
}

impl MatchBoundsHelper<'_> {
    fn get_match_byte_position_range(&self, r#match: &Match) -> [usize; 2] {
        let byte_start = match r#match.position {
            MatchPosition::Word { token_position, .. } => self.tokens[token_position].byte_start,
            MatchPosition::Phrase { token_position_range: [ftp, ..], .. } => {
                self.tokens[ftp].byte_start
            }
        };

        [byte_start, byte_start + r#match.byte_len]
    }

    // TODO: Rename this
    fn get_match_byte_position_rangee(
        &self,
        index: &mut usize,
        crop_thing: CropThing,
    ) -> [usize; 2] {
        let new_index = match crop_thing {
            CropThing::First(_) if *index != 0 => *index - 1,
            CropThing::Last(_) if *index != self.matches.len() - 1 => *index + 1,
            _ => {
                return self.get_match_byte_position_range(&self.matches[*index]);
            }
        };

        let [byte_start, byte_end] = self.get_match_byte_position_range(&self.matches[new_index]);

        // NOTE: This doesn't need additional checks, because `get_best_match_index_range` already
        // guarantees that the next or preceding match contains the crop boundary
        match crop_thing {
            CropThing::First(crop_byte_start) if crop_byte_start < byte_end => {
                *index -= 1;
                [byte_start, byte_end]
            }
            CropThing::Last(crop_byte_end) if byte_start < crop_byte_end => {
                *index += 1;
                [byte_start, byte_end]
            }
            _ => self.get_match_byte_position_range(&self.matches[*index]),
        }
    }

    /// TODO: Description
    fn get_match_bounds(&self, mci: MatchesAndCropIndices) -> MatchBounds {
        let MatchesAndCropIndices {
            mut matches_first_index,
            mut matches_last_index,
            crop_byte_start,
            crop_byte_end,
        } = mci;

        let [first_match_first_byte, first_match_last_byte] = self.get_match_byte_position_rangee(
            &mut matches_first_index,
            CropThing::First(crop_byte_start),
        );
        let first_match_first_byte = max(first_match_first_byte, crop_byte_start);

        let [last_match_first_byte, last_match_last_byte] =
            if matches_first_index != matches_last_index {
                self.get_match_byte_position_rangee(
                    &mut matches_last_index,
                    CropThing::Last(crop_byte_end),
                )
            } else {
                [first_match_first_byte, first_match_last_byte]
            };
        let last_match_last_byte = min(last_match_last_byte, crop_byte_end);

        let selected_matches_len = matches_last_index - matches_first_index + 1;
        let mut indices_size = 2 * selected_matches_len;

        let crop_byte_start_is_not_first_match_start = crop_byte_start != first_match_first_byte;
        let crop_byte_end_is_not_last_match_end = crop_byte_end != last_match_last_byte;

        if crop_byte_start_is_not_first_match_start {
            indices_size += 1;
        }

        if crop_byte_end_is_not_last_match_end {
            indices_size += 1;
        }

        let mut indices = Vec::with_capacity(indices_size);

        if crop_byte_start_is_not_first_match_start {
            indices.push(crop_byte_start);
        }

        indices.push(first_match_first_byte);

        if selected_matches_len > 1 {
            indices.push(first_match_last_byte);
        }

        if selected_matches_len > 2 {
            for index in (matches_first_index + 1)..matches_last_index {
                let [m_byte_start, m_byte_end] =
                    self.get_match_byte_position_range(&self.matches[index]);

                indices.push(m_byte_start);
                indices.push(m_byte_end);
            }
        }

        if selected_matches_len > 1 {
            indices.push(last_match_first_byte);
        }

        indices.push(last_match_last_byte);

        if crop_byte_end_is_not_last_match_end {
            indices.push(crop_byte_end);
        }

        MatchBounds { highlight_toggle: !crop_byte_start_is_not_first_match_start, indices }
    }

    /// For crop but no highlight.
    fn get_crop_bounds_with_no_matches(&self, crop_size: usize) -> MatchBounds {
        let final_token_index = get_adjusted_index_forward_for_crop_size(self.tokens, crop_size);
        let final_token = &self.tokens[final_token_index];

        // TODO: Why is it that when we match all of the tokens we need to get byte_end instead of start?

        // TODO: Can here be an error, because it's byte_start but it could be byte_end?
        MatchBounds { highlight_toggle: false, indices: vec![0, final_token.byte_start] }
    }

    fn get_matches_and_crop_indices(&self, crop_size: usize) -> MatchesAndCropIndices {
        let asd = |i1, i2| {
            println!(
                "{}|{}|{}\n{} {}",
                self.tokens[..i1].iter().map(|v| v.lemma()).collect::<Vec<_>>().join(""),
                self.tokens[i1..i2].iter().map(|v| v.lemma()).collect::<Vec<_>>().join(""),
                self.tokens[i2..].iter().map(|v| v.lemma()).collect::<Vec<_>>().join(""),
                i1,
                i2
            );
        };

        // TODO: This doesn't give back 2 phrases if one is out of crop window
        // Solution: also get next and previous matches, and if they're in the crop window, even if partially, highlight them
        let [matches_first_index, matches_last_index] =
            super::best_match_range::get_best_match_index_range(
                self.matches,
                self.query_positions,
                crop_size,
            );

        let first_match = &self.matches[matches_first_index];
        let last_match = &self.matches[matches_last_index];

        let last_match_last_word_pos = last_match.get_last_word_pos();
        let first_match_first_word_pos = first_match.get_first_word_pos();

        let words_count = last_match_last_word_pos - first_match_first_word_pos + 1;
        let [index_backward, index_forward] = get_adjusted_indices_for_highlights_and_crop_size(
            self.tokens,
            first_match.get_first_token_pos(),
            last_match.get_last_token_pos(),
            words_count,
            crop_size,
        );

        asd(first_match.get_first_token_pos(), last_match.get_last_token_pos());
        asd(index_backward, index_forward);

        let backward_token = &self.tokens[index_backward];
        let forward_token = &self.tokens[index_forward];

        MatchesAndCropIndices {
            matches_first_index,
            matches_last_index,
            crop_byte_start: backward_token.byte_start,
            crop_byte_end: forward_token.byte_end,
        }
    }

    /// TODO: description
    fn get_crop_and_highlight_bounds_with_matches(&self, crop_size: usize) -> MatchBounds {
        self.get_match_bounds(self.get_matches_and_crop_indices(crop_size))
    }

    /// For when there are no matches, but crop is required.
    fn get_crop_bounds_with_matches(&self, crop_size: usize) -> MatchBounds {
        let mci = self.get_matches_and_crop_indices(crop_size);

        MatchBounds {
            highlight_toggle: false,
            indices: vec![mci.crop_byte_start, mci.crop_byte_end],
        }
    }
}

impl MatchBounds {
    pub fn try_new(
        tokens: &[Token],
        matches: &[Match],
        query_positions: &[QueryPosition],
        format_options: FormatOptions,
    ) -> Option<MatchBounds> {
        let mbh = MatchBoundsHelper { tokens, matches, query_positions };

        if let Some(crop_size) = format_options.crop.filter(|v| *v != 0) {
            if matches.is_empty() {
                return Some(mbh.get_crop_bounds_with_no_matches(crop_size));
            }

            if format_options.highlight {
                return Some(mbh.get_crop_and_highlight_bounds_with_matches(crop_size));
            }

            return Some(mbh.get_crop_bounds_with_matches(crop_size));
        }

        if !format_options.highlight || matches.is_empty() {
            return None;
        }

        Some(mbh.get_match_bounds(MatchesAndCropIndices {
            matches_first_index: 0,
            matches_last_index: matches.len() - 1,
            crop_byte_start: 0,
            crop_byte_end: tokens[tokens.len() - 1].byte_end,
        }))
    }
}
