use std::cmp::Ordering;

use charabia::{SeparatorKind, Token};

#[derive(Clone)]
enum Direction {
    Forwards,
    Backwards,
}

impl Direction {
    fn switch(&mut self) {
        *self = match self {
            Direction::Backwards => Direction::Forwards,
            Direction::Forwards => Direction::Backwards,
        }
    }
}

fn get_adjusted_indices_for_too_few_words(
    tokens: &[Token],
    mut index_backward: usize,
    mut index_forward: usize,
    mut words_count: usize,
    crop_size: usize,
) -> [usize; 2] {
    let mut valid_index_backward = index_backward;
    let mut valid_index_forward = index_forward;

    let mut is_end_reached = index_forward == tokens.len() - 1;
    let mut is_beginning_reached = index_backward == 0;

    let mut is_index_backwards_at_hard_separator = false;
    let mut is_index_forwards_at_hard_separator = false;

    let mut is_crop_size_or_both_ends_reached =
        words_count == crop_size || (is_end_reached && is_beginning_reached);

    let mut dir = Direction::Forwards;

    loop {
        if is_crop_size_or_both_ends_reached {
            break;
        }

        let (index, valid_index) = match dir {
            Direction::Backwards => (&mut index_backward, &mut valid_index_backward),
            Direction::Forwards => (&mut index_forward, &mut valid_index_forward),
        };

        loop {
            match dir {
                Direction::Forwards => {
                    if is_end_reached {
                        break;
                    }

                    *index += 1;

                    is_end_reached = *index == tokens.len() - 1;
                }
                Direction::Backwards => {
                    if is_beginning_reached
                        || (!is_end_reached
                            && is_index_backwards_at_hard_separator
                            && !is_index_forwards_at_hard_separator)
                    {
                        break;
                    }

                    *index -= 1;

                    is_beginning_reached = *index == 0;
                }
            };

            if is_end_reached && is_beginning_reached {
                is_crop_size_or_both_ends_reached = true;
            }

            let maybe_is_token_hard_separator = tokens[*index]
                .separator_kind()
                .map(|sep_kind| matches!(sep_kind, SeparatorKind::Hard));

            // it's not a separator
            if maybe_is_token_hard_separator.is_none() {
                *valid_index = *index;
                words_count += 1;

                if words_count == crop_size {
                    is_crop_size_or_both_ends_reached = true;
                }

                break;
            }

            let is_index_at_hard_separator = match dir {
                Direction::Backwards => &mut is_index_backwards_at_hard_separator,
                Direction::Forwards => &mut is_index_forwards_at_hard_separator,
            };
            *is_index_at_hard_separator =
                maybe_is_token_hard_separator.is_some_and(|is_hard| is_hard);
        }

        dir.switch();

        // 1. if end is reached, we can only advance backwards
        // 2. if forwards index reached a hard separator and backwards is currently hard, we can go backwards
    }

    // keep advancing forward and backward to check if there's only separator tokens
    // left until the end if so, then include those too in the index range

    let saved_index = valid_index_forward;
    loop {
        if valid_index_forward == tokens.len() - 1 {
            break;
        }

        valid_index_forward += 1;

        if !tokens[valid_index_forward].is_separator() {
            valid_index_forward = saved_index;
            break;
        }
    }

    let saved_index = valid_index_backward;
    loop {
        if valid_index_backward == 0 {
            break;
        }

        valid_index_backward -= 1;

        if !tokens[valid_index_backward].is_separator() {
            valid_index_backward = saved_index;
            break;
        }
    }

    [valid_index_backward, valid_index_forward]
}

fn get_adjusted_index_forward_for_too_many_words(
    tokens: &[Token],
    index_backward: usize,
    mut index_forward: usize,
    mut words_count: usize,
    crop_size: usize,
) -> usize {
    loop {
        if index_forward == index_backward {
            return index_forward;
        }

        index_forward -= 1;

        if tokens[index_forward].is_separator() {
            continue;
        }

        words_count -= 1;

        if words_count == crop_size {
            break;
        }
    }

    index_forward
}

pub fn get_adjusted_indices_for_highlights_and_crop_size(
    tokens: &[Token],
    index_backward: usize,
    index_forward: usize,
    words_count: usize,
    crop_size: usize,
) -> [usize; 2] {
    match words_count.cmp(&crop_size) {
        Ordering::Equal | Ordering::Less => get_adjusted_indices_for_too_few_words(
            tokens,
            index_backward,
            index_forward,
            words_count,
            crop_size,
        ),
        Ordering::Greater => [
            index_backward,
            get_adjusted_index_forward_for_too_many_words(
                tokens,
                index_backward,
                index_forward,
                words_count,
                crop_size,
            ),
        ],
    }
}

pub fn get_adjusted_index_forward_for_crop_size(tokens: &[Token], crop_size: usize) -> usize {
    let mut words_count = 0;
    let mut index = 0;

    while index != tokens.len() - 1 {
        if !tokens[index].is_separator() {
            words_count += 1;

            if words_count == crop_size {
                break;
            }
        }

        index += 1;
    }

    if index == tokens.len() - 1 {
        return index;
    }

    index + 1
}
