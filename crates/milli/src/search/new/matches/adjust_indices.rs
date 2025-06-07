use std::cmp::Ordering;

use charabia::{SeparatorKind, Token, TokenKind};

enum SimpleTokenKind {
    Separator(SeparatorKind),
    NonSeparator,
    Done,
}

impl SimpleTokenKind {
    fn new(token: &Token) -> Self {
        match token.kind {
            TokenKind::Separator(separator_kind) => Self::Separator(separator_kind),
            _ => Self::NonSeparator,
        }
    }
}

struct CropBoundsHelper<'a> {
    tokens: &'a [Token<'a>],
    index_backward: usize,
    backward_token_kind: SimpleTokenKind,
    index_forward: usize,
    forward_token_kind: SimpleTokenKind,
}

impl CropBoundsHelper<'_> {
    fn advance_backward(&mut self) {
        if matches!(self.backward_token_kind, SimpleTokenKind::Done) {
            return;
        }

        if self.index_backward != 0 {
            self.index_backward -= 1;
            self.backward_token_kind = SimpleTokenKind::new(&self.tokens[self.index_backward]);
        } else {
            self.backward_token_kind = SimpleTokenKind::Done;
        }
    }

    fn advance_forward(&mut self) {
        if matches!(self.forward_token_kind, SimpleTokenKind::Done) {
            return;
        }

        if self.index_forward != self.tokens.len() - 1 {
            self.index_forward += 1;
            self.forward_token_kind = SimpleTokenKind::new(&self.tokens[self.index_forward]);
        } else {
            self.forward_token_kind = SimpleTokenKind::Done;
        }
    }
}

fn get_adjusted_indices_for_too_few_words(
    tokens: &[Token],
    index_backward: usize,
    index_forward: usize,
    mut words_count: usize,
    crop_size: usize,
) -> [usize; 2] {
    let crop_size = crop_size + 2;
    let mut cbh = CropBoundsHelper {
        tokens,
        index_backward,
        backward_token_kind: SimpleTokenKind::new(&tokens[index_backward]),
        index_forward,
        forward_token_kind: SimpleTokenKind::new(&tokens[index_forward]),
    };

    loop {
        match [&cbh.backward_token_kind, &cbh.forward_token_kind] {
            // if they are both separators and are the same kind then advance both,
            // or expand in the soft separator side
            [SimpleTokenKind::Separator(backward_sk), SimpleTokenKind::Separator(forward_sk)] => {
                if backward_sk == forward_sk {
                    cbh.advance_backward();

                    // this avoids having an ending separator before crop marker
                    if words_count < crop_size - 1 {
                        cbh.advance_forward();
                    }
                } else if matches!(backward_sk, SeparatorKind::Hard) {
                    cbh.advance_forward();
                } else {
                    cbh.advance_backward();
                }
            }
            // both are words, advance left then right if we haven't reached `crop_size`
            [SimpleTokenKind::NonSeparator, SimpleTokenKind::NonSeparator] => {
                cbh.advance_backward();
                words_count += 1;

                if words_count != crop_size {
                    cbh.advance_forward();
                    words_count += 1;
                }
            }
            [SimpleTokenKind::Done, SimpleTokenKind::Done] => break,
            // if one of the tokens is non-separator and the other a separator, we expand in the non-separator side
            // if one of the sides reached the end, we expand in the opposite direction
            [backward_stk, SimpleTokenKind::Done]
            | [backward_stk @ SimpleTokenKind::NonSeparator, SimpleTokenKind::Separator(_)] => {
                if matches!(backward_stk, SimpleTokenKind::NonSeparator) {
                    words_count += 1;
                }
                cbh.advance_backward();
            }
            [SimpleTokenKind::Done, forward_stk]
            | [SimpleTokenKind::Separator(_), forward_stk @ SimpleTokenKind::NonSeparator] => {
                if matches!(forward_stk, SimpleTokenKind::NonSeparator) {
                    words_count += 1;
                }
                cbh.advance_forward();
            }
        }

        if words_count == crop_size {
            break;
        }
    }

    [cbh.index_backward, cbh.index_forward]
}

fn get_adjusted_index_forward_for_too_many_words(
    tokens: &[Token],
    mut index_forward: usize,
    mut words_count: usize,
    crop_size: usize,
) -> usize {
    while index_forward != 0 {
        if matches!(SimpleTokenKind::new(&tokens[index_forward]), SimpleTokenKind::NonSeparator) {
            words_count -= 1;

            if words_count == crop_size {
                break;
            }
        }

        index_forward -= 1;
    }

    if index_forward == 0 {
        return index_forward;
    }

    index_forward - 1
}

pub fn get_adjusted_indices_for_highlights_and_crop_size(
    tokens: &[Token],
    index_backward: usize,
    index_forward: usize,
    words_count: usize,
    crop_size: usize,
) -> [usize; 2] {
    match words_count.cmp(&crop_size) {
        Ordering::Less => get_adjusted_indices_for_too_few_words(
            tokens,
            index_backward,
            index_forward,
            words_count,
            crop_size,
        ),
        Ordering::Equal => [
            if index_backward != 0 { index_backward - 1 } else { index_backward },
            if index_forward != tokens.len() - 1 { index_forward + 1 } else { index_forward },
        ],
        Ordering::Greater => [
            index_backward,
            get_adjusted_index_forward_for_too_many_words(
                tokens,
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
        if matches!(SimpleTokenKind::new(&tokens[index]), SimpleTokenKind::NonSeparator) {
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
