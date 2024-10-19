use super::matching_words::WordId;

#[derive(Clone, Debug)]
pub enum MatchPosition {
    Word {
        // position of the word in the whole text.
        word_position: usize,
        // position of the token in the whole text.
        token_position: usize,
    },
    Phrase {
        // position of the first and last word in the phrase in the whole text.
        word_positions: [usize; 2],
        // position of the first and last token in the phrase in the whole text.
        token_positions: [usize; 2],
    },
}

#[derive(Clone, Debug)]
pub struct Match {
    pub char_count: usize,
    // ids of the query words that matches.
    pub ids: Vec<WordId>,
    pub position: MatchPosition,
}

impl Match {
    pub(super) fn get_first_word_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { word_position, .. } => word_position,
            MatchPosition::Phrase { word_positions: [fwp, _], .. } => fwp,
        }
    }

    pub(super) fn get_last_word_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { word_position, .. } => word_position,
            MatchPosition::Phrase { word_positions: [_, lwp], .. } => lwp,
        }
    }

    pub(super) fn get_first_token_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { token_position, .. } => token_position,
            MatchPosition::Phrase { token_positions: [ftp, _], .. } => ftp,
        }
    }

    pub(super) fn get_last_token_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { token_position, .. } => token_position,
            MatchPosition::Phrase { token_positions: [_, ltp], .. } => ltp,
        }
    }

    pub(super) fn get_word_count(&self) -> usize {
        match self.position {
            MatchPosition::Word { .. } => 1,
            MatchPosition::Phrase { word_positions: [fwp, lwp], .. } => lwp - fwp + 1,
        }
    }
}
