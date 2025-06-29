#[derive(Debug, PartialEq)]
pub enum MatchPosition {
    Word { word_position: usize, token_position: usize },
    Phrase { word_position_range: [usize; 2], token_position_range: [usize; 2] },
}

#[derive(Debug, PartialEq)]
pub struct Match {
    pub char_count: usize,
    pub byte_len: usize,
    pub position: MatchPosition,
}

impl Match {
    pub fn get_first_word_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { word_position, .. } => word_position,
            MatchPosition::Phrase { word_position_range: [fwp, _], .. } => fwp,
        }
    }

    pub fn get_last_word_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { word_position, .. } => word_position,
            MatchPosition::Phrase { word_position_range: [_, lwp], .. } => lwp,
        }
    }

    pub fn get_first_token_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { token_position, .. } => token_position,
            MatchPosition::Phrase { token_position_range: [ftp, _], .. } => ftp,
        }
    }

    pub fn get_last_token_pos(&self) -> usize {
        match self.position {
            MatchPosition::Word { token_position, .. } => token_position,
            MatchPosition::Phrase { token_position_range: [_, ltp], .. } => ltp,
        }
    }

    pub fn get_word_count(&self) -> usize {
        match self.position {
            MatchPosition::Word { .. } => 1,
            MatchPosition::Phrase { word_position_range: [fwp, lwp], .. } => lwp - fwp + 1,
        }
    }
}
