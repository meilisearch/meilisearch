use std::fmt;

/// Represent a word position in bytes along with the length of it.
///
/// It can represent words byte index to maximum 2^22 and
/// up to words of length 1024.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WordArea(u32);

impl WordArea {
    /// Construct a `WordArea` from a word position in expresed as
    /// a number of characters and the length of it.
    ///
    /// # Panics
    ///
    /// The char index must not be greater than 2^22
    /// and the length not greater than 1024.
    pub(crate) fn new(char_index: u32, length: u16) -> Result<WordArea, WordAreaError> {
        if char_index & 0b1111_1111_1100_0000_0000_0000_0000 != 0 {
            return Err(WordAreaError::ByteIndexTooBig)
        }

        if length & 0b1111_1100_0000_0000 != 0 {
            return Err(WordAreaError::LengthTooBig)
        }

        let char_index = char_index << 10;
        Ok(WordArea(char_index | u32::from(length)))
    }

    pub(crate) fn new_faillible(char_index: u32, length: u16) -> WordArea {
        match WordArea::new(char_index, length) {
            Ok(word_area) => word_area,
            Err(WordAreaError::ByteIndexTooBig) => {
                panic!("word area byte index must not be greater than 2^22")
            },
            Err(WordAreaError::LengthTooBig) => {
                panic!("word area length must not be greater than 1024")
            },
        }
    }

    pub(crate) fn max_value() -> WordArea {
        WordArea(u32::max_value())
    }

    #[inline]
    pub fn char_index(self) -> u32 {
        self.0 >> 10
    }

    #[inline]
    pub fn length(self) -> u16 {
        (self.0 & 0b0000_0000_0000_0000_0011_1111_1111) as u16
    }
}

impl fmt::Debug for WordArea {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("WordArea")
            .field("char_index", &self.char_index())
            .field("length", &self.length())
            .finish()
    }
}

pub enum WordAreaError {
    ByteIndexTooBig,
    LengthTooBig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{quickcheck, TestResult};

    quickcheck! {
        fn qc_word_area(gen_char_index: u32, gen_length: u16) -> TestResult {
            if gen_char_index > 2_u32.pow(22) || gen_length > 2_u16.pow(10) {
                return TestResult::discard()
            }

            let word_area = WordArea::new_faillible(gen_char_index, gen_length);

            let valid_char_index = word_area.char_index() == gen_char_index;
            let valid_length = word_area.length() == gen_length;

            TestResult::from_bool(valid_char_index && valid_length)
        }

        fn qc_word_area_ord(gen_char_index: u32, gen_length: u16) -> TestResult {
            if gen_char_index >= 2_u32.pow(22) || gen_length >= 2_u16.pow(10) {
                return TestResult::discard()
            }

            let a = WordArea::new_faillible(gen_char_index, gen_length);
            let b = WordArea::new_faillible(gen_char_index + 1, gen_length + 1);

            TestResult::from_bool(a < b)
        }
    }
}
