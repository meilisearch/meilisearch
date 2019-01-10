use std::fmt;

/// Represent an attribute number along with the word index
/// according to the tokenizer used.
///
/// It can accept up to 1024 attributes and word positions
/// can be maximum 2^22.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Attribute(u32);

impl Attribute {
    /// Construct an `Attribute` from an attribute number and
    /// the word position of a match according to the tokenizer used.
    pub(crate) fn new(attribute: u16, index: u32) -> Result<Attribute, AttributeError> {
        if attribute & 0b1111_1100_0000_0000 != 0 {
            return Err(AttributeError::AttributeTooBig)
        }

        if index & 0b1111_1111_1100_0000_0000_0000_0000 != 0 {
            return Err(AttributeError::IndexTooBig)
        }

        let attribute = u32::from(attribute) << 22;
        Ok(Attribute(attribute | index))
    }

    /// Construct an `Attribute` from an attribute number and
    /// the word position of a match according to the tokenizer used.
    ///
    /// # Panics
    ///
    /// The attribute must not be greater than 1024
    /// and the word index not greater than 2^22.
    pub(crate) fn new_faillible(attribute: u16, index: u32) -> Attribute {
        match Attribute::new(attribute, index) {
            Ok(attribute) => attribute,
            Err(AttributeError::AttributeTooBig) => {
                panic!("attribute must not be greater than 1024")
            },
            Err(AttributeError::IndexTooBig) => {
                panic!("attribute word index must not be greater than 2^22")
            },
        }
    }

    pub(crate) fn max_value() -> Attribute {
        Attribute(u32::max_value())
    }

    #[inline]
    pub fn attribute(self) -> u16 {
        (self.0 >> 22) as u16
    }

    #[inline]
    pub fn word_index(self) -> u32 {
        self.0 & 0b0000_0000_0011_1111_1111_1111_1111
    }
}

impl fmt::Debug for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Attribute")
            .field("attribute", &self.attribute())
            .field("word_index", &self.word_index())
            .finish()
    }
}

pub enum AttributeError {
    AttributeTooBig,
    IndexTooBig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{quickcheck, TestResult};

    quickcheck! {
        fn qc_attribute(gen_attr: u16, gen_index: u32) -> TestResult {
            if gen_attr > 2_u16.pow(10) || gen_index > 2_u32.pow(22) {
                return TestResult::discard()
            }

            let attribute = Attribute::new_faillible(gen_attr, gen_index);

            let valid_attribute = attribute.attribute() == gen_attr;
            let valid_index = attribute.word_index() == gen_index;

            TestResult::from_bool(valid_attribute && valid_index)
        }

        fn qc_attribute_ord(gen_attr: u16, gen_index: u32) -> TestResult {
            if gen_attr >= 2_u16.pow(10) || gen_index >= 2_u32.pow(22) {
                return TestResult::discard()
            }

            let a = Attribute::new_faillible(gen_attr, gen_index);
            let b = Attribute::new_faillible(gen_attr + 1, gen_index + 1);

            TestResult::from_bool(a < b)
        }
    }
}
