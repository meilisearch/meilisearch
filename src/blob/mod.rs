mod ops;
pub mod positive;
pub mod negative;

pub use self::positive::{PositiveBlob, PositiveBlobBuilder};
pub use self::negative::{NegativeBlob, NegativeBlobBuilder};
pub use self::ops::OpBuilder;

use std::fmt;

use serde::ser::{Serialize, Serializer, SerializeTuple};
use serde::de::{self, Deserialize, Deserializer, SeqAccess, Visitor};

pub enum Blob {
    Positive(PositiveBlob),
    Negative(NegativeBlob),
}

impl Blob {
    pub fn is_negative(&self) -> bool {
        self.sign() == Sign::Negative
    }

    pub fn is_positive(&self) -> bool {
        self.sign() == Sign::Positive
    }

    pub fn sign(&self) -> Sign {
        match self {
            Blob::Positive(_) => Sign::Positive,
            Blob::Negative(_) => Sign::Negative,
        }
    }
}

impl Serialize for Blob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Blob::Positive(blob) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&Sign::Positive)?;
                tuple.serialize_element(&blob)?;
                tuple.end()
            },
            Blob::Negative(blob) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&Sign::Negative)?;
                tuple.serialize_element(&blob)?;
                tuple.end()
            },
        }
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Blob, D::Error> {
        struct TupleVisitor;

        impl<'de> Visitor<'de> for TupleVisitor {
            type Value = Blob;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a Blob struct")
            }

            #[inline]
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let sign = match seq.next_element()? {
                    Some(value) => value,
                    None => return Err(de::Error::invalid_length(0, &self)),
                };
                match sign {
                    Sign::Positive => {
                        let blob = match seq.next_element()? {
                            Some(value) => value,
                            None => return Err(de::Error::invalid_length(1, &self)),
                        };
                        Ok(Blob::Positive(blob))
                    },
                    Sign::Negative => {
                        let blob = match seq.next_element()? {
                            Some(value) => value,
                            None => return Err(de::Error::invalid_length(1, &self)),
                        };
                        Ok(Blob::Negative(blob))
                    },
                }
            }
        }

        deserializer.deserialize_tuple(2, TupleVisitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Sign {
    Positive,
    Negative,
}

impl Sign {
    pub fn invert(self) -> Sign {
        match self {
            Sign::Positive => Sign::Negative,
            Sign::Negative => Sign::Positive,
        }
    }
}
