mod ops;
pub mod positive;
pub mod negative;

pub use self::positive::{PositiveBlob, PositiveBlobBuilder};
pub use self::negative::NegativeBlob;
pub use self::ops::OpBuilder;

use std::io::{Cursor, BufRead};
use std::error::Error;
use std::sync::Arc;

use byteorder::{ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
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

    pub fn from_shared_bytes(bytes: Arc<Vec<u8>>, offset: usize, len: usize) -> Result<Blob, Box<Error>> {
        let mut cursor = Cursor::new(&bytes.as_slice()[..len]);
        cursor.consume(offset);

        let byte = cursor.read_u8()?;
        let blob = match Sign::from_byte(byte)? {
            Sign::Positive => {
                let offset = cursor.position() as usize;
                let len = len - offset;
                let blob = PositiveBlob::from_shared_bytes(bytes, offset, len)?;
                Blob::Positive(blob)
            },
            Sign::Negative => {
                let offset = cursor.position() as usize;
                let len = len - offset;
                let blob = NegativeBlob::from_shared_bytes(bytes, offset, len)?;
                Blob::Negative(blob)
            },
        };

        Ok(blob)
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        let sign = self.sign();
        sign.write_to_bytes(bytes);
        match self {
            Blob::Positive(b) => b.write_to_bytes(bytes),
            Blob::Negative(b) => b.write_to_bytes(bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    pub fn from_byte(byte: u8) -> Result<Sign, Box<Error>> {
        match byte {
            0 => Ok(Sign::Positive),
            1 => Ok(Sign::Negative),
            b => Err(format!("Invalid sign byte {:?}", b).into()),
        }
    }

    pub fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        match self {
            Sign::Positive => bytes.write_u8(0).unwrap(),
            Sign::Negative => bytes.write_u8(1).unwrap(),
        }
    }
}
