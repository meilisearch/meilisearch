use std::cmp::Ordering;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Hash)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(OrderedFloat<f64>),
    Null,
}

impl Default for Number {
    fn default() -> Self {
        Self::Null
    }
}

impl FromStr for Number {
    type Err = ParseNumberError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uint_error = match u64::from_str(s) {
            Ok(unsigned) => return Ok(Number::Unsigned(unsigned)),
            Err(error) => error,
        };

        let int_error = match i64::from_str(s) {
            Ok(signed) => return Ok(Number::Signed(signed)),
            Err(error) => error,
        };

        let float_error = match f64::from_str(s) {
            Ok(float) => return Ok(Number::Float(OrderedFloat(float))),
            Err(error) => error,
        };

        Err(ParseNumberError {
            uint_error,
            int_error,
            float_error,
        })
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Number) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Number {}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Number) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        use Number::{Float, Signed, Unsigned, Null};

        match (*self, *other) {
            (Unsigned(a), Unsigned(b)) => a.cmp(&b),
            (Unsigned(a), Signed(b)) => {
                if b < 0 {
                    Ordering::Greater
                } else {
                    a.cmp(&(b as u64))
                }
            }
            (Unsigned(a), Float(b)) => (OrderedFloat(a as f64)).cmp(&b),
            (Signed(a), Unsigned(b)) => {
                if a < 0 {
                    Ordering::Less
                } else {
                    (a as u64).cmp(&b)
                }
            }
            (Signed(a), Signed(b)) => a.cmp(&b),
            (Signed(a), Float(b)) => OrderedFloat(a as f64).cmp(&b),
            (Float(a), Unsigned(b)) => a.cmp(&OrderedFloat(b as f64)),
            (Float(a), Signed(b)) => a.cmp(&OrderedFloat(b as f64)),
            (Float(a), Float(b)) => a.cmp(&b),
            (Null, Null) => Ordering::Equal,
            (_, Null) => Ordering::Less,
            (Null, _) => Ordering::Greater,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseNumberError {
    uint_error: ParseIntError,
    int_error: ParseIntError,
    float_error: ParseFloatError,
}

impl fmt::Display for ParseNumberError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.uint_error == self.int_error {
            write!(
                f,
                "can not parse number: {}, {}",
                self.uint_error, self.float_error
            )
        } else {
            write!(
                f,
                "can not parse number: {}, {}, {}",
                self.uint_error, self.int_error, self.float_error
            )
        }
    }
}
