use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(OrderedFloat<f64>),
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
