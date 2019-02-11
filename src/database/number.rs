use std::cmp::Ordering;
use std::str::FromStr;
use std::fmt;

use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
#[derive(Debug, Copy, Clone)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
}

impl FromStr for Number {
    type Err = ParseNumberError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(unsigned) = u64::from_str(s) {
            return Ok(Number::Unsigned(unsigned))
        }

        if let Ok(signed) = i64::from_str(s) {
            return Ok(Number::Signed(signed))
        }

        if let Ok(float) = f64::from_str(s) {
            if float == 0.0 || float.is_normal() {
                return Ok(Number::Float(float))
            }
        }

        Err(ParseNumberError)
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Number) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Number) -> Ordering {
        use Number::*;
        match (self, other) {
            (Unsigned(s), Unsigned(o)) => s.cmp(o),
            (Unsigned(s), Signed(o)) => {
                let s = i128::from(*s);
                let o = i128::from(*o);
                s.cmp(&o)
            },
            (Unsigned(s), Float(o)) => {
                let s = *s as f64;
                s.partial_cmp(&o).unwrap_or(Ordering::Equal)
            },

            (Signed(s), Unsigned(o)) => {
                let s = i128::from(*s);
                let o = i128::from(*o);
                s.cmp(&o)
            },
            (Signed(s), Signed(o)) => s.cmp(o),
            (Signed(s), Float(o)) => {
                let s = *s as f64;
                s.partial_cmp(o).unwrap_or(Ordering::Equal)
            },

            (Float(s), Unsigned(o)) => {
                let o = *o as f64;
                s.partial_cmp(&o).unwrap_or(Ordering::Equal)
            },
            (Float(s), Signed(o)) => {
                let o = *o as f64;
                s.partial_cmp(&o).unwrap_or(Ordering::Equal)
            },
            (Float(s), Float(o)) => {
                s.partial_cmp(o).unwrap_or(Ordering::Equal)
            },
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Number) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Number { }

pub struct ParseNumberError;

impl fmt::Display for ParseNumberError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("can not parse number")
    }
}
