//! This module provides the `Boost` type and defines all the errors related to this type.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::RankingRuleError;

/// This error type is never supposed to be shown to the end user.
/// You must always cast it to a sort error or a criterion error.
#[derive(Error, Debug)]
pub enum BoostError {
    #[error("Invalid syntax for the boost parameter: expected expression starting with `boost:`, found `{name}`.")]
    InvalidSyntax { name: String },
}

impl From<BoostError> for RankingRuleError {
    fn from(error: BoostError) -> Self {
        match error {
            BoostError::InvalidSyntax { name } => RankingRuleError::InvalidName { name },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Boost(pub String);

impl FromStr for Boost {
    type Err = BoostError;

    fn from_str(text: &str) -> Result<Boost, Self::Err> {
        match text.split_once(':') {
            Some(("boost", right)) => Ok(Boost(right.to_string())), // TODO check filter validity
            _ => Err(BoostError::InvalidSyntax { name: text.to_string() }),
        }
    }
}
