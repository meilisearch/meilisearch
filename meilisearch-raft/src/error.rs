use actix_raft::AppError;
use serde::{Serialize, Deserialize};

/// The application's error struct. This could be an enum as well.
///
/// NOTE: the below impls for Display & Error can be
/// derived using crates like `Failure` &c.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

impl std::error::Error for Error {}

// Mark this type for use as an `actix_raft::AppError`.
impl AppError for Error {}
