use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Index not found")]
    IndexNotFound,
}
