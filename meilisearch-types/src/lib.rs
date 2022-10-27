pub mod error;
pub mod index_uid;
pub mod star_or;
pub type StarIndexType = star_or::StarOr<index_uid::IndexType>;
