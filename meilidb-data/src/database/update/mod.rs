mod documents_addition;
mod documents_deletion;
mod synonyms_addition;
mod synonyms_deletion;

pub use self::documents_addition::{DocumentsAddition, apply_documents_addition};
pub use self::documents_deletion::{DocumentsDeletion, apply_documents_deletion};
pub use self::synonyms_addition::{SynonymsAddition, apply_synonyms_addition};
pub use self::synonyms_deletion::{SynonymsDeletion, apply_synonyms_deletion};
