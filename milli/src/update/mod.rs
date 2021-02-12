mod available_documents_ids;
mod clear_documents;
mod delete_documents;
mod facets;
mod index_documents;
mod settings;
mod update_builder;
mod update_step;

pub use self::available_documents_ids::AvailableDocumentsIds;
pub use self::clear_documents::ClearDocuments;
pub use self::delete_documents::DeleteDocuments;
pub use self::index_documents::{IndexDocuments, IndexDocumentsMethod, UpdateFormat, DocumentAdditionResult};
pub use self::facets::Facets;
pub use self::settings::Settings;
pub use self::update_builder::UpdateBuilder;
pub use self::update_step::UpdateIndexingStep;
