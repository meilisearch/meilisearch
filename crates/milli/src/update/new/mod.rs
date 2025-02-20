pub use document_change::{Deletion, DocumentChange, Insertion, Update};
pub use indexer::ChannelCongestion;
pub use merger::{
    merge_and_send_docids, merge_and_send_facet_docids, FacetDatabases, FacetFieldIdsDelta,
};

use super::del_add::DelAdd;
use crate::FieldId;

mod channel;
pub mod document;
mod document_change;
mod extract;
mod facet_search_builder;
mod fst_merger_builder;
pub mod indexer;
mod merger;
mod parallel_iterator_ext;
mod ref_cell_ext;
pub mod reindex;
pub(crate) mod steps;
pub(crate) mod thread_local;
pub mod vector_document;
mod word_fst_builder;
mod words_prefix_docids;

/// TODO move them elsewhere
pub type StdResult<T, E> = std::result::Result<T, E>;
pub type KvReaderDelAdd = obkv::KvReader<DelAdd>;
pub type KvReaderFieldId = obkv::KvReader<FieldId>;
pub type KvWriterDelAdd<W> = obkv::KvWriter<W, DelAdd>;
pub type KvWriterFieldId<W> = obkv::KvWriter<W, FieldId>;
