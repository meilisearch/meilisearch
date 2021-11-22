pub use search::{default_crop_length, SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{apply_settings_to_builder, Checked, Facets, Settings, Unchecked};

mod dump;
pub mod error;
mod search;
pub mod update_handler;
pub mod updates;

#[allow(clippy::module_inception)]
mod index;

pub use index::{Document, IndexMeta, IndexStats};

#[cfg(not(test))]
pub use index::Index;

#[cfg(test)]
pub use test::MockIndex as Index;

/// The index::test module provides means of mocking an index instance. I can be used throughout the
/// code for unit testing, in places where an index would normally be used.
#[cfg(test)]
pub mod test {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use milli::update::DocumentAdditionResult;
    use milli::update::DocumentDeletionResult;
    use milli::update::IndexDocumentsMethod;
    use nelson::Mocker;
    use serde_json::{Map, Value};
    use uuid::Uuid;

    use crate::index_controller::update_file_store::UpdateFileStore;

    use super::error::Result;
    use super::index::Index;
    use super::update_handler::UpdateHandler;
    use super::{Checked, IndexMeta, IndexStats, SearchQuery, SearchResult, Settings};

    #[derive(Clone)]
    pub enum MockIndex {
        Real(Index),
        Mock(Arc<Mocker>),
    }

    impl MockIndex {
        pub fn faux(faux: Mocker) -> Self {
            Self::Mock(Arc::new(faux))
        }

        pub fn open(
            path: impl AsRef<Path>,
            size: usize,
            update_file_store: Arc<UpdateFileStore>,
            uuid: Uuid,
            update_handler: Arc<UpdateHandler>,
        ) -> Result<Self> {
            let index = Index::open(path, size, update_file_store, uuid, update_handler)?;
            Ok(Self::Real(index))
        }

        pub fn load_dump(
            src: impl AsRef<Path>,
            dst: impl AsRef<Path>,
            size: usize,
            update_handler: &UpdateHandler,
        ) -> anyhow::Result<()> {
            Index::load_dump(src, dst, size, update_handler)?;
            Ok(())
        }

        pub fn uuid(&self) -> Uuid {
            match self {
                MockIndex::Real(index) => index.uuid(),
                MockIndex::Mock(m) => unsafe { m.get("uuid").call(()) },
            }
        }

        pub fn stats(&self) -> Result<IndexStats> {
            match self {
                MockIndex::Real(index) => index.stats(),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn meta(&self) -> Result<IndexMeta> {
            match self {
                MockIndex::Real(index) => index.meta(),
                MockIndex::Mock(_) => todo!(),
            }
        }
        pub fn settings(&self) -> Result<Settings<Checked>> {
            match self {
                MockIndex::Real(index) => index.settings(),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn retrieve_documents<S: AsRef<str>>(
            &self,
            offset: usize,
            limit: usize,
            attributes_to_retrieve: Option<Vec<S>>,
        ) -> Result<Vec<Map<String, Value>>> {
            match self {
                MockIndex::Real(index) => {
                    index.retrieve_documents(offset, limit, attributes_to_retrieve)
                }
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn retrieve_document<S: AsRef<str>>(
            &self,
            doc_id: String,
            attributes_to_retrieve: Option<Vec<S>>,
        ) -> Result<Map<String, Value>> {
            match self {
                MockIndex::Real(index) => index.retrieve_document(doc_id, attributes_to_retrieve),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn size(&self) -> u64 {
            match self {
                MockIndex::Real(index) => index.size(),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
            match self {
                MockIndex::Real(index) => index.snapshot(path),
                MockIndex::Mock(m) => unsafe { m.get("snapshot").call(path.as_ref()) },
            }
        }

        pub fn inner(&self) -> &milli::Index {
            match self {
                MockIndex::Real(index) => index.inner(),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn perform_search(&self, query: SearchQuery) -> Result<SearchResult> {
            match self {
                MockIndex::Real(index) => index.perform_search(query),
                MockIndex::Mock(m) => unsafe { m.get("perform_search").call(query) },
            }
        }

        pub fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
            match self {
                MockIndex::Real(index) => index.dump(path),
                MockIndex::Mock(m) => unsafe { m.get("dump").call(path.as_ref()) },
            }
        }

        pub fn update_documents(
            &self,
            method: IndexDocumentsMethod,
            content_uuid: Uuid,
            primary_key: Option<String>,
        ) -> Result<DocumentAdditionResult> {
            match self {
                MockIndex::Real(index) => index.update_documents(method, content_uuid, primary_key),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn update_settings(&self, settings: &Settings<Checked>) -> Result<()> {
            match self {
                MockIndex::Real(index) => index.update_settings(settings),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn update_primary_key(&self, primary_key: String) -> Result<IndexMeta> {
            match self {
                MockIndex::Real(index) => index.update_primary_key(primary_key),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn delete_documents(&self, ids: &[String]) -> Result<DocumentDeletionResult> {
            match self {
                MockIndex::Real(index) => index.delete_documents(ids),
                MockIndex::Mock(_) => todo!(),
            }
        }

        pub fn clear_documents(&self) -> Result<()> {
            match self {
                MockIndex::Real(index) => index.clear_documents(),
                MockIndex::Mock(_) => todo!(),
            }
        }
    }

    #[test]
    fn test_faux_index() {
        let faux = Mocker::default();
        faux.when("snapshot")
            .times(2)
            .then(|_: &Path| -> Result<()> { Ok(()) });

        let index = MockIndex::faux(faux);

        let path = PathBuf::from("hello");
        index.snapshot(&path).unwrap();
        index.snapshot(&path).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_faux_unexisting_method_stub() {
        let faux = Mocker::default();

        let index = MockIndex::faux(faux);

        let path = PathBuf::from("hello");
        index.snapshot(&path).unwrap();
        index.snapshot(&path).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_faux_panic() {
        let faux = Mocker::default();
        faux.when("snapshot")
            .times(2)
            .then(|_: &Path| -> Result<()> {
                panic!();
            });

        let index = MockIndex::faux(faux);

        let path = PathBuf::from("hello");
        index.snapshot(&path).unwrap();
        index.snapshot(&path).unwrap();
    }
}
