pub use search::{
    SearchQuery, SearchResult, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER,
    DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT,
};
pub use updates::{apply_settings_to_builder, Checked, Facets, Settings, Unchecked};

mod dump;
pub mod error;
mod search;
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

    use milli::update::IndexerConfig;
    use milli::update::{DocumentAdditionResult, DocumentDeletionResult, IndexDocumentsMethod};
    use nelson::Mocker;
    use uuid::Uuid;

    use super::error::Result;
    use super::index::Index;
    use super::Document;
    use super::{Checked, IndexMeta, IndexStats, SearchQuery, SearchResult, Settings};
    use crate::update_file_store::UpdateFileStore;

    #[derive(Clone)]
    pub enum MockIndex {
        Real(Index),
        Mock(Arc<Mocker>),
    }

    impl MockIndex {
        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(Arc::new(mocker))
        }

        pub fn open(
            path: impl AsRef<Path>,
            size: usize,
            uuid: Uuid,
            update_handler: Arc<IndexerConfig>,
        ) -> Result<Self> {
            let index = Index::open(path, size, uuid, update_handler)?;
            Ok(Self::Real(index))
        }

        pub fn load_dump(
            src: impl AsRef<Path>,
            dst: impl AsRef<Path>,
            size: usize,
            update_handler: &IndexerConfig,
        ) -> anyhow::Result<()> {
            Index::load_dump(src, dst, size, update_handler)
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
                MockIndex::Mock(m) => unsafe { m.get("stats").call(()) },
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
        ) -> Result<(u64, Vec<Document>)> {
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
        ) -> Result<Document> {
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

        pub fn close(self) {
            match self {
                MockIndex::Real(index) => index.close(),
                MockIndex::Mock(m) => unsafe { m.get("close").call(()) },
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
            primary_key: Option<String>,
            file_store: UpdateFileStore,
            contents: impl Iterator<Item = Uuid>,
        ) -> Result<DocumentAdditionResult> {
            match self {
                MockIndex::Real(index) => {
                    index.update_documents(method, primary_key, file_store, contents)
                }
                MockIndex::Mock(mocker) => unsafe {
                    mocker
                        .get("update_documents")
                        .call((method, primary_key, file_store, contents))
                },
            }
        }

        pub fn update_settings(&self, settings: &Settings<Checked>) -> Result<()> {
            match self {
                MockIndex::Real(index) => index.update_settings(settings),
                MockIndex::Mock(m) => unsafe { m.get("update_settings").call(settings) },
            }
        }

        pub fn update_primary_key(&self, primary_key: String) -> Result<IndexMeta> {
            match self {
                MockIndex::Real(index) => index.update_primary_key(primary_key),
                MockIndex::Mock(m) => unsafe { m.get("update_primary_key").call(primary_key) },
            }
        }

        pub fn delete_documents(&self, ids: &[String]) -> Result<DocumentDeletionResult> {
            match self {
                MockIndex::Real(index) => index.delete_documents(ids),
                MockIndex::Mock(m) => unsafe { m.get("delete_documents").call(ids) },
            }
        }

        pub fn clear_documents(&self) -> Result<()> {
            match self {
                MockIndex::Real(index) => index.clear_documents(),
                MockIndex::Mock(m) => unsafe { m.get("clear_documents").call(()) },
            }
        }
    }

    #[test]
    fn test_faux_index() {
        let faux = Mocker::default();
        faux.when("snapshot")
            .times(2)
            .then(|_: &Path| -> Result<()> { Ok(()) });

        let index = MockIndex::mock(faux);

        let path = PathBuf::from("hello");
        index.snapshot(&path).unwrap();
        index.snapshot(&path).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_faux_unexisting_method_stub() {
        let faux = Mocker::default();

        let index = MockIndex::mock(faux);

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

        let index = MockIndex::mock(faux);

        let path = PathBuf::from("hello");
        index.snapshot(&path).unwrap();
        index.snapshot(&path).unwrap();
    }
}
