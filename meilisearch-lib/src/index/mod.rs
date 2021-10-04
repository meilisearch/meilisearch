pub use search::{default_crop_length, SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{apply_settings_to_builder, Checked, Facets, Settings, Unchecked};

pub mod error;
pub mod update_handler;
mod dump;
mod search;
mod updates;
mod index;

pub use index::{Document, IndexMeta, IndexStats};

#[cfg(not(test))]
pub use index::Index;

#[cfg(test)]
pub use test::MockIndex as Index;

#[cfg(test)]
pub mod test {
    use std::any::Any;
    use std::collections::HashMap;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::{path::Path, sync::Arc};

    use serde_json::{Map, Value};
    use uuid::Uuid;

    use crate::index_controller::update_file_store::UpdateFileStore;
    use crate::index_controller::updates::status::{Failed, Processed, Processing};

    use super::{Checked, IndexMeta, IndexStats, SearchQuery, SearchResult, Settings};
    use super::index::Index;
    use super::error::Result;
    use super::update_handler::UpdateHandler;


    pub struct Stub<A, R> {
        name: String,
        times: Option<usize>,
        stub: Box<dyn Fn(A) -> R + Sync + Send>,
        invalidated: bool,
    }

    impl<A, R> Drop for Stub<A, R> {
        fn drop(&mut self) {
            if !self.invalidated {
                if let Some(n) = self.times {
                    assert_eq!(n, 0, "{} not called enough times", self.name);
                }
            }
        }
    }

    impl<A: UnwindSafe, R> Stub<A, R> {
        fn call(&mut self, args: A) -> R {
            match self.times {
                Some(0) => panic!("{} called to many times", self.name),
                Some(ref mut times) => { *times -= 1; },
                None => (),
            }

            // Since we add assertions in drop implementation for Stub, an panic can occur in a
            // panic, cause a hard abort of the program. To handle that, we catch the panic, and
            // set the stub as invalidated so the assertions are not run during the drop.
            impl<'a, A, R> RefUnwindSafe for StubHolder<'a, A, R> {}
            struct StubHolder<'a, A, R>(&'a (dyn Fn(A) -> R + Sync + Send));

            let stub = StubHolder(self.stub.as_ref());

            match std::panic::catch_unwind(|| (stub.0)(args)) {
                Ok(r) => r,
                Err(panic) => {
                    self.invalidated = true;
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }

    #[derive(Debug, Default)]
    struct StubStore {
        inner: Arc<Mutex<HashMap<String, Box<dyn Any + Sync + Send>>>>
    }

    impl StubStore {
        pub fn insert<A: 'static, R: 'static>(&self, name: String, stub: Stub<A, R>) {
            let mut lock = self.inner.lock().unwrap();
            lock.insert(name, Box::new(stub));
        }

        pub fn get_mut<A, B>(&self, name: &str) -> Option<&mut Stub<A, B>> {
            let mut lock = self.inner.lock().unwrap();
            match lock.get_mut(name) {
                Some(s) => {
                    let s = s.as_mut() as *mut dyn Any as *mut Stub<A, B>;
                    Some(unsafe { &mut *s })
                }
                None => None,
            }
        }
    }

    pub struct StubBuilder<'a> {
        name: String,
        store: &'a StubStore,
        times: Option<usize>,
    }

    impl<'a> StubBuilder<'a> {
       #[must_use]
        pub fn times(mut self, times: usize) -> Self {
            self.times = Some(times);
            self
        }

        pub fn then<A: 'static, R: 'static>(self, f: impl Fn(A) -> R + Sync + Send + 'static) {
            let stub = Stub {
                stub: Box::new(f),
                times: self.times,
                name: self.name.clone(),
                invalidated: false,
            };

            self.store.insert(self.name, stub);
        }
    }

    /// Mocker allows to stub metod call on any struct. you can register stubs by calling
    /// `Mocker::when` and retrieve it in the proxy implementation when with `Mocker::get`.
    ///
    /// Mocker uses unsafe code to erase function types, because `Any` is too restrictive with it's
    /// requirement for all stub arguments to be static. Because of that panic inside a stub is UB,
    /// and it has been observed to crash with an illegal hardware instruction. Use with caution.
    #[derive(Debug, Default)]
    pub struct Mocker {
        store: StubStore,
    }

    impl Mocker {
        pub fn when(&self, name: &str) -> StubBuilder {
            StubBuilder {
                name: name.to_string(),
                store: &self.store,
                times: None,
            }
        }

        pub fn get<'a, A, R>(&'a self, name: &str) -> &'a mut Stub<A, R> {
            match self.store.get_mut(name) {
                Some(stub) => stub,
                None => panic!("unexpected call to {}", name),
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum MockIndex {
        Vrai(Index),
        Faux(Arc<Mocker>),
    }

    impl MockIndex {
        pub fn faux(faux: Mocker) -> Self {
            Self::Faux(Arc::new(faux))
        }

        pub fn open(
            path: impl AsRef<Path>,
            size: usize,
            update_file_store: Arc<UpdateFileStore>,
            uuid: Uuid,
            update_handler: Arc<UpdateHandler>,
        ) -> Result<Self> {
            let index = Index::open(path, size, update_file_store, uuid, update_handler)?;
            Ok(Self::Vrai(index))
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

        pub fn handle_update(&self, update: Processing) -> std::result::Result<Processed, Failed> {
            match self {
                MockIndex::Vrai(index) => index.handle_update(update),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn uuid(&self) -> Uuid {
            match self {
                MockIndex::Vrai(index) => index.uuid(),
                MockIndex::Faux(faux) => faux.get("uuid").call(()),
            }
        }

        pub fn stats(&self) -> Result<IndexStats> {
            match self {
                MockIndex::Vrai(index) => index.stats(),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn meta(&self) -> Result<IndexMeta> {
            match self {
                MockIndex::Vrai(index) => index.meta(),
                MockIndex::Faux(_) => todo!(),
            }
        }
        pub fn settings(&self) -> Result<Settings<Checked>> {
            match self {
                MockIndex::Vrai(index) => index.settings(),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn retrieve_documents<S: AsRef<str>>(
            &self,
            offset: usize,
            limit: usize,
            attributes_to_retrieve: Option<Vec<S>>,
        ) -> Result<Vec<Map<String, Value>>> {
            match self {
                MockIndex::Vrai(index) => index.retrieve_documents(offset, limit, attributes_to_retrieve),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn retrieve_document<S: AsRef<str>>(
            &self,
            doc_id: String,
            attributes_to_retrieve: Option<Vec<S>>,
        ) -> Result<Map<String, Value>> {
            match self {
                MockIndex::Vrai(index) => index.retrieve_document(doc_id, attributes_to_retrieve),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn size(&self) -> u64 {
            match self {
                MockIndex::Vrai(index) => index.size(),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
            match self {
                MockIndex::Vrai(index) => index.snapshot(path),
                MockIndex::Faux(faux) => {
                    faux.get("snapshot").call(path.as_ref())
                }
            }
        }

        pub fn inner(&self) -> &milli::Index {
            match self {
                MockIndex::Vrai(index) => index.inner(),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn update_primary_key(&self, primary_key: Option<String>) -> Result<IndexMeta> {
            match self {
                MockIndex::Vrai(index) => index.update_primary_key(primary_key),
                MockIndex::Faux(_) => todo!(),
            }
        }
        pub fn perform_search(&self, query: SearchQuery) -> Result<SearchResult> {
            match self {
                MockIndex::Vrai(index) => index.perform_search(query),
                MockIndex::Faux(_) => todo!(),
            }
        }

        pub fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
            match self {
                MockIndex::Vrai(index) => index.dump(path),
                MockIndex::Faux(_) => todo!(),
            }
        }
    }

    #[test]
    fn test_faux_index() {
        let faux = Mocker::default();
        faux
            .when("snapshot")
            .times(2)
            .then(|_: &Path| -> Result<()> {
                Ok(())
            });

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
        faux
            .when("snapshot")
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
