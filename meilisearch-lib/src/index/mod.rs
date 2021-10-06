pub use search::{default_crop_length, SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{apply_settings_to_builder, Checked, Facets, Settings, Unchecked};

mod dump;
pub mod error;
mod search;
pub mod update_handler;
mod updates;

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
    use std::any::Any;
    use std::collections::HashMap;
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use serde_json::{Map, Value};
    use uuid::Uuid;

    use crate::index_controller::update_file_store::UpdateFileStore;
    use crate::index_controller::updates::status::{Failed, Processed, Processing};

    use super::error::Result;
    use super::index::Index;
    use super::update_handler::UpdateHandler;
    use super::{Checked, IndexMeta, IndexStats, SearchQuery, SearchResult, Settings};

    pub struct Stub<A, R> {
        name: String,
        times: Mutex<Option<usize>>,
        stub: Box<dyn Fn(A) -> R + Sync + Send>,
        invalidated: AtomicBool,
    }

    impl<A, R> Drop for Stub<A, R> {
        fn drop(&mut self) {
            if !self.invalidated.load(Ordering::Relaxed) {
                let lock = self.times.lock().unwrap();
                if let Some(n) = *lock {
                    assert_eq!(n, 0, "{} not called enough times", self.name);
                }
            }
        }
    }

    impl<A, R> Stub<A, R> {
        fn invalidate(&self) {
            self.invalidated.store(true, Ordering::Relaxed);
        }
    }

    impl<A: UnwindSafe, R> Stub<A, R> {
        fn call(&self, args: A) -> R {
            let mut lock = self.times.lock().unwrap();
            match *lock {
                Some(0) => panic!("{} called to many times", self.name),
                Some(ref mut times) => {
                    *times -= 1;
                }
                None => (),
            }

            // Since we add assertions in the drop implementation for Stub, a panic can occur in a
            // panic, causing a hard abort of the program. To handle that, we catch the panic, and
            // set the stub as invalidated so the assertions aren't run during the drop.
            impl<'a, A, R> RefUnwindSafe for StubHolder<'a, A, R> {}
            struct StubHolder<'a, A, R>(&'a (dyn Fn(A) -> R + Sync + Send));

            let stub = StubHolder(self.stub.as_ref());

            match std::panic::catch_unwind(|| (stub.0)(args)) {
                Ok(r) => r,
                Err(panic) => {
                    self.invalidate();
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }

    #[derive(Debug, Default)]
    struct StubStore {
        inner: Arc<Mutex<HashMap<String, Box<dyn Any + Sync + Send>>>>,
    }

    impl StubStore {
        pub fn insert<A: 'static, R: 'static>(&self, name: String, stub: Stub<A, R>) {
            let mut lock = self.inner.lock().unwrap();
            lock.insert(name, Box::new(stub));
        }

        pub fn get<A, B>(&self, name: &str) -> Option<&Stub<A, B>> {
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

    pub struct StubBuilder<'a, A, R> {
        name: String,
        store: &'a StubStore,
        times: Option<usize>,
        _f: std::marker::PhantomData<fn(A) -> R>,
    }

    impl<'a, A: 'static, R: 'static> StubBuilder<'a, A, R> {
        /// Asserts the stub has been called exactly `times` times.
        #[must_use]
        pub fn times(mut self, times: usize) -> Self {
            self.times = Some(times);
            self
        }

        /// Asserts the stub has been called exactly once.
        #[must_use]
        pub fn once(mut self) -> Self {
            self.times = Some(1);
            self
        }

        /// The function that will be called when the stub is called. This needs to be called to
        /// actually build the stub and register it to the stub store.
        pub fn then(self, f: impl Fn(A) -> R + Sync + Send + 'static) {
            let times = Mutex::new(self.times);
            let stub = Stub {
                stub: Box::new(f),
                times,
                name: self.name.clone(),
                invalidated: AtomicBool::new(false),
            };

            self.store.insert(self.name, stub);
        }
    }

    /// Mocker allows to stub metod call on any struct. you can register stubs by calling
    /// `Mocker::when` and retrieve it in the proxy implementation when with `Mocker::get`.
    #[derive(Debug, Default)]
    pub struct Mocker {
        store: StubStore,
    }

    impl Mocker {
        pub fn when<A, R>(&self, name: &str) -> StubBuilder<A, R> {
            StubBuilder {
                name: name.to_string(),
                store: &self.store,
                times: None,
                _f: std::marker::PhantomData,
            }
        }

        pub fn get<A, R>(&self, name: &str) -> &Stub<A, R> {
            match self.store.get(name) {
                Some(stub) => stub,
                None => {
                    // panic here causes the stubs to get dropped, and panic in turn. To prevent
                    // that, we forget them, and let them be cleaned by the os later. This is not
                    // optimal, but is still better than nested panicks.
                    let mut stubs = self.store.inner.lock().unwrap();
                    let stubs = std::mem::take(&mut *stubs);
                    std::mem::forget(stubs);
                    panic!("unexpected call to {}", name)
                }
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
                MockIndex::Faux(faux) => faux.get("handle_update").call(update),
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
                MockIndex::Vrai(index) => {
                    index.retrieve_documents(offset, limit, attributes_to_retrieve)
                }
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
                MockIndex::Faux(faux) => faux.get("snapshot").call(path.as_ref()),
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
                MockIndex::Faux(faux) => faux.get("perform_search").call(query),
            }
        }

        pub fn dump(&self, path: impl AsRef<Path>) -> Result<()> {
            match self {
                MockIndex::Vrai(index) => index.dump(path),
                MockIndex::Faux(faux) => faux.get("dump").call(path.as_ref()),
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
