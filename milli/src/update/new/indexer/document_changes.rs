use std::cell::{Cell, RefCell};
use std::sync::{Arc, RwLock};

use bumpalo::Bump;
use heed::RoTxn;
use raw_collections::alloc::RefBump;
use rayon::iter::IndexedParallelIterator;

use super::super::document_change::DocumentChange;
use crate::update::new::parallel_iterator_ext::ParallelIteratorExt as _;
use crate::{FieldsIdsMap, GlobalFieldsIdsMap, Index, Result};

/// A trait for types that are **not** [`Send`] only because they would then allow concurrent access to a type that is not [`Sync`].
///
/// The primary example of such a type is `&T`, with `T: !Sync`.
///
/// In the authors' understanding, a type can be `!Send` for two distinct reasons:
///
/// 1. Because it contains data that *genuinely* cannot be moved between threads, such as thread-local data.
/// 2. Because sending the type would allow concurrent access to a `!Sync` type, which is undefined behavior.
///
/// `MostlySend` exists to be used in bounds where you need a type whose data is **not** *attached* to a thread
/// because you might access it from a different thread, but where you will never access the type **concurrently** from
/// multiple threads.
///
/// Like [`Send`], `MostlySend` assumes properties on types that cannot be verified by the compiler, which is why implementing
/// this trait is unsafe.
///
/// # Safety
///
/// Implementers of this trait promises that the following properties hold on the implementing type:
///
/// 1. Its data can be accessed from any thread and will be the same regardless of the thread accessing it.
/// 2. Any operation that can be performed on the type does not depend on the thread that executes it.
///
/// As these properties are subtle and are not generally tracked by the Rust type system, great care should be taken before
/// implementing `MostlySend` on a type, especially a foreign type.
///
/// - An example of a type that verifies (1) and (2) is [`std::rc::Rc`] (when `T` is `Send` and `Sync`).
/// - An example of a type that doesn't verify (1) is thread-local data.
/// - An example of a type that doesn't verify (2) is [`std::sync::MutexGuard`]: a lot of mutex implementations require that
/// a lock is returned to the operating system on the same thread that initially locked the mutex, failing to uphold this
/// invariant will cause Undefined Behavior
/// (see last § in [the nomicon](https://doc.rust-lang.org/nomicon/send-and-sync.html)).
///
/// It is **always safe** to implement this trait on a type that is `Send`, but no placeholder impl is provided due to limitations in
/// coherency. Use the [`FullySend`] wrapper in this situation.
pub unsafe trait MostlySend {}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FullySend<T>(pub T);

// SAFETY: a type **fully** send is always mostly send as well.
unsafe impl<T> MostlySend for FullySend<T> where T: Send {}

impl<T> FullySend<T> {
    pub fn into(self) -> T {
        self.0
    }
}

impl<T> From<T> for FullySend<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MostlySendWrapper<T>(T);

impl<T: MostlySend> MostlySendWrapper<T> {
    /// # Safety
    ///
    /// - (P1) Users of this type will never access the type concurrently from multiple threads without synchronization
    unsafe fn new(t: T) -> Self {
        Self(t)
    }

    fn as_ref(&self) -> &T {
        &self.0
    }

    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }

    fn into_inner(self) -> T {
        self.0
    }
}

/// # Safety
///
/// 1. `T` is [`MostlySend`], so by its safety contract it can be accessed by any thread and all of its operations are available
///   from any thread.
/// 2. (P1) of `MostlySendWrapper::new` forces the user to never access the value from multiple threads concurrently.
unsafe impl<T: MostlySend> Send for MostlySendWrapper<T> {}

/// A wrapper around [`thread_local::ThreadLocal`] that accepts [`MostlySend`] `T`s.
#[derive(Default)]
pub struct ThreadLocal<T: MostlySend> {
    inner: thread_local::ThreadLocal<MostlySendWrapper<T>>,
    // FIXME: this should be necessary
    //_no_send: PhantomData<*mut ()>,
}

impl<T: MostlySend> ThreadLocal<T> {
    pub fn new() -> Self {
        Self { inner: thread_local::ThreadLocal::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { inner: thread_local::ThreadLocal::with_capacity(capacity) }
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }

    pub fn get(&self) -> Option<&T> {
        self.inner.get().map(|t| t.as_ref())
    }

    pub fn get_or<F>(&self, create: F) -> &T
    where
        F: FnOnce() -> T,
    {
        self.inner.get_or(|| unsafe { MostlySendWrapper::new(create()) }).as_ref()
    }

    pub fn get_or_try<F, E>(&self, create: F) -> std::result::Result<&T, E>
    where
        F: FnOnce() -> std::result::Result<T, E>,
    {
        self.inner
            .get_or_try(|| unsafe { Ok(MostlySendWrapper::new(create()?)) })
            .map(MostlySendWrapper::as_ref)
    }

    pub fn get_or_default(&self) -> &T
    where
        T: Default,
    {
        self.inner.get_or_default().as_ref()
    }

    pub fn iter_mut(&mut self) -> IterMut<T> {
        IterMut(self.inner.iter_mut())
    }
}

impl<T: MostlySend> IntoIterator for ThreadLocal<T> {
    type Item = T;

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.inner.into_iter())
    }
}

pub struct IterMut<'a, T: MostlySend>(thread_local::IterMut<'a, MostlySendWrapper<T>>);

impl<'a, T: MostlySend> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|t| t.as_mut())
    }
}

pub struct IntoIter<T: MostlySend>(thread_local::IntoIter<MostlySendWrapper<T>>);

impl<T: MostlySend> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|t| t.into_inner())
    }
}

pub struct DocumentChangeContext<
    'doc,             // covariant lifetime of a single `process` call
    'extractor: 'doc, // invariant lifetime of the extractor_allocs
    'fid: 'doc,       // invariant lifetime of the new_fields_ids_map
    'indexer: 'doc,   // covariant lifetime of objects that outlive a single `process` call
    T: MostlySend,
> {
    /// The index we're indexing in
    pub index: &'indexer Index,
    /// The fields ids map as it was at the start of this indexing process. Contains at least all top-level fields from documents
    /// inside of the DB.
    pub db_fields_ids_map: &'indexer FieldsIdsMap,
    /// A transaction providing data from the DB before all indexing operations
    pub txn: RoTxn<'indexer>,

    /// Global field id map that is up to date with the current state of the indexing process.
    ///
    /// - Inserting a field will take a lock
    /// - Retrieving a field may take a lock as well
    pub new_fields_ids_map: &'doc std::cell::RefCell<GlobalFieldsIdsMap<'fid>>,

    /// Data allocated in this allocator is cleared between each call to `process`.
    pub doc_alloc: Bump,

    /// Data allocated in this allocator is not cleared between each call to `process`, unless the data spills.
    pub extractor_alloc: RefBump<'extractor>,

    /// Pool of doc allocators, used to retrieve the doc allocator we provided for the documents
    doc_allocs: &'doc ThreadLocal<FullySend<Cell<Bump>>>,

    /// Extractor-specific data
    pub data: &'doc T,
}

impl<
        'doc,             // covariant lifetime of a single `process` call
        'data: 'doc,      // invariant on T lifetime of the datastore
        'extractor: 'doc, // invariant lifetime of extractor_allocs
        'fid: 'doc,       // invariant lifetime of fields ids map
        'indexer: 'doc,   // covariant lifetime of objects that survive a `process` call
        T: MostlySend,
    > DocumentChangeContext<'doc, 'extractor, 'fid, 'indexer, T>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new<F>(
        index: &'indexer Index,
        db_fields_ids_map: &'indexer FieldsIdsMap,
        new_fields_ids_map: &'fid RwLock<FieldsIdsMap>,
        extractor_allocs: &'extractor ThreadLocal<FullySend<RefCell<Bump>>>,
        doc_allocs: &'doc ThreadLocal<FullySend<Cell<Bump>>>,
        datastore: &'data ThreadLocal<T>,
        fields_ids_map_store: &'doc ThreadLocal<FullySend<RefCell<GlobalFieldsIdsMap<'fid>>>>,
        init_data: F,
    ) -> Result<Self>
    where
        F: FnOnce(RefBump<'extractor>) -> Result<T>,
    {
        let doc_alloc =
            doc_allocs.get_or(|| FullySend(Cell::new(Bump::with_capacity(1024 * 1024 * 1024))));
        let doc_alloc = doc_alloc.0.take();
        let fields_ids_map = fields_ids_map_store
            .get_or(|| RefCell::new(GlobalFieldsIdsMap::new(new_fields_ids_map)).into());

        let fields_ids_map = &fields_ids_map.0;
        let extractor_alloc = extractor_allocs.get_or_default();

        let extractor_alloc = RefBump::new(extractor_alloc.0.borrow());

        let data = datastore.get_or_try(|| init_data(RefBump::clone(&extractor_alloc)))?;

        let txn = index.read_txn()?;
        Ok(DocumentChangeContext {
            index,
            txn,
            db_fields_ids_map,
            new_fields_ids_map: fields_ids_map,
            doc_alloc,
            extractor_alloc,
            data,
            doc_allocs,
        })
    }
}

/// An internal iterator (i.e. using `foreach`) of `DocumentChange`s
pub trait Extractor<'extractor>: Sync {
    type Data: MostlySend;

    fn init_data<'doc>(&'doc self, extractor_alloc: RefBump<'extractor>) -> Result<Self::Data>;

    fn process<'doc>(
        &'doc self,
        change: DocumentChange<'doc>,
        context: &'doc DocumentChangeContext<Self::Data>,
    ) -> Result<()>;
}

pub trait DocumentChanges<'pl // lifetime of the underlying payload
>: Sync {
    type Item: Send;

    fn iter(&self) -> impl IndexedParallelIterator<Item = Self::Item>;

    fn item_to_document_change<'doc, // lifetime of a single `process` call
     T: MostlySend>(
        &'doc self,
        context: &'doc DocumentChangeContext<T>,
        item: Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>> where 'pl: 'doc // the payload must survive the process calls
    ;
}

#[derive(Clone, Copy)]
pub struct IndexingContext<
    'fid,     // invariant lifetime of fields ids map
    'indexer, // covariant lifetime of objects that are borrowed  during the entire indexing operation
    'index,   // covariant lifetime of the index
> {
    pub index: &'index Index,
    pub db_fields_ids_map: &'indexer FieldsIdsMap,
    pub new_fields_ids_map: &'fid RwLock<FieldsIdsMap>,
    pub doc_allocs: &'indexer ThreadLocal<FullySend<Cell<Bump>>>,
    pub fields_ids_map_store: &'indexer ThreadLocal<FullySend<RefCell<GlobalFieldsIdsMap<'fid>>>>,
}

pub fn for_each_document_change<
    'pl,        // covariant lifetime of the underlying payload
    'extractor, // invariant lifetime of extractor_alloc
    'fid,       // invariant lifetime of fields ids map
    'indexer,   // covariant lifetime of objects that are borrowed during the entire indexing
    'data,      // invariant on EX::Data lifetime of datastore
    'index,     // covariant lifetime of the index
    EX,
    DC: DocumentChanges<'pl>,
>(
    document_changes: &DC,
    extractor: &EX,
    IndexingContext {
        index,
        db_fields_ids_map,
        new_fields_ids_map,
        doc_allocs,
        fields_ids_map_store,
    }: IndexingContext<'fid, 'indexer, 'index>,
    extractor_allocs: &'extractor mut ThreadLocal<FullySend<RefCell<Bump>>>,
    datastore: &'data ThreadLocal<EX::Data>,
) -> Result<()>
where
    EX: Extractor<'extractor>,
{
    // Clean up and reuse the extractor allocs
    for extractor_alloc in extractor_allocs.iter_mut() {
        extractor_alloc.0.get_mut().reset();
    }

    let pi = document_changes.iter();
    pi.try_arc_for_each_try_init(
        || {
            DocumentChangeContext::new(
                index,
                db_fields_ids_map,
                new_fields_ids_map,
                extractor_allocs,
                doc_allocs,
                datastore,
                fields_ids_map_store,
                move |index_alloc| extractor.init_data(index_alloc),
            )
        },
        |context, item| {
            // Clean up and reuse the document-specific allocator
            context.doc_alloc.reset();

            let Some(change) =
                document_changes.item_to_document_change(context, item).map_err(Arc::new)?
            else {
                return Ok(());
            };

            let res = extractor.process(change, context).map_err(Arc::new);

            // send back the doc_alloc in the pool
            context.doc_allocs.get_or_default().0.set(std::mem::take(&mut context.doc_alloc));

            res
        },
    )
}
