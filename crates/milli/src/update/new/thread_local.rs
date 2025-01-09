use std::cell::RefCell;

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
///   a lock is returned to the operating system on the same thread that initially locked the mutex, failing to uphold this
///   invariant will cause Undefined Behavior
///   (see last § in [the nomicon](https://doc.rust-lang.org/nomicon/send-and-sync.html)).
///
/// It is **always safe** to implement this trait on a type that is `Send`, but no placeholder impl is provided due to limitations in
/// coherency. Use the [`FullySend`] wrapper in this situation.
pub unsafe trait MostlySend {}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FullySend<T>(pub T);

// SAFETY: a type **fully** send is always mostly send as well.
unsafe impl<T> MostlySend for FullySend<T> where T: Send {}

unsafe impl<T> MostlySend for RefCell<T> where T: MostlySend {}

unsafe impl<T> MostlySend for Option<T> where T: MostlySend {}

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
///    from any thread.
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
