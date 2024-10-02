use std::sync::Arc;

use rayon::iter::{MapInit, ParallelIterator};

pub trait ParallelIteratorExt: ParallelIterator {
    /// Maps items based on the init function.
    ///
    /// The init function is ran only as necessary which is basically once by thread.
    fn try_map_try_init<F, INIT, T, E, R>(
        self,
        init: INIT,
        map_op: F,
    ) -> MapInit<
        Self,
        impl Fn() -> Result<T, Arc<E>> + Sync + Send + Clone,
        impl Fn(&mut Result<T, Arc<E>>, Self::Item) -> Result<R, Arc<E>> + Sync + Send + Clone,
    >
    where
        E: Send + Sync,
        F: Fn(&mut T, Self::Item) -> Result<R, E> + Sync + Send + Clone,
        INIT: Fn() -> Result<T, E> + Sync + Send + Clone,
        R: Send,
    {
        self.map_init(
            move || match init() {
                Ok(t) => Ok(t),
                Err(err) => Err(Arc::new(err)),
            },
            move |result, item| match result {
                Ok(t) => map_op(t, item).map_err(Arc::new),
                Err(err) => Err(err.clone()),
            },
        )
    }

    /// A method to run a closure of all the items and return an owned error.
    ///
    /// The init function is ran only as necessary which is basically once by thread.
    fn try_arc_for_each_try_init<F, INIT, T, E>(self, init: INIT, op: F) -> Result<(), E>
    where
        E: Send + Sync,
        F: Fn(&mut T, Self::Item) -> Result<(), Arc<E>> + Sync + Send + Clone,
        INIT: Fn() -> Result<T, E> + Sync + Send + Clone,
    {
        let result = self.try_for_each_init(
            move || match init() {
                Ok(t) => Ok(t),
                Err(err) => Err(Arc::new(err)),
            },
            move |result, item| match result {
                Ok(t) => op(t, item),
                Err(err) => Err(err.clone()),
            },
        );

        match result {
            Ok(()) => Ok(()),
            Err(err) => Err(Arc::into_inner(err).expect("the error must be only owned by us")),
        }
    }

    fn try_arc_for_each<F, E>(self, op: F) -> Result<(), E>
    where
        E: Send + Sync,
        F: Fn(Self::Item) -> Result<(), Arc<E>> + Sync + Send + Clone,
    {
        match self.try_for_each(op) {
            Ok(()) => Ok(()),
            Err(err) => Err(Arc::into_inner(err).expect("the error must be only owned by us")),
        }
    }
}

impl<T: ParallelIterator> ParallelIteratorExt for T {}
