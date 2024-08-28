use crossbeam_channel::{Receiver, Sender, TryRecvError};

/// A pool of items that can be pull and generated on demand.
pub struct ItemsPool<F, T, E>
where
    F: Fn() -> Result<T, E>,
{
    init: F,
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<F, T, E> ItemsPool<F, T, E>
where
    F: Fn() -> Result<T, E>,
{
    /// Create a new unbounded items pool with the specified function
    /// to generate items when needed.
    ///
    /// The `init` function will be invoked whenever a call to `with` requires new items.
    pub fn new(init: F) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        ItemsPool { init, sender, receiver }
    }

    /// Consumes the pool to retrieve all remaining items.
    ///
    /// This method is useful for cleaning up and managing the items once they are no longer needed.
    pub fn into_items(self) -> crossbeam_channel::IntoIter<T> {
        self.receiver.into_iter()
    }

    /// Allows running a function on an item from the pool,
    /// potentially generating a new item if the pool is empty.
    pub fn with<G, R>(&self, f: G) -> Result<R, E>
    where
        G: FnOnce(&mut T) -> Result<R, E>,
    {
        let mut item = match self.receiver.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => (self.init)()?,
            Err(TryRecvError::Disconnected) => unreachable!(),
        };

        // Run the user's closure with the retrieved item
        let result = f(&mut item);

        if let Err(e) = self.sender.send(item) {
            unreachable!("error when sending into channel {e}");
        }

        result
    }
}
