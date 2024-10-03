use std::sync::atomic::AtomicPtr;
use std::{fmt, mem};

/// An append-only linked-list that returns a mutable references to the pushed items.
pub struct AppendOnlyLinkedList<T> {
    head: AtomicPtr<Node<T>>,
}

struct Node<T> {
    item: T,
    parent: AtomicPtr<Node<T>>,
}

impl<T> AppendOnlyLinkedList<T> {
    /// Creates an empty list.
    pub fn new() -> AppendOnlyLinkedList<T> {
        AppendOnlyLinkedList { head: AtomicPtr::default() }
    }

    /// Pushes the item at the front of the linked-list and returns a unique and mutable reference to it.
    #[allow(clippy::mut_from_ref)] // the mut ref is derived from T and unique each time
    pub fn push(&self, item: T) -> &mut T {
        use std::sync::atomic::Ordering::{Relaxed, SeqCst};

        let node = Box::leak(Box::new(Node { item, parent: AtomicPtr::default() }));

        let mut head = self.head.load(SeqCst);
        loop {
            std::hint::spin_loop();
            match self.head.compare_exchange_weak(head, node, SeqCst, Relaxed) {
                Ok(parent) => {
                    node.parent = AtomicPtr::new(parent);
                    break;
                }
                Err(new) => head = new,
            }
        }

        &mut node.item
    }
}

impl<T> Default for AppendOnlyLinkedList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for AppendOnlyLinkedList<T> {
    fn drop(&mut self) {
        // Let's use the drop implementation of the IntoIter struct
        IntoIter(mem::take(&mut self.head));
    }
}

impl<T> fmt::Debug for AppendOnlyLinkedList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendOnlyLinkedList").finish()
    }
}

impl<T> IntoIterator for AppendOnlyLinkedList<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(mut self) -> Self::IntoIter {
        IntoIter(mem::take(&mut self.head))
    }
}

pub struct IntoIter<T>(AtomicPtr<Node<T>>);

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let ptr = *self.0.get_mut();
        if ptr.is_null() {
            None
        } else {
            let node = unsafe { Box::from_raw(ptr) };
            // Let's set the next node to read to be the parent of this one
            self.0 = node.parent;
            Some(node.item)
        }
    }
}

impl<T> Drop for IntoIter<T> {
    fn drop(&mut self) {
        let mut ptr = *self.0.get_mut();
        while !ptr.is_null() {
            let mut node = unsafe { Box::from_raw(ptr) };
            // Let's set the next node to read to be the parent of this one
            ptr = *node.parent.get_mut();
        }
    }
}

#[test]
fn test_parallel_pushing() {
    use std::sync::Arc;
    let v = Arc::new(AppendOnlyLinkedList::<u64>::new());
    let mut threads = Vec::new();
    const N: u64 = 100;
    for thread_num in 0..N {
        let v = v.clone();
        threads.push(std::thread::spawn(move || {
            let which1 = v.push(thread_num);
            let which2 = v.push(thread_num);
            assert_eq!(*which1, thread_num);
            assert_eq!(*which2, thread_num);
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    let v = Arc::into_inner(v).unwrap().into_iter().collect::<Vec<_>>();
    for thread_num in (0..N).rev() {
        assert_eq!(2, v.iter().copied().filter(|&x| x == thread_num).count());
    }
}

#[test]
fn test_into_vec() {
    struct SafeToDrop(bool);

    impl Drop for SafeToDrop {
        fn drop(&mut self) {
            assert!(self.0);
        }
    }

    let v = AppendOnlyLinkedList::new();

    for _ in 0..50 {
        v.push(SafeToDrop(false));
    }

    let mut v = v.into_iter().collect::<Vec<_>>();
    assert_eq!(v.len(), 50);

    for i in v.iter_mut() {
        i.0 = true;
    }
}

#[test]
fn test_push_then_index_mut() {
    let v = AppendOnlyLinkedList::<usize>::new();
    let mut w = Vec::new();
    for i in 0..1024 {
        *v.push(i) += 1;
        w.push(i + 1);
    }

    let mut v = v.into_iter().collect::<Vec<_>>();
    v.reverse();
    assert_eq!(v, w);
}
