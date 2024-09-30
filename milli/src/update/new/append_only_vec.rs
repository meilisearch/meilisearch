// Code taken from <https://github.com/droundy/append-only-vec/blob/main/src/lib.rs>
// and modified in order to get a ref mut instead of the index of newly inserted items.

//! AppendOnlyVec
//!
//! This is a pretty simple type, which is a vector that you can push into and
//! receive a reference to the item you just inserted. The data structure never
//! moves an element once allocated, so you can push to the vec even while holding
//! mutable references to elements that have already been pushed.
//!
//! ### Scaling
//!
//! 1. Accessing an element is O(1), but slightly more expensive than for a
//!    standard `Vec`.
//!
//! 2. Pushing a new element amortizes to O(1), but may require allocation of a
//!    new chunk.
//!
//! ### Example
//!
//! ```
//! use append_only_vec::AppendOnlyVec;
//!
//! static V: AppendOnlyVec<String> = AppendOnlyVec::<String>::new();
//! let mut threads = Vec::new();
//! for thread_num in 0..10 {
//!     threads.push(std::thread::spawn(move || {
//!          for n in 0..100 {
//!               let s = format!("thread {} says {}", thread_num, n);
//!               let which = V.push(s.clone());
//!               assert_eq!(&which, &s);
//!          }
//!     }));
//! }
//!
//! for t in threads {
//!    t.join();
//! }
//!
//! assert_eq!(V.len(), 1000);
//! ```

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct AppendOnlyVec<T> {
    count: AtomicUsize,
    _reserved: AtomicUsize,
    data: [UnsafeCell<*mut T>; BITS_USED - 1 - 3],
}

unsafe impl<T: Send> Send for AppendOnlyVec<T> {}
unsafe impl<T: Sync + Send> Sync for AppendOnlyVec<T> {}

const BITS: usize = std::mem::size_of::<usize>() * 8;

#[cfg(target_arch = "x86_64")]
const BITS_USED: usize = 48;
#[cfg(all(not(target_arch = "x86_64"), target_pointer_width = "64"))]
const BITS_USED: usize = 64;
#[cfg(target_pointer_width = "32")]
const BITS_USED: usize = 32;

// This takes an index into a vec, and determines which data array will hold it
// (the first return value), and what the index will be into that data array
// (second return value)
//
// The ith data array holds 1<<i values.
const fn indices(i: usize) -> (u32, usize) {
    let i = i + 8;
    let bin = BITS as u32 - 1 - i.leading_zeros();
    let bin = bin - 3;
    let offset = i - bin_size(bin);
    (bin, offset)
}

const fn bin_size(array: u32) -> usize {
    (1 << 3) << array
}

#[test]
fn test_indices() {
    for i in 0..32 {
        println!("{:3}: {} {}", i, indices(i).0, indices(i).1);
    }
    let mut array = 0;
    let mut offset = 0;
    let mut index = 0;
    while index < 1000 {
        index += 1;
        offset += 1;
        if offset >= bin_size(array) {
            offset = 0;
            array += 1;
        }
        assert_eq!(indices(index), (array, offset));
    }
}

impl<T> Default for AppendOnlyVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AppendOnlyVec<T> {
    const EMPTY: UnsafeCell<*mut T> = UnsafeCell::new(ptr::null_mut());

    /// Allocate a new empty array.
    pub const fn new() -> Self {
        AppendOnlyVec {
            count: AtomicUsize::new(0),
            _reserved: AtomicUsize::new(0),
            data: [Self::EMPTY; BITS_USED - 1 - 3],
        }
    }

    /// Find the length of the array.
    #[inline]
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    fn layout(array: u32) -> std::alloc::Layout {
        std::alloc::Layout::array::<T>(bin_size(array)).unwrap()
    }

    /// Append an element to the array and get a mutable ref to it.
    ///
    /// This is notable in that it doesn't require a `&mut self`, because it
    /// does appropriate atomic synchronization.
    pub fn push(&self, val: T) -> &mut T {
        let idx = self._reserved.fetch_add(1, Ordering::Relaxed);
        let (array, offset) = indices(idx);
        let ptr = if self.len() < 1 + idx - offset {
            // We are working on a new array, which may not have been allocated...
            if offset == 0 {
                // It is our job to allocate the array! The size of the array
                // is determined in the self.layout method, which needs to be
                // consistent with the indices function.
                let layout = Self::layout(array);
                let ptr = unsafe { std::alloc::alloc(layout) } as *mut T;
                unsafe {
                    *self.data[array as usize].get() = ptr;
                }
                ptr
            } else {
                // We need to wait for the array to be allocated.
                while self.len() < 1 + idx - offset {
                    std::hint::spin_loop();
                }
                // The Ordering::Acquire semantics of self.len() ensures that
                // this pointer read will get the non-null pointer allocated
                // above.
                unsafe { *self.data[array as usize].get() }
            }
        } else {
            // The Ordering::Acquire semantics of self.len() ensures that
            // this pointer read will get the non-null pointer allocated
            // above.
            unsafe { *self.data[array as usize].get() }
        };

        // The contents of this offset are guaranteed to be unused (so far)
        // because we got the idx from our fetch_add above, and ptr is
        // guaranteed to be valid because of the loop we used above, which used
        // self.len() which has Ordering::Acquire semantics.
        unsafe { (ptr.add(offset)).write(val) };

        // Now we need to increase the size of the vec, so it can get read. We
        // use Release upon success, to ensure that the value which we wrote is
        // visible to any thread that has confirmed that the count is big enough
        // to read that element. In case of failure, we can be relaxed, since
        // we don't do anything with the result other than try again.
        while self
            .count
            .compare_exchange(idx, idx + 1, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            // This means that someone else *started* pushing before we started,
            // but hasn't yet finished. We have to wait for them to finish
            // pushing before we can update the count. Note that using a
            // spinloop here isn't really ideal, but except when allocating a
            // new array, the window between reserving space and using it is
            // pretty small, so contention will hopefully be rare, and having a
            // context switch during that interval will hopefully be vanishingly
            // unlikely.
            std::hint::spin_loop();
        }

        unsafe { &mut *ptr }
    }

    /// Convert into a standard `Vec`.
    pub fn into_vec(self) -> Vec<T> {
        let mut vec = Vec::with_capacity(self.len());

        for idx in 0..self.len() {
            let (array, offset) = indices(idx);
            // We use a Relaxed load of the pointer, because the loop above (which
            // ends before `self.len()`) should ensure that the data we want is
            // already visible, since it Acquired `self.count` which synchronizes
            // with the write in `self.push`.
            let ptr = unsafe { *self.data[array as usize].get() };

            // Copy the element value. The copy remaining in the array must not
            // be used again (i.e. make sure we do not drop it)
            let value = unsafe { ptr.add(offset).read() };

            vec.push(value);
        }

        // Prevent dropping the copied-out values by marking the count as 0 before
        // our own drop is run
        self.count.store(0, Ordering::Relaxed);

        vec
    }
}

impl<T> Debug for AppendOnlyVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendOnlyVec").field("len", &self.len()).finish()
    }
}

impl<T> Drop for AppendOnlyVec<T> {
    fn drop(&mut self) {
        // First we'll drop all the `T` in a slightly sloppy way.  FIXME this
        // could be optimized to avoid reloading the `ptr`.
        for idx in 0..self.len() {
            let (array, offset) = indices(idx);
            // We use a Relaxed load of the pointer, because the loop above (which
            // ends before `self.len()`) should ensure that the data we want is
            // already visible, since it Acquired `self.count` which synchronizes
            // with the write in `self.push`.
            let ptr = unsafe { *self.data[array as usize].get() };
            unsafe {
                ptr::drop_in_place(ptr.add(offset));
            }
        }
        // Now we will free all the arrays.
        for array in 0..self.data.len() as u32 {
            // This load is relaxed because no other thread can have a reference
            // to Self because we have a &mut self.
            let ptr = unsafe { *self.data[array as usize].get() };
            if !ptr.is_null() {
                let layout = Self::layout(array);
                unsafe { std::alloc::dealloc(ptr as *mut u8, layout) };
            } else {
                break;
            }
        }
    }
}

impl<T> IntoIterator for AppendOnlyVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_vec().into_iter()
    }
}

#[test]
fn test_parallel_pushing() {
    use std::sync::Arc;
    let v = Arc::new(AppendOnlyVec::<u64>::new());
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
    let v = Arc::into_inner(v).unwrap().into_vec();
    for thread_num in 0..N {
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

    let v = AppendOnlyVec::new();

    for _ in 0..50 {
        v.push(SafeToDrop(false));
    }

    let mut v = v.into_vec();
    assert_eq!(v.len(), 50);

    for i in v.iter_mut() {
        i.0 = true;
    }
}

#[test]
fn test_push_then_index_mut() {
    let v = AppendOnlyVec::<usize>::new();
    for i in 0..1024 {
        *v.push(i) += 1;
    }

    let v = v.into_vec();
    for i in 0..1024 {
        assert_eq!(v[i], 2 * i);
    }
}
