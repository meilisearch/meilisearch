use std::ops::Deref;
use std::sync::Arc;
use std::fmt;

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct VecReadOnly<T> {
    inner: Arc<Vec<T>>,
    offset: usize,
    len: usize,
}

impl<T> VecReadOnly<T> {
    pub fn new(vec: Vec<T>) -> Self {
        let len = vec.len();
        Self {
            inner: Arc::new(vec),
            offset: 0,
            len: len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn range(&self, offset: usize, len: usize) -> Self {
        Self {
            inner: self.inner.clone(),
            offset: self.offset + offset,
            len: len,
        }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.inner[self.offset..self.offset + self.len]
    }
}

impl<T> Deref for VecReadOnly<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T: fmt::Debug> fmt::Debug for VecReadOnly<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}
