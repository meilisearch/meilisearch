use std::sync::Arc;

/// An `Arc<[u8]>` that is transitive over `AsRef<[u8]>`.
pub struct TransitiveArc<T>(pub Arc<T>);

impl<T: AsRef<[u8]>> AsRef<[u8]> for TransitiveArc<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}

impl<T> Clone for TransitiveArc<T> {
    fn clone(&self) -> TransitiveArc<T> {
        TransitiveArc(self.0.clone())
    }
}
