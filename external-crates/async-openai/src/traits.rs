pub trait AsyncTryFrom<T>: Sized {
    /// The type returned in the event of a conversion error.
    type Error;

    /// Performs the conversion.
    fn try_from(value: T) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send;
}
