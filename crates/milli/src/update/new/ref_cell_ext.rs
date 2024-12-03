use std::cell::{RefCell, RefMut};

pub trait RefCellExt<T: ?Sized> {
    fn try_borrow_mut_or_yield(
        &self,
    ) -> std::result::Result<RefMut<'_, T>, std::cell::BorrowMutError>;

    #[track_caller]
    fn borrow_mut_or_yield(&self) -> RefMut<'_, T> {
        self.try_borrow_mut_or_yield().unwrap()
    }
}

impl<T: ?Sized> RefCellExt<T> for RefCell<T> {
    fn try_borrow_mut_or_yield(
        &self,
    ) -> std::result::Result<RefMut<'_, T>, std::cell::BorrowMutError> {
        loop {
            match self.try_borrow_mut() {
                Ok(borrow) => break Ok(borrow),
                Err(error) => {
                    tracing::warn!("dynamic borrow failed, yielding to local tasks");

                    match rayon::yield_local() {
                        Some(rayon::Yield::Executed) => continue,
                        _ => return Err(error),
                    }
                }
            }
        }
    }
}
