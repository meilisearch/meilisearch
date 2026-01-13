use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Default, Clone, Debug)]
pub struct MustStopProcessing(Arc<AtomicBool>);

impl MustStopProcessing {
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn must_stop(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}
