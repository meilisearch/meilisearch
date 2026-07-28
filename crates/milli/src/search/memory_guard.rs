use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct QueryMemoryGuard {
    max_bytes: usize,
    used_bytes: AtomicUsize,
}

impl QueryMemoryGuard {
    pub const fn unbounded() -> Self {
        Self { max_bytes: usize::MAX, used_bytes: AtomicUsize::new(0) }
    }

    pub const fn new(max_bytes: usize) -> Self {
        Self { max_bytes, used_bytes: AtomicUsize::new(0) }
    }

    pub fn try_consume(&self, bytes: usize) -> bool {
        if self.max_bytes == usize::MAX || bytes == 0 { return true; }
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = match current.checked_add(bytes) { Some(n) => n, None => return false };
            if next > self.max_bytes { return false; }
            match self.used_bytes.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

pub fn estimate_str_bytes(s: &str) -> usize { s.len() }

pub fn estimate_json_bytes(v: &serde_json::Value) -> usize {
    use serde_json::Value::*;
    match v {
        Null => 1,
        Bool(_) => 1,
        Number(_) => 16,
        String(s) => s.len(),
        Array(arr) => arr.iter().map(estimate_json_bytes).sum(),
        Object(map) => map.iter().map(|(k, v)| k.len() + estimate_json_bytes(v)).sum(),
    }
}
