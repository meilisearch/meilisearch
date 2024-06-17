use std::alloc::GlobalAlloc;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use tracing_error::SpanTrace;

#[derive(Debug, Clone)]
pub struct AllocEntry {
    generation: u64,
    span: SpanTrace,
}

impl AllocEntry {
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl std::fmt::Display for AllocEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut res = Ok(());
        let mut depth = 0;
        self.span.with_spans(|metadata, fields| {
            let name_with_module_name: Vec<&str> = metadata
                .module_path()
                .into_iter()
                .chain(std::iter::once(metadata.name()))
                .collect();
            let name_with_module_name = name_with_module_name.join("::");
            let location = format!(
                "{}:{}",
                metadata.file().unwrap_or_default(),
                metadata.line().unwrap_or_default()
            );
            if let Err(error) =
                writeln!(f, "[{depth}]{name_with_module_name}({fields}) at {location}")
            {
                res = Err(error);
                return false;
            }
            depth += 1;
            true
        });
        res
    }
}

struct AllocatorState {
    is_allocating: Cell<bool>,
    state: RefCell<HashMap<*mut u8, AllocEntry>>,
}

thread_local! {
    static ALLOCATOR_STATE: AllocatorState = AllocatorState { is_allocating: Cell::new(false), state: RefCell::new(Default::default()) };
}

pub struct InspectingAllocator<InnerAllocator> {
    inner: InnerAllocator,
    current_generation: AtomicU64,
}

impl AllocatorState {
    fn handle_alloc(&self, allocated: *mut u8, current_generation: u64) -> *mut u8 {
        if self.is_allocating.get() {
            return allocated;
        }
        self.is_allocating.set(true);
        {
            self.state.borrow_mut().insert(
                allocated,
                AllocEntry { generation: current_generation, span: SpanTrace::capture() },
            );
        }
        self.is_allocating.set(false);

        allocated
    }

    fn handle_dealloc(&self, allocated: *mut u8) {
        if self.is_allocating.get() {
            return;
        }
        self.is_allocating.set(true);
        {
            self.state.borrow_mut().remove(&allocated);
        }
        self.is_allocating.set(false);
    }

    fn find_older_generations(&self, older_generation: u64) -> Vec<(*mut u8, AllocEntry)> {
        if self.is_allocating.get() {
            return Vec::new();
        }
        self.is_allocating.set(true);
        let mut entries = Vec::new();
        self.state.borrow_mut().retain(|k, v| {
            if v.generation > older_generation {
                return true;
            }
            entries.push((*k, v.clone()));
            false
        });
        self.is_allocating.set(false);
        entries
    }
}

impl<A> InspectingAllocator<A> {
    pub const fn wrap(inner: A) -> Self {
        Self { inner, current_generation: AtomicU64::new(0) }
    }

    pub fn next_generation(&self) {
        self.current_generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn find_older_generations(&self, older_than: u64) -> Vec<(*mut u8, AllocEntry)> {
        let current_generation = self.current_generation.load(std::sync::atomic::Ordering::Relaxed);
        if current_generation < older_than {
            return Vec::new();
        }
        ALLOCATOR_STATE.with(|allocator_state| {
            allocator_state.find_older_generations(current_generation - older_than)
        })
    }
}

unsafe impl<InnerAllocator: GlobalAlloc> GlobalAlloc for InspectingAllocator<InnerAllocator> {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let allocated = self.inner.alloc(layout);
        let current_generation = self.current_generation.load(std::sync::atomic::Ordering::Relaxed);
        ALLOCATOR_STATE
            .with(|allocator_state| allocator_state.handle_alloc(allocated, current_generation))
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        self.inner.dealloc(ptr, layout);
        ALLOCATOR_STATE.with(|allocator_state| allocator_state.handle_dealloc(ptr))
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        let allocated = self.inner.alloc_zeroed(layout);

        let current_generation = self.current_generation.load(std::sync::atomic::Ordering::Relaxed);
        ALLOCATOR_STATE
            .with(|allocator_state| allocator_state.handle_alloc(allocated, current_generation))
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        let reallocated = self.inner.realloc(ptr, layout, new_size);
        if reallocated == ptr {
            return reallocated;
        }
        let current_generation = self.current_generation.load(std::sync::atomic::Ordering::Relaxed);
        ALLOCATOR_STATE.with(|allocator_state| allocator_state.handle_dealloc(ptr));
        ALLOCATOR_STATE
            .with(|allocator_state| allocator_state.handle_alloc(reallocated, current_generation))
    }
}
