use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use tracing::span::Id as TracingId;

#[derive(Debug, Serialize, Deserialize)]
pub enum Entry {
    /// A code location was accessed for the first time
    NewCallsite(NewCallsite),

    /// A new thread was accessed
    NewThread(NewThread),

    /// A new call started
    NewSpan(NewSpan),

    /// An already in-flight call started doing work.
    ///
    /// For synchronous functions, open should always be followed immediately by enter, exit and close,
    /// but for asynchronous functions, work can suspend (exiting the span without closing it), and then
    /// later resume (entering the span again without opening it).
    ///
    /// The timer for a span only starts when the span is entered.
    SpanEnter(SpanEnter),

    /// An in-flight call suspended and paused work.
    ///
    /// For synchronous functions, exit should always be followed immediately by close,
    /// but for asynchronous functions, work can suspend and then later resume.
    ///
    /// The timer for a span pauses when the span is exited.
    SpanExit(SpanExit),

    /// A call ended
    SpanClose(SpanClose),

    /// An event occurred
    Event(Event),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SpanId(u64);

impl From<&TracingId> for SpanId {
    fn from(value: &TracingId) -> Self {
        Self(value.into_u64())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewCallsite {
    pub call_id: ResourceId,
    pub name: Cow<'static, str>,
    pub module_path: Option<Cow<'static, str>>,
    pub file: Option<Cow<'static, str>>,
    pub line: Option<u32>,
    pub target: Cow<'static, str>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewThread {
    pub thread_id: ResourceId,
    pub name: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SpanEnter {
    pub id: SpanId,
    pub time: std::time::Duration,
    pub memory: Option<MemoryStats>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SpanExit {
    pub id: SpanId,
    pub time: std::time::Duration,
    pub memory: Option<MemoryStats>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Event {
    pub call_id: ResourceId,
    pub thread_id: ResourceId,
    pub parent_id: Option<SpanId>,
    pub time: std::time::Duration,
    pub memory: Option<MemoryStats>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct NewSpan {
    pub id: SpanId,
    pub call_id: ResourceId,
    pub parent_id: Option<SpanId>,
    pub thread_id: ResourceId,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SpanClose {
    pub id: SpanId,
    pub time: std::time::Duration,
}

/// A struct with a memory allocation stat.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Resident set size, measured in bytes.
    /// (same as VmRSS in /proc/<pid>/status).
    pub resident: u64,
}

impl MemoryStats {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub fn fetch() -> Option<Self> {
        use libproc::libproc::pid_rusage::{pidrusage, RUsageInfoV0};

        match pidrusage(std::process::id() as i32) {
            Ok(RUsageInfoV0 { ri_resident_size, .. }) => {
                Some(MemoryStats { resident: ri_resident_size })
            }
            Err(_) => None, /* ignoring error to avoid spamming */
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub fn fetch() -> Option<Self> {
        None
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self { resident: self.resident.checked_sub(other.resident)? })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ResourceId(pub(crate) usize);

impl ResourceId {
    pub fn to_usize(self) -> usize {
        self.0
    }
}
