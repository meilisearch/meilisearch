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

/// A struct with a lot of memory allocation stats akin
/// to the `procfs::Process::StatsM` one plus the OOM score.
///
/// Note that all the values are in bytes not in pages.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Resident set size, measured in bytes.
    /// (same as VmRSS in /proc/<pid>/status).
    pub resident: u64,
    /// Number of resident shared bytes (i.e., backed by a file).
    /// (same as RssFile+RssShmem in /proc/<pid>/status).
    pub shared: u64,
    /// The current score that the kernel gives to this process
    /// for the purpose of selecting a process for the OOM-killer
    ///
    /// A higher score means that the process is more likely to be selected
    /// by the OOM-killer. The basis for this score is the amount of memory used
    /// by the process, plus other factors.
    ///
    /// (Since linux 2.6.11)
    pub oom_score: u32,
}

impl MemoryStats {
    #[cfg(target_os = "linux")]
    pub fn fetch() -> procfs::ProcResult<Self> {
        let process = procfs::process::Process::myself().unwrap();
        let procfs::process::StatM { resident, shared, .. } = process.statm()?;
        let oom_score = process.oom_score()?;
        let page_size = procfs::page_size();

        Ok(MemoryStats { resident: resident * page_size, shared: shared * page_size, oom_score })
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            resident: self.resident.checked_sub(other.resident)?,
            shared: self.shared.checked_sub(other.shared)?,
            oom_score: self.oom_score.checked_sub(other.oom_score)?,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ResourceId(pub(crate) usize);

impl ResourceId {
    pub fn to_usize(self) -> usize {
        self.0
    }
}
