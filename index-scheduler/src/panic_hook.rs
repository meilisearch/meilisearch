//! Panic hook designed to fetch a panic from a subthread and recover it on join.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::panic::PanicInfo;
use std::sync::{Arc, RwLock};
use std::thread::{JoinHandle, ThreadId};

use backtrace::Backtrace;

// Represents a panic in a shallowy structured fashion
pub struct Panic {
    pub payload: Option<String>,
    pub location: Option<String>,
    pub thread_name: Option<String>,
    pub thread_id: ThreadId,
    pub backtrace: Backtrace,
}

/// A panic enriched with a unique id
#[derive(serde::Serialize)]
pub struct Report {
    pub id: uuid::Uuid,
    #[serde(serialize_with = "serialize_panic")]
    pub panic: Panic,
}

fn serialize_panic<S>(panic: &Panic, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::Serialize;

    panic.to_json().serialize(s)
}

impl Report {
    pub fn new(panic: Panic) -> Self {
        Self { id: uuid::Uuid::new_v4(), panic }
    }
}

impl Panic {
    pub fn to_json(&self) -> serde_json::Value {
        json::panic_to_json(self)
    }
}

mod json {
    use backtrace::{Backtrace, BacktraceFrame, BacktraceSymbol};
    use serde_json::{json, Value};

    use super::Panic;

    fn symbol_to_json(symbol: &BacktraceSymbol) -> Value {
        let address = symbol.addr().map(|addr| format!("{:p}", addr));
        let column = symbol.colno();
        let line = symbol.lineno();
        let function = symbol.name().map(|name| name.to_string());
        let filename = symbol.filename();
        json!({
            "function": function,
            "filename": filename,
            "line": line,
            "column": column,
            "address": address,
        })
    }

    fn frame_to_json(frame: &BacktraceFrame) -> Value {
        let symbols: Vec<_> = frame.symbols().iter().map(symbol_to_json).collect();
        match symbols.as_slice() {
            [] => {
                let address = format!("{:p}", frame.ip());
                json!({"address": address})
            }
            [symbol] => json!(symbol),
            symbols => json!(symbols),
        }
    }

    fn backtrace_to_json(backtrace: &Backtrace) -> Value {
        let frames: Vec<_> = backtrace.frames().iter().map(frame_to_json).collect();
        json!(frames)
    }

    pub fn panic_to_json(panic: &Panic) -> Value {
        let thread_id = format!("{:?}", panic.thread_id);
        serde_json::json!({
            "payload": panic.payload,
            "location": panic.location,
            "thread": {
                "id": thread_id,
                "name": panic.thread_name,
            },
            "backtrace": backtrace_to_json(&panic.backtrace),
        })
    }
}

struct ReportWriter(Arc<RwLock<ReportRegistry>>);

/// A FIFO queue of reports.
pub struct ReportRegistry {
    reports: std::collections::VecDeque<Report>,
}

impl ReportRegistry {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self { reports: VecDeque::with_capacity(capacity.get()) }
    }

    pub fn push(&mut self, report: Report) -> Option<Report> {
        let popped = if self.reports.len() == self.reports.capacity() {
            self.reports.pop_back()
        } else {
            None
        };
        self.reports.push_front(report);
        popped
    }

    pub fn iter(&self) -> impl Iterator<Item = &Report> {
        self.reports.iter()
    }

    pub fn find(&self, report_id: uuid::Uuid) -> Option<&Report> {
        self.iter().find(|report| report.id == report_id)
    }
}

impl ReportWriter {
    #[track_caller]
    fn write_panic(&self, panic_info: &PanicInfo<'_>) {
        let payload = panic_info
            .payload()
            .downcast_ref::<&str>()
            .map(ToString::to_string)
            .or_else(|| panic_info.payload().downcast_ref::<String>().cloned());
        let location = panic_info.location().map(|loc| {
            format!(
                "{file}:{line}:{column}",
                file = loc.file(),
                line = loc.line(),
                column = loc.column()
            )
        });

        let thread_name = std::thread::current().name().map(ToString::to_string);
        let thread_id = std::thread::current().id();
        let backtrace = backtrace::Backtrace::new();

        let panic = Panic { payload, location, thread_name, thread_id, backtrace };

        let report = Report::new(panic);

        log::error!(
            "An unexpected panic occurred on thread {name} at {location}: {payload}. See report '{report}' for details.",
            payload = report.panic.payload.as_deref().unwrap_or("Box<dyn Any>"),
            name = report.panic.thread_name.as_deref().unwrap_or("<unnamed>"),
            location = report.panic.location.as_deref().unwrap_or("<unknown>"),
            report = report.id,
        );

        if let Ok(mut registry) = self.0.write() {
            if let Some(old_report) = registry.push(report) {
                log::trace!("Forgetting report {} to make space for new report.", old_report.id)
            }
        }
    }
}

/// Reads the reports written in case of a panic.
#[derive(Clone)]
pub struct ReportReader(Arc<RwLock<ReportRegistry>>);

impl ReportReader {
    /// Installs a new global panic hook, overriding any existing hook.
    ///
    /// The hook writes any incoming panic in reports.
    /// The reports can then be read by the returned [`ReportReader`].
    pub fn install_panic_hook(capacity: NonZeroUsize) -> Self {
        let registry = Arc::new(RwLock::new(ReportRegistry::new(capacity)));
        let reader = ReportReader(registry.clone());
        let writer = ReportWriter(registry.clone());

        std::panic::set_hook(Box::new(move |panic_info| writer.write_panic(panic_info)));
        reader
    }

    /// Join the thread corresponding to the passed handle, recovering either its value
    /// or, in case the thread panicked, the id of the report corresponding to the panic.
    ///
    /// The id can be used to read the report from the [`self.registry()`].
    pub fn join_thread<T>(&self, thread: JoinHandle<T>) -> Result<T, Option<uuid::Uuid>> {
        let thread_id = thread.thread().id();
        thread.join().map_err(|_e| {
            self.0
                .read()
                .unwrap()
                .iter()
                .find(|report| report.panic.thread_id == thread_id)
                .map(|report| report.id)
        })
    }

    /// Returns a registry that can be used to read the reports written during a panic.
    pub fn registry(&self) -> Arc<RwLock<ReportRegistry>> {
        self.0.clone()
    }
}
