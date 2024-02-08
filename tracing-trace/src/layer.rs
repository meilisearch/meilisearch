use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use std::ops::ControlFlow;
use std::sync::RwLock;

use tracing::span::{Attributes, Id as TracingId};
use tracing::{Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use crate::entry::{
    Entry, Event, MemoryStats, NewCallsite, NewSpan, NewThread, ResourceId, SpanClose, SpanEnter,
    SpanExit, SpanId,
};
use crate::{Error, Trace, TraceWriter};

/// Layer that measures the time spent in spans.
pub struct TraceLayer {
    sender: tokio::sync::mpsc::UnboundedSender<Entry>,
    callsites: RwLock<HashMap<OpaqueIdentifier, ResourceId>>,
    start_time: std::time::Instant,
    profile_memory: bool,
}

impl Trace {
    pub fn new(profile_memory: bool) -> (Self, TraceLayer) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let trace = Trace { receiver };
        let layer = TraceLayer {
            sender,
            callsites: Default::default(),
            start_time: std::time::Instant::now(),
            profile_memory,
        };
        (trace, layer)
    }
}

impl<W: Write> TraceWriter<W> {
    pub fn new(writer: W, profile_memory: bool) -> (Self, TraceLayer) {
        let (trace, layer) = Trace::new(profile_memory);
        (trace.into_writer(writer), layer)
    }

    pub async fn receive(&mut self) -> Result<ControlFlow<(), ()>, Error> {
        let Some(entry) = self.receiver.recv().await else {
            return Ok(ControlFlow::Break(()));
        };
        self.write(entry)?;
        Ok(ControlFlow::Continue(()))
    }

    /// Panics if called from an asynchronous context
    pub fn blocking_receive(&mut self) -> Result<ControlFlow<(), ()>, Error> {
        let Some(entry) = self.receiver.blocking_recv() else {
            return Ok(ControlFlow::Break(()));
        };
        self.write(entry)?;
        Ok(ControlFlow::Continue(()))
    }

    pub fn write(&mut self, entry: Entry) -> Result<(), Error> {
        Ok(serde_json::ser::to_writer(&mut self.writer, &entry)?)
    }

    pub fn try_receive(&mut self) -> Result<ControlFlow<(), ()>, Error> {
        let Ok(entry) = self.receiver.try_recv() else {
            return Ok(ControlFlow::Break(()));
        };
        self.write(entry)?;
        Ok(ControlFlow::Continue(()))
    }

    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()
    }
}

#[derive(PartialEq, Eq, Hash)]
enum OpaqueIdentifier {
    Thread(std::thread::ThreadId),
    Call(tracing::callsite::Identifier),
}

impl TraceLayer {
    fn resource_id(&self, opaque: OpaqueIdentifier) -> Option<ResourceId> {
        self.callsites.read().unwrap().get(&opaque).copied()
    }

    fn register_resource_id(&self, opaque: OpaqueIdentifier) -> ResourceId {
        let mut map = self.callsites.write().unwrap();
        let len = map.len();
        *map.entry(opaque).or_insert(ResourceId(len))
    }

    fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    fn memory_stats(&self) -> Option<MemoryStats> {
        if self.profile_memory {
            MemoryStats::fetch()
        } else {
            None
        }
    }

    fn send(&self, entry: Entry) {
        // we never care that the other end hanged on us
        let _ = self.sender.send(entry);
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> ResourceId {
        let call_id = self.register_resource_id(OpaqueIdentifier::Call(metadata.callsite()));

        let module_path = metadata.module_path();
        let file = metadata.file();
        let line = metadata.line();
        let name = metadata.name();
        let target = metadata.target();

        self.send(Entry::NewCallsite(NewCallsite {
            call_id,
            module_path: module_path.map(Cow::Borrowed),
            file: file.map(Cow::Borrowed),
            line,
            name: Cow::Borrowed(name),
            target: Cow::Borrowed(target),
        }));
        call_id
    }

    fn register_thread(&self) -> ResourceId {
        let thread_id = std::thread::current().id();
        let name = std::thread::current().name().map(ToOwned::to_owned);
        let thread_id = self.register_resource_id(OpaqueIdentifier::Thread(thread_id));
        self.send(Entry::NewThread(NewThread { thread_id, name }));
        thread_id
    }
}

impl<S> Layer<S> for TraceLayer
where
    S: Subscriber,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &TracingId, _ctx: Context<'_, S>) {
        let call_id = self
            .resource_id(OpaqueIdentifier::Call(attrs.metadata().callsite()))
            .unwrap_or_else(|| self.register_callsite(attrs.metadata()));

        let thread_id = self
            .resource_id(OpaqueIdentifier::Thread(std::thread::current().id()))
            .unwrap_or_else(|| self.register_thread());

        let parent_id = attrs
            .parent()
            .cloned()
            .or_else(|| tracing::Span::current().id())
            .map(|id| SpanId::from(&id));

        self.send(Entry::NewSpan(NewSpan { id: id.into(), call_id, parent_id, thread_id }));
    }

    fn on_enter(&self, id: &TracingId, _ctx: Context<'_, S>) {
        self.send(Entry::SpanEnter(SpanEnter {
            id: id.into(),
            time: self.elapsed(),
            memory: self.memory_stats(),
        }))
    }

    fn on_exit(&self, id: &TracingId, _ctx: Context<'_, S>) {
        self.send(Entry::SpanExit(SpanExit {
            id: id.into(),
            time: self.elapsed(),
            memory: self.memory_stats(),
        }))
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let call_id = self
            .resource_id(OpaqueIdentifier::Call(event.metadata().callsite()))
            .unwrap_or_else(|| self.register_callsite(event.metadata()));

        let thread_id = self
            .resource_id(OpaqueIdentifier::Thread(std::thread::current().id()))
            .unwrap_or_else(|| self.register_thread());

        let parent_id = event
            .parent()
            .cloned()
            .or_else(|| tracing::Span::current().id())
            .map(|id| SpanId::from(&id));

        self.send(Entry::Event(Event {
            call_id,
            thread_id,
            parent_id,
            time: self.elapsed(),
            memory: self.memory_stats(),
        }))
    }

    fn on_close(&self, id: TracingId, _ctx: Context<'_, S>) {
        self.send(Entry::SpanClose(SpanClose { id: Into::into(&id), time: self.elapsed() }))
    }
}
