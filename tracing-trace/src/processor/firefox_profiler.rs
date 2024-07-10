use std::collections::HashMap;

use fxprof_processed_profile::{
    CategoryHandle, CategoryPairHandle, CounterHandle, CpuDelta, Frame, FrameFlags, FrameInfo,
    MarkerDynamicField, MarkerFieldFormat, MarkerLocation, MarkerSchema, MarkerSchemaField,
    ProcessHandle, Profile, ProfilerMarker, ReferenceTimestamp, SamplingInterval, StringHandle,
    Timestamp,
};
use serde_json::json;

use crate::entry::{
    Entry, Event, MemoryStats, NewCallsite, NewSpan, ResourceId, SpanClose, SpanEnter, SpanExit,
    SpanId,
};
use crate::{Error, TraceReader};

pub fn to_firefox_profile<R: std::io::Read>(
    trace: TraceReader<R>,
    app: &str,
) -> Result<Profile, Error> {
    let mut profile = Profile::new(
        app,
        ReferenceTimestamp::from_millis_since_unix_epoch(0.0),
        SamplingInterval::from_nanos(15),
    );

    let mut last_timestamp = Timestamp::from_nanos_since_reference(0);
    let main = profile.add_process(app, 0, last_timestamp);

    let mut calls = HashMap::new();
    let mut threads = HashMap::new();
    let mut spans = HashMap::new();

    let category = profile.add_category("general", fxprof_processed_profile::CategoryColor::Blue);
    let subcategory = profile.add_subcategory(category, "subcategory");

    let mut last_memory = MemoryStats::default();

    let mut memory_counters = None;

    for entry in trace {
        let entry = entry?;
        match entry {
            Entry::NewCallsite(callsite) => {
                let string_handle = profile.intern_string(callsite.name.as_ref());
                calls.insert(callsite.call_id, (callsite, string_handle));
            }
            Entry::NewThread(thread) => {
                let thread_handle = profile.add_thread(
                    main,
                    thread.thread_id.to_usize() as u32,
                    last_timestamp,
                    threads.is_empty(),
                );
                if let Some(name) = &thread.name {
                    profile.set_thread_name(thread_handle, name)
                }
                threads.insert(thread.thread_id, thread_handle);
            }
            Entry::NewSpan(span) => {
                spans.insert(span.id, (span, SpanStatus::Outside));
            }
            Entry::SpanEnter(SpanEnter { id, time, memory }) => {
                let (_span, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Outside = status else {
                    continue;
                };

                *status = SpanStatus::Inside { time, memory };

                last_timestamp = Timestamp::from_nanos_since_reference(time.as_nanos() as u64);

                add_memory_samples(
                    &mut profile,
                    main,
                    memory,
                    last_timestamp,
                    &mut memory_counters,
                    &mut last_memory,
                );
            }
            Entry::SpanExit(SpanExit { id, time, memory }) => {
                let (span, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Inside { time: begin, memory: begin_memory } = status else {
                    continue;
                };
                last_timestamp = Timestamp::from_nanos_since_reference(time.as_nanos() as u64);

                let begin = *begin;
                let begin_memory = *begin_memory;

                *status = SpanStatus::Outside;

                let span = *span;
                let thread_handle = threads.get(&span.thread_id).unwrap();

                let frames = make_frames(span, &spans, &calls, subcategory);

                profile.add_sample(
                    *thread_handle,
                    to_timestamp(begin),
                    frames.iter().rev().cloned(),
                    CpuDelta::ZERO,
                    1,
                );
                profile.add_sample(
                    *thread_handle,
                    to_timestamp(time),
                    frames.iter().rev().cloned(),
                    CpuDelta::from_nanos((time - begin).as_nanos() as u64),
                    1,
                );

                add_memory_samples(
                    &mut profile,
                    main,
                    memory,
                    last_timestamp,
                    &mut memory_counters,
                    &mut last_memory,
                );

                let (callsite, _) = calls.get(&span.call_id).unwrap();

                let memory_delta =
                    begin_memory.zip(memory).and_then(|(begin, end)| end.checked_sub(begin));
                let marker = SpanMarker { callsite, span: &span, memory_delta };

                profile.add_marker_with_stack(
                    *thread_handle,
                    CategoryHandle::OTHER,
                    &callsite.name,
                    marker,
                    fxprof_processed_profile::MarkerTiming::Interval(
                        to_timestamp(begin),
                        to_timestamp(time),
                    ),
                    frames.iter().rev().cloned(),
                )
            }
            Entry::Event(event) => {
                let span = event
                    .parent_id
                    .as_ref()
                    .and_then(|parent_id| spans.get(parent_id))
                    .and_then(|(span, status)| match status {
                        SpanStatus::Outside => None,
                        SpanStatus::Inside { .. } => Some(span),
                    })
                    .copied();
                let timestamp = to_timestamp(event.time);

                let thread_handle = threads.get(&event.thread_id).unwrap();

                let frames = span
                    .map(|span| make_frames(span, &spans, &calls, subcategory))
                    .unwrap_or_default();

                profile.add_sample(
                    *thread_handle,
                    timestamp,
                    frames.iter().rev().cloned(),
                    CpuDelta::ZERO,
                    1,
                );

                let memory_delta = add_memory_samples(
                    &mut profile,
                    main,
                    event.memory,
                    last_timestamp,
                    &mut memory_counters,
                    &mut last_memory,
                );

                let (callsite, _) = calls.get(&event.call_id).unwrap();

                let marker = EventMarker { callsite, event: &event, memory_delta };

                profile.add_marker_with_stack(
                    *thread_handle,
                    CategoryHandle::OTHER,
                    &callsite.name,
                    marker,
                    fxprof_processed_profile::MarkerTiming::Instant(timestamp),
                    frames.iter().rev().cloned(),
                );

                last_timestamp = timestamp;
            }
            Entry::SpanClose(SpanClose { id, time }) => {
                spans.remove(&id);
                last_timestamp = to_timestamp(time);
            }
        }
    }

    Ok(profile)
}

struct MemoryCounterHandles {
    usage: CounterHandle,
}

impl MemoryCounterHandles {
    fn new(profile: &mut Profile, main: ProcessHandle) -> Self {
        let usage =
            profile.add_counter(main, "mimmalloc", "Memory", "Amount of memory currently in use");
        Self { usage }
    }
}

fn add_memory_samples(
    profile: &mut Profile,
    main: ProcessHandle,
    memory: Option<MemoryStats>,
    last_timestamp: Timestamp,
    memory_counters: &mut Option<MemoryCounterHandles>,
    last_memory: &mut MemoryStats,
) -> Option<MemoryStats> {
    let stats = memory?;

    let memory_counters =
        memory_counters.get_or_insert_with(|| MemoryCounterHandles::new(profile, main));

    profile.add_counter_sample(
        memory_counters.usage,
        last_timestamp,
        stats.resident as f64 - last_memory.resident as f64,
        0,
    );

    let delta = stats.checked_sub(*last_memory);
    *last_memory = stats;
    delta
}

fn to_timestamp(time: std::time::Duration) -> Timestamp {
    Timestamp::from_nanos_since_reference(time.as_nanos() as u64)
}

fn make_frames(
    span: NewSpan,
    spans: &HashMap<SpanId, (NewSpan, SpanStatus)>,
    calls: &HashMap<ResourceId, (NewCallsite, StringHandle)>,
    subcategory: CategoryPairHandle,
) -> Vec<FrameInfo> {
    let mut frames = Vec::new();
    let mut current_span = span;
    loop {
        let frame = make_frame(current_span, calls, subcategory);
        frames.push(frame);
        if let Some(parent) = current_span.parent_id {
            current_span = spans.get(&parent).unwrap().0;
        } else {
            break;
        }
    }
    frames
}

fn make_frame(
    span: NewSpan,
    calls: &HashMap<ResourceId, (NewCallsite, StringHandle)>,
    subcategory: CategoryPairHandle,
) -> FrameInfo {
    let (_, call) = calls.get(&span.call_id).unwrap();
    FrameInfo { frame: Frame::Label(*call), category_pair: subcategory, flags: FrameFlags::empty() }
}

#[derive(Debug, Clone, Copy)]
enum SpanStatus {
    Outside,
    Inside { time: std::time::Duration, memory: Option<MemoryStats> },
}

struct SpanMarker<'a> {
    span: &'a NewSpan,
    callsite: &'a NewCallsite,
    memory_delta: Option<MemoryStats>,
}

impl<'a> ProfilerMarker for SpanMarker<'a> {
    const MARKER_TYPE_NAME: &'static str = "span";

    fn schema() -> MarkerSchema {
        let fields = vec![
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "filename",
                label: "File name",
                format: MarkerFieldFormat::FilePath,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "line",
                label: "Line",
                format: MarkerFieldFormat::Integer,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "module_path",
                label: "Module path",
                format: MarkerFieldFormat::String,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "span_id",
                label: "Span ID",
                format: MarkerFieldFormat::Integer,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "thread_id",
                label: "Thread ID",
                format: MarkerFieldFormat::Integer,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "resident",
                label: "Resident set size, measured in bytes while this function was executing",
                format: MarkerFieldFormat::Bytes,
                searchable: false,
            }),
        ];

        MarkerSchema {
            type_name: Self::MARKER_TYPE_NAME,
            locations: vec![
                MarkerLocation::MarkerTable,
                MarkerLocation::MarkerChart,
                MarkerLocation::TimelineOverview,
            ],
            chart_label: None,
            tooltip_label: Some("{marker.name} - {marker.data.filename}:{marker.data.line}"),
            table_label: Some("{marker.data.filename}:{marker.data.line}"),
            fields,
        }
    }

    fn json_marker_data(&self) -> serde_json::Value {
        let filename = self.callsite.file.as_deref();
        let line = self.callsite.line;
        let module_path = self.callsite.module_path.as_deref();
        let span_id = self.span.id;
        let thread_id = self.span.thread_id;

        let mut value = json!({
            "type": Self::MARKER_TYPE_NAME,
            "filename": filename,
            "line": line,
            "module_path": module_path,
            "span_id": span_id,
            "thread_id": thread_id,
        });

        if let Some(MemoryStats { resident }) = self.memory_delta {
            value["resident"] = json!(resident);
        }

        value
    }
}

struct EventMarker<'a> {
    event: &'a Event,
    callsite: &'a NewCallsite,
    memory_delta: Option<MemoryStats>,
}

impl<'a> ProfilerMarker for EventMarker<'a> {
    const MARKER_TYPE_NAME: &'static str = "tracing-event";

    fn schema() -> MarkerSchema {
        let fields = vec![
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "filename",
                label: "File name",
                format: MarkerFieldFormat::FilePath,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "line",
                label: "Line",
                format: MarkerFieldFormat::Integer,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "module_path",
                label: "Module path",
                format: MarkerFieldFormat::String,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "parent_span_id",
                label: "Parent Span ID",
                format: MarkerFieldFormat::Integer,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "thread_id",
                label: "Thread ID",
                format: MarkerFieldFormat::Integer,
                searchable: true,
            }),
            MarkerSchemaField::Dynamic(MarkerDynamicField {
                key: "resident",
                label: "Resident set size, measured in bytes while this function was executing",
                format: MarkerFieldFormat::Bytes,
                searchable: false,
            }),
        ];

        MarkerSchema {
            type_name: Self::MARKER_TYPE_NAME,
            locations: vec![
                MarkerLocation::MarkerTable,
                MarkerLocation::MarkerChart,
                MarkerLocation::TimelineOverview,
            ],
            chart_label: None,
            tooltip_label: Some("{marker.name} - {marker.data.filename}:{marker.data.line}"),
            table_label: Some("{marker.data.filename}:{marker.data.line}"),
            fields,
        }
    }

    fn json_marker_data(&self) -> serde_json::Value {
        let filename = self.callsite.file.as_deref();
        let line = self.callsite.line;
        let module_path = self.callsite.module_path.as_deref();
        let span_id = self.event.parent_id;
        let thread_id = self.event.thread_id;

        let mut value = json!({
            "type": Self::MARKER_TYPE_NAME,
            "filename": filename,
            "line": line,
            "module_path": module_path,
            "parent_span_id": span_id,
            "thread_id": thread_id,
        });

        if let Some(MemoryStats { resident }) = self.memory_delta {
            value["resident"] = json!(resident);
        }

        value
    }
}
