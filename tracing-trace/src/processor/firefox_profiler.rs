use std::collections::HashMap;

use fxprof_processed_profile::{
    CategoryPairHandle, CpuDelta, Frame, FrameFlags, FrameInfo, MarkerDynamicField,
    MarkerFieldFormat, MarkerLocation, MarkerSchema, MarkerSchemaField, Profile, ProfilerMarker,
    ReferenceTimestamp, SamplingInterval, StringHandle, Timestamp,
};
use serde_json::json;

use crate::entry::{
    Entry, MemoryStats, NewCallsite, NewSpan, ResourceId, SpanClose, SpanEnter, SpanExit, SpanId,
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

    let mut current_memory = MemoryStats::default();
    let init_allocations = |profile: &mut Profile| {
        profile.add_counter(main, "mimmalloc", "Memory", "Amount of allocation calls")
    };
    let init_deallocations = |profile: &mut Profile| {
        profile.add_counter(main, "mimmalloc", "Memory", "Amount of deallocation calls")
    };
    let init_reallocations = |profile: &mut Profile| {
        profile.add_counter(main, "mimmalloc", "Memory", "Amount of reallocation calls")
    };
    let mut allocations_counter = None;
    let mut deallocations_counter = None;
    let mut reallocations_counter = None;

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

                *status = SpanStatus::Inside(time);

                last_timestamp = Timestamp::from_nanos_since_reference(time.as_nanos() as u64);

                if let Some(stats) = memory {
                    let MemoryStats {
                        allocations,
                        deallocations,
                        reallocations,
                        bytes_allocated,
                        bytes_deallocated,
                        bytes_reallocated,
                    } = current_memory - stats;

                    let counter =
                        *allocations_counter.get_or_insert_with(|| init_allocations(&mut profile));
                    profile.add_counter_sample(
                        counter,
                        last_timestamp,
                        bytes_allocated as f64,
                        allocations.try_into().unwrap(),
                    );

                    let counter = *deallocations_counter
                        .get_or_insert_with(|| init_deallocations(&mut profile));
                    profile.add_counter_sample(
                        counter,
                        last_timestamp,
                        bytes_deallocated as f64,
                        deallocations.try_into().unwrap(),
                    );

                    let counter = *reallocations_counter
                        .get_or_insert_with(|| init_reallocations(&mut profile));
                    profile.add_counter_sample(
                        counter,
                        last_timestamp,
                        bytes_reallocated as f64,
                        reallocations.try_into().unwrap(),
                    );

                    current_memory = stats;
                }
            }
            Entry::SpanExit(SpanExit { id, time, memory }) => {
                let (span, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Inside(begin) = status else {
                    continue;
                };
                last_timestamp = Timestamp::from_nanos_since_reference(time.as_nanos() as u64);

                let begin = *begin;

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

                if let Some(stats) = memory {
                    let MemoryStats {
                        allocations,
                        deallocations,
                        reallocations,
                        bytes_allocated,
                        bytes_deallocated,
                        bytes_reallocated,
                    } = current_memory - stats;

                    let counter =
                        *allocations_counter.get_or_insert_with(|| init_allocations(&mut profile));
                    profile.add_counter_sample(
                        counter,
                        last_timestamp,
                        bytes_allocated as f64,
                        allocations.try_into().unwrap(),
                    );

                    let counter = *deallocations_counter
                        .get_or_insert_with(|| init_deallocations(&mut profile));
                    profile.add_counter_sample(
                        counter,
                        last_timestamp,
                        bytes_deallocated as f64,
                        deallocations.try_into().unwrap(),
                    );

                    let counter = *reallocations_counter
                        .get_or_insert_with(|| init_reallocations(&mut profile));
                    profile.add_counter_sample(
                        counter,
                        last_timestamp,
                        bytes_reallocated as f64,
                        reallocations.try_into().unwrap(),
                    );

                    current_memory = stats;
                }

                let (callsite, _) = calls.get(&span.call_id).unwrap();

                let marker = SpanMarker { callsite, span: &span };

                profile.add_marker_with_stack(
                    *thread_handle,
                    &callsite.name,
                    marker,
                    fxprof_processed_profile::MarkerTiming::Interval(
                        to_timestamp(begin),
                        to_timestamp(time),
                    ),
                    frames.iter().rev().cloned(),
                )
            }
            Entry::SpanClose(SpanClose { id, time }) => {
                spans.remove(&id);
                last_timestamp = Timestamp::from_nanos_since_reference(time.as_nanos() as u64);
            }
        }
    }

    Ok(profile)
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
    Inside(std::time::Duration),
}

struct SpanMarker<'a> {
    span: &'a NewSpan,
    callsite: &'a NewCallsite,
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
        json!({
            "type": Self::MARKER_TYPE_NAME,
            "filename": filename,
            "line": line,
            "module_path": module_path,
            "span_id": span_id,
            "thread_id": thread_id,
        })
    }
}
