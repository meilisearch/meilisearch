use std::collections::HashMap;
use std::io::Read;

use crate::entry::{
    Entry, MemoryStats, NewCallsite, NewSpan, NewThread, ResourceId, SpanClose, SpanEnter,
    SpanExit, SpanId,
};
use crate::{Error, TraceReader};

#[derive(Debug, Clone, Copy)]
enum SpanStatus {
    Outside,
    Inside(std::time::Duration),
}

pub fn print_trace<R: Read>(trace: TraceReader<R>) -> Result<(), Error> {
    let mut calls = HashMap::new();
    let mut threads = HashMap::new();
    let mut spans = HashMap::new();
    for entry in trace {
        let entry = entry?;
        match entry {
            Entry::NewCallsite(callsite) => {
                calls.insert(callsite.call_id, callsite);
            }
            Entry::NewThread(NewThread { thread_id, name }) => {
                threads.insert(thread_id, name);
            }
            Entry::NewSpan(span) => {
                spans.insert(span.id, (span, SpanStatus::Outside));
            }
            Entry::SpanEnter(SpanEnter { id, time, memory }) => {
                let (span, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Outside = status else {
                    continue;
                };

                *status = SpanStatus::Inside(time);

                let span = *span;

                match memory {
                    Some(stats) => println!(
                        "[{}]{}::{} ({}) <-",
                        print_thread(&threads, span.thread_id),
                        print_backtrace(&spans, &calls, &span),
                        print_span(&calls, &span),
                        print_memory(stats),
                    ),
                    None => println!(
                        "[{}]{}::{} <-",
                        print_thread(&threads, span.thread_id),
                        print_backtrace(&spans, &calls, &span),
                        print_span(&calls, &span),
                    ),
                }
            }
            Entry::SpanExit(SpanExit { id, time, memory }) => {
                let (span, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Inside(begin) = status else {
                    continue;
                };
                let begin = *begin;

                *status = SpanStatus::Outside;

                let span = *span;

                match memory {
                    Some(stats) => println!(
                        "[{}]{}::{} ({}) -> {}",
                        print_thread(&threads, span.thread_id),
                        print_backtrace(&spans, &calls, &span),
                        print_span(&calls, &span),
                        print_memory(stats),
                        print_duration(time - begin),
                    ),
                    None => println!(
                        "[{}]{}::{} -> {}",
                        print_thread(&threads, span.thread_id),
                        print_backtrace(&spans, &calls, &span),
                        print_span(&calls, &span),
                        print_duration(time - begin),
                    ),
                }
            }
            Entry::SpanClose(SpanClose { id, time: _ }) => {
                spans.remove(&id);
            }
        }
    }
    Ok(())
}

fn print_thread(threads: &HashMap<ResourceId, Option<String>>, thread_id: ResourceId) -> String {
    let thread = threads.get(&thread_id).unwrap();
    let thread =
        thread.as_ref().cloned().unwrap_or_else(|| format!("ThreadId({})", thread_id.to_usize()));
    thread
}

fn print_backtrace(
    spans: &HashMap<SpanId, (NewSpan, SpanStatus)>,
    calls: &HashMap<ResourceId, NewCallsite>,
    span: &NewSpan,
) -> String {
    let mut parents = Vec::new();
    let mut current = span.parent_id;
    while let Some(current_id) = &current {
        let (span, _) = spans.get(current_id).unwrap();
        let callsite = calls.get(&span.call_id).unwrap();
        parents.push(callsite.name.clone());

        current = span.parent_id;
    }

    let x: Vec<String> = parents.into_iter().rev().map(|x| x.to_string()).collect();
    x.join("::")
}

fn print_span(calls: &HashMap<ResourceId, NewCallsite>, span: &NewSpan) -> String {
    let callsite = calls.get(&span.call_id).unwrap();
    match (callsite.file.clone(), callsite.line) {
        (Some(file), None) => format!("{} ({})", callsite.name, file),
        (Some(file), Some(line)) => format!("{} ({}:{})", callsite.name, file, line),
        _ => callsite.name.to_string(),
    }
}

fn print_duration(duration: std::time::Duration) -> String {
    if duration.as_nanos() < 1000 {
        format!("{}ns", duration.as_nanos())
    } else if duration.as_micros() < 1000 {
        format!("{}μs", duration.as_micros())
    } else if duration.as_millis() < 1000 {
        format!("{}ms", duration.as_millis())
    } else if duration.as_secs() < 120 {
        format!("{}s", duration.as_secs())
    } else if duration.as_secs_f64() / 60.0 < 60.0 {
        format!("{}min", duration.as_secs_f64() / 60.0)
    } else if duration.as_secs_f64() / 3600.0 < 8.0 {
        format!("{}h", duration.as_secs_f64() / 3600.0)
    } else {
        format!("{}d", duration.as_secs_f64() / 3600.0 / 24.0)
    }
}

fn print_memory(memory: MemoryStats) -> String {
    // Format only the total allocations in GiB, MiB, KiB, Bytes
}
