use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::entry::{Entry, NewCallsite, SpanClose, SpanEnter, SpanExit};
use crate::{Error, TraceReader};

#[derive(Debug, Clone, Copy)]
enum SpanStatus {
    Outside,
    Inside(std::time::Duration),
}

#[derive(Serialize, Deserialize)]
pub struct CallStats {
    pub call_count: usize,
    pub time: u64,
    pub self_time: u64,
}

#[derive(Debug, Default)]
pub struct SelfTime {
    child_ranges: Vec<Range<Duration>>,
}

impl SelfTime {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_child_range(&mut self, child_range: Range<Duration>) {
        self.child_ranges.push(child_range)
    }

    pub fn self_duration(&mut self, self_range: Range<Duration>) -> Duration {
        if self.child_ranges.is_empty() {
            return self_range.end - self_range.start;
        }

        // by sorting child ranges by their start time,
        // we make sure that no child will start before the last one we visited.
        self.child_ranges
            .sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
        // self duration computed by adding all the segments where the span is not executing a child
        let mut self_duration = Duration::from_nanos(0);

        // last point in time where we are certain that this span was not executing a child.
        let mut committed_point = self_range.start;

        for child_range in &self.child_ranges {
            if child_range.start > committed_point {
                // we add to the self duration the point between the end of the latest span and the beginning of the next span
                self_duration += child_range.start - committed_point;
            }
            if committed_point < child_range.end {
                // then we set ourselves to the end of the latest span
                committed_point = child_range.end;
            }
        }

        self_duration
    }
}

pub fn to_call_stats<R: std::io::Read>(
    trace: TraceReader<R>,
) -> Result<BTreeMap<String, CallStats>, Error> {
    let mut calls = HashMap::new();
    let mut spans = HashMap::new();
    let mut last_point = Duration::from_nanos(0);
    let mut first_point = None;
    let mut total_self_time = SelfTime::new();
    for entry in trace {
        let entry = entry?;
        match entry {
            Entry::NewCallsite(callsite) => {
                calls.insert(callsite.call_id, (callsite, vec![]));
            }
            Entry::NewThread(_) => {}
            Entry::NewSpan(span) => {
                spans.insert(span.id, (span, SpanStatus::Outside, SelfTime::new()));
            }
            Entry::SpanEnter(SpanEnter { id, time, memory: _ }) => {
                first_point.get_or_insert(time);
                let (_, status, _) = spans.get_mut(&id).unwrap();

                let SpanStatus::Outside = status else {
                    continue;
                };

                *status = SpanStatus::Inside(time);
            }
            Entry::SpanExit(SpanExit { id, time: end, memory: _ }) => {
                let (span, status, self_time) = spans.get_mut(&id).unwrap();

                let SpanStatus::Inside(begin) = status else {
                    continue;
                };
                let begin = *begin;

                if last_point < end {
                    last_point = end;
                }

                *status = SpanStatus::Outside;

                let self_range = begin..end;

                let self_duration = self_time.self_duration(self_range.clone());
                *self_time = SelfTime::new();

                let span = *span;
                if let Some(parent_id) = span.parent_id {
                    let Some((_, _, parent_self_time)) = spans.get_mut(&parent_id) else {
                        let (c, _) = calls.get_mut(&span.call_id).unwrap();
                        panic!("parent span not found for span: module_path: {:?}, name: {:?}, target: {:?}", c.module_path.as_deref().unwrap_or_default(), c.name, c.target);
                    };
                    parent_self_time.add_child_range(self_range.clone())
                }
                total_self_time.add_child_range(self_range);
                let (_, call_list) = calls.get_mut(&span.call_id).unwrap();
                call_list.push((end - begin, self_duration));
            }
            Entry::SpanClose(SpanClose { id, time: _ }) => {
                spans.remove(&id);
            }
            Entry::Event(_) => {}
        }
    }

    let total_self_time = first_point
        .map(|first_point| (first_point, total_self_time.self_duration(first_point..last_point)));

    Ok(calls
        .into_iter()
        .map(|(_, (call_site, calls))| (site_to_string(call_site), calls_to_stats(calls)))
        .chain(total_self_time.map(|(first_point, total_self_time)| {
            (
                "::meta::total".to_string(),
                CallStats {
                    call_count: 1,
                    time: (last_point - first_point).as_nanos() as u64,
                    self_time: total_self_time.as_nanos() as u64,
                },
            )
        }))
        .collect())
}

fn site_to_string(call_site: NewCallsite) -> String {
    format!("{}::{}", call_site.target, call_site.name)
}
fn calls_to_stats(calls: Vec<(Duration, Duration)>) -> CallStats {
    let nb = calls.len();
    let sum: Duration = calls.iter().map(|(total, _)| total).sum();
    let self_sum: Duration = calls.iter().map(|(_, self_duration)| self_duration).sum();
    CallStats { call_count: nb, time: sum.as_nanos() as u64, self_time: self_sum.as_nanos() as u64 }
}
