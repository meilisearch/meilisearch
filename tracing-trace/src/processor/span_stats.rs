use std::collections::{BTreeMap, HashMap};
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
}

pub fn to_call_stats<R: std::io::Read>(
    trace: TraceReader<R>,
) -> Result<BTreeMap<String, CallStats>, Error> {
    let mut calls = HashMap::new();
    let mut spans = HashMap::new();
    for entry in trace {
        let entry = entry?;
        match entry {
            Entry::NewCallsite(callsite) => {
                calls.insert(callsite.call_id, (callsite, vec![]));
            }
            Entry::NewThread(_) => {}
            Entry::NewSpan(span) => {
                spans.insert(span.id, (span, SpanStatus::Outside));
            }
            Entry::SpanEnter(SpanEnter { id, time, memory: _ }) => {
                let (_, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Outside = status else {
                    continue;
                };

                *status = SpanStatus::Inside(time);
            }
            Entry::SpanExit(SpanExit { id, time: end, memory: _ }) => {
                let (span, status) = spans.get_mut(&id).unwrap();

                let SpanStatus::Inside(begin) = status else {
                    continue;
                };
                let begin = *begin;

                *status = SpanStatus::Outside;

                let span = *span;
                let (_, call_list) = calls.get_mut(&span.call_id).unwrap();
                call_list.push(end - begin);
            }
            Entry::SpanClose(SpanClose { id, time: _ }) => {
                spans.remove(&id);
            }
            Entry::Event(_) => {}
        }
    }

    Ok(calls
        .into_iter()
        .map(|(_, (call_site, calls))| (site_to_string(call_site), calls_to_stats(calls)))
        .collect())
}

fn site_to_string(call_site: NewCallsite) -> String {
    format!("{}::{}", call_site.target, call_site.name)
}
fn calls_to_stats(calls: Vec<Duration>) -> CallStats {
    let nb = calls.len();
    let sum: Duration = calls.iter().sum();
    CallStats { call_count: nb, time: sum.as_nanos() as u64 }
}
