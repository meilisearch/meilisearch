use std::collections::BinaryHeap;

use serde_json::{json, Value};

use crate::analytics::Aggregate;

#[derive(Default)]
pub struct ChatCompletionAggregator {
    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // chat completion specific metrics
    total_messages: usize,
    total_streamed_requests: usize,
    total_non_streamed_requests: usize,

    // model usage tracking
    models_used: std::collections::HashMap<String, usize>,
}

impl ChatCompletionAggregator {
    pub fn from_request(model: &str, message_count: usize, is_stream: bool) -> Self {
        let mut models_used = std::collections::HashMap::new();
        models_used.insert(model.to_string(), 1);

        Self {
            total_received: 1,
            total_succeeded: 0,
            time_spent: BinaryHeap::new(),

            total_messages: message_count,
            total_streamed_requests: if is_stream { 1 } else { 0 },
            total_non_streamed_requests: if is_stream { 0 } else { 1 },

            models_used,
        }
    }

    pub fn succeed(&mut self, time_spent: std::time::Duration) {
        self.total_succeeded += 1;
        self.time_spent.push(time_spent.as_millis() as usize);
    }
}

impl Aggregate for ChatCompletionAggregator {
    fn event_name(&self) -> &'static str {
        "Chat Completion POST"
    }

    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        let Self {
            total_received,
            total_succeeded,
            mut time_spent,
            total_messages,
            total_streamed_requests,
            total_non_streamed_requests,
            models_used,
            ..
        } = *new;

        // Aggregate time spent
        self.time_spent.append(&mut time_spent);

        // Aggregate counters
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);
        self.total_messages = self.total_messages.saturating_add(total_messages);
        self.total_streamed_requests =
            self.total_streamed_requests.saturating_add(total_streamed_requests);
        self.total_non_streamed_requests =
            self.total_non_streamed_requests.saturating_add(total_non_streamed_requests);

        // Aggregate model usage
        for (model, count) in models_used {
            *self.models_used.entry(model).or_insert(0) += count;
        }

        self
    }

    fn into_event(self: Box<Self>) -> Value {
        let Self {
            total_received,
            total_succeeded,
            time_spent,
            total_messages,
            total_streamed_requests,
            total_non_streamed_requests,
            models_used,
            ..
        } = *self;

        // Compute time statistics
        let time_spent: Vec<usize> = time_spent.into_sorted_vec();
        let (max_time, min_time, avg_time) = if time_spent.is_empty() {
            (0, 0, 0)
        } else {
            let max_time = time_spent.last().unwrap_or(&0);
            let min_time = time_spent.first().unwrap_or(&0);
            let sum: usize = time_spent.iter().sum();
            let avg_time = sum / time_spent.len();
            (*max_time, *min_time, avg_time)
        };

        // Compute average messages per request
        let avg_messages_per_request =
            if total_received > 0 { total_messages as f64 / total_received as f64 } else { 0.0 };

        // Compute streaming vs non-streaming proportions
        let streaming_ratio = if total_received > 0 {
            total_streamed_requests as f64 / total_received as f64
        } else {
            0.0
        };

        json!({
            "total_received": total_received,
            "total_succeeded": total_succeeded,
            "time_spent": {
                "max": max_time,
                "min": min_time,
                "avg": avg_time
            },
            "total_messages": total_messages,
            "avg_messages_per_request": avg_messages_per_request,
            "total_streamed_requests": total_streamed_requests,
            "total_non_streamed_requests": total_non_streamed_requests,
            "streaming_ratio": streaming_ratio,
            "models_used": models_used,
        })
    }
}
