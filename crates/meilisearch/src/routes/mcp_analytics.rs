use std::collections::{BinaryHeap, HashSet};

use serde_json::{json, Value};

use crate::analytics::Aggregate;

#[derive(Default)]
pub struct McpAggregator {
    // requests
    total_received: usize,
    total_succeeded: usize,
    time_spent: BinaryHeap<usize>,

    // client usage tracking
    clients_used: HashSet<String>,
}

impl McpAggregator {
    pub fn from_client(client: String) -> Self {
        let mut clients_used = HashSet::new();
        clients_used.insert(client);
        Self { total_received: 1, total_succeeded: 0, time_spent: BinaryHeap::new(), clients_used }
    }

    pub fn without_client() -> Self {
        Self {
            total_received: 1,
            total_succeeded: 0,
            time_spent: BinaryHeap::new(),
            clients_used: HashSet::new(),
        }
    }

    pub fn succeed(&mut self, time_spent: std::time::Duration) {
        self.total_succeeded += 1;
        self.time_spent.push(time_spent.as_millis() as usize);
    }
}

impl Aggregate for McpAggregator {
    fn event_name(&self) -> &'static str {
        "MCP POST"
    }

    fn aggregate(mut self: Box<Self>, new: Box<Self>) -> Box<Self> {
        let Self { total_received, total_succeeded, mut time_spent, clients_used } = *new;

        // Aggregate time spent
        self.time_spent.append(&mut time_spent);

        // Aggregate counters
        self.total_received = self.total_received.saturating_add(total_received);
        self.total_succeeded = self.total_succeeded.saturating_add(total_succeeded);

        // Aggregate clients usage
        self.clients_used.extend(clients_used);

        self
    }

    fn into_event(self: Box<Self>) -> Value {
        let Self { total_received, total_succeeded, time_spent, clients_used } = *self;

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

        json!({
            "total_received": total_received,
            "total_succeeded": total_succeeded,
            "time_spent": {
                "max": max_time,
                "min": min_time,
                "avg": avg_time,
            },
            "clients_used": clients_used,
        })
    }
}
