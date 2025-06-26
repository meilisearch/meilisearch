use crate::analytics::Aggregate;
use crate::routes::export::Export;

#[derive(Default)]
pub struct ExportAnalytics {
    total_received: usize,
    has_api_key: bool,
    sum_index_patterns: usize,
    sum_patterns_with_filter: usize,
    payload_sizes: Vec<u64>,
}

impl ExportAnalytics {
    pub fn from_export(export: &Export) -> Self {
        let Export { url: _, api_key, payload_size, indexes } = export;

        let has_api_key = api_key.is_some();
        let index_patterns_count = indexes.len();
        let patterns_with_filter_count =
            indexes.values().filter(|settings| settings.filter.is_some()).count();
        let payload_sizes =
            if let Some(crate::routes::export::ByteWithDeserr(byte_size)) = payload_size {
                vec![byte_size.as_u64()]
            } else {
                vec![]
            };

        Self {
            total_received: 1,
            has_api_key,
            sum_index_patterns: index_patterns_count,
            sum_patterns_with_filter: patterns_with_filter_count,
            payload_sizes,
        }
    }
}

impl Aggregate for ExportAnalytics {
    fn event_name(&self) -> &'static str {
        "Export Triggered"
    }

    fn aggregate(mut self: Box<Self>, other: Box<Self>) -> Box<Self> {
        self.total_received += other.total_received;
        self.has_api_key |= other.has_api_key;
        self.sum_index_patterns += other.sum_index_patterns;
        self.sum_patterns_with_filter += other.sum_patterns_with_filter;
        self.payload_sizes.extend(other.payload_sizes);
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        let avg_payload_size = if self.payload_sizes.is_empty() {
            None
        } else {
            Some(self.payload_sizes.iter().sum::<u64>() / self.payload_sizes.len() as u64)
        };

        let avg_index_patterns = if self.total_received == 0 {
            None
        } else {
            Some(self.sum_index_patterns as f64 / self.total_received as f64)
        };

        let avg_patterns_with_filter = if self.total_received == 0 {
            None
        } else {
            Some(self.sum_patterns_with_filter as f64 / self.total_received as f64)
        };

        serde_json::json!({
            "total_received": self.total_received,
            "has_api_key": self.has_api_key,
            "avg_index_patterns": avg_index_patterns,
            "avg_patterns_with_filter": avg_patterns_with_filter,
            "avg_payload_size": avg_payload_size,
        })
    }
}
