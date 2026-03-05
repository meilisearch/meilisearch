use std::time::{Duration, Instant};

use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use tokio::sync::broadcast::error::RecvError;

use crate::routes::parse_barrier_header;

/// Information about a barrier timeout, used to build the 503 response.
struct BarrierTimeoutInfo {
    message: String,
    retry_after_secs: u64,
}

/// Enforce barrier constraints from the `X-Meili-Barrier` header.
///
/// If the header is absent, returns `Ok(None)` immediately.
/// If all barrier conditions are already satisfied, returns `Ok(None)`.
/// If the barrier times out, returns `Ok(Some(response))` with a 503 response.
/// The caller should return this response to the client.
pub async fn enforce_barrier(
    req: &HttpRequest,
    index_scheduler: &IndexScheduler,
    barrier_timeout: Duration,
) -> Result<Option<HttpResponse>, ResponseError> {
    let barrier = match parse_barrier_header(req)? {
        Some(b) => b,
        None => return Ok(None),
    };

    if barrier.0.is_empty() {
        return Ok(None);
    }

    // Fast path: check if all barrier conditions are already satisfied
    if is_barrier_satisfied(&barrier, index_scheduler) {
        return Ok(None);
    }

    // Estimate wait: count remaining tasks per index
    let estimated_remaining = estimate_remaining_tasks(&barrier, index_scheduler);
    if estimated_remaining == u64::MAX {
        // Index doesn't exist or something is very off — likely will timeout
        // but we still try waiting since the index might be created soon
    }

    let start = Instant::now();
    let mut rx = index_scheduler.subscribe_to_task_completions();

    loop {
        let elapsed = start.elapsed();
        if elapsed >= barrier_timeout {
            let info = build_timeout_info(&barrier, index_scheduler, barrier_timeout, start);
            return Ok(Some(barrier_timeout_response(info)));
        }

        let remaining = barrier_timeout - elapsed;

        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(_task_id)) => {
                // A task completed — recheck barrier
                if is_barrier_satisfied(&barrier, index_scheduler) {
                    return Ok(None);
                }
            }
            Ok(Err(RecvError::Lagged(_))) => {
                // We missed some messages — recheck from database (source of truth)
                if is_barrier_satisfied(&barrier, index_scheduler) {
                    return Ok(None);
                }
            }
            Ok(Err(RecvError::Closed)) => {
                // Channel closed — scheduler is shutting down
                let info = BarrierTimeoutInfo {
                    message: "Scheduler is shutting down".to_string(),
                    retry_after_secs: 1,
                };
                return Ok(Some(barrier_timeout_response(info)));
            }
            Err(_) => {
                // Timeout elapsed
                let info = build_timeout_info(&barrier, index_scheduler, barrier_timeout, start);
                return Ok(Some(barrier_timeout_response(info)));
            }
        }
    }
}

fn is_barrier_satisfied(
    barrier: &crate::routes::Barrier,
    index_scheduler: &IndexScheduler,
) -> bool {
    for &task_id in barrier.0.values() {
        match index_scheduler.is_task_finished(task_id) {
            Ok(true) => continue,
            _ => return false,
        }
    }
    true
}

fn estimate_remaining_tasks(
    barrier: &crate::routes::Barrier,
    index_scheduler: &IndexScheduler,
) -> u64 {
    let mut total = 0u64;
    for (index, &target_task_id) in &barrier.0 {
        match index_scheduler.latest_completed_task_for_index(index) {
            Ok(Some(latest)) if latest >= target_task_id => {
                // Already satisfied
            }
            Ok(Some(_latest)) => {
                // We know there are tasks remaining but can't easily count per-index
                // tasks between latest and target. Use a conservative estimate.
                total = total.saturating_add(1);
            }
            Ok(None) => {
                // No completed tasks for this index yet
                total = total.saturating_add(1);
            }
            Err(_) => {
                return u64::MAX;
            }
        }
    }
    total
}

fn build_timeout_info(
    barrier: &crate::routes::Barrier,
    index_scheduler: &IndexScheduler,
    timeout: Duration,
    start: Instant,
) -> BarrierTimeoutInfo {
    let elapsed_ms = start.elapsed().as_millis();
    let mut details = Vec::new();

    for (index, &target_task_id) in &barrier.0 {
        let latest = index_scheduler.latest_completed_task_for_index(index).ok().flatten();
        match latest {
            Some(latest_id) => {
                if latest_id >= target_task_id {
                    // This one was actually satisfied
                } else {
                    let behind = target_task_id.saturating_sub(latest_id);
                    details.push(format!(
                        "{index}={target_task_id} (latest completed: {latest_id}, {behind} tasks behind)"
                    ));
                }
            }
            None => {
                details.push(format!("{index}={target_task_id} (no completed tasks for index)"));
            }
        }
    }

    let requested: Vec<String> = barrier.0.iter().map(|(i, t)| format!("{i}={t}")).collect();

    let message = if details.is_empty() {
        format!(
            "Barrier timeout after {elapsed_ms}ms. Requested: {}. All conditions were satisfied during timeout computation.",
            requested.join(",")
        )
    } else {
        format!("Barrier timeout after {elapsed_ms}ms. Unsatisfied: {}.", details.join("; "))
    };

    // Estimate retry-after from the timeout duration itself
    let retry_after_secs = std::cmp::max(1, timeout.as_secs().div_ceil(2));

    BarrierTimeoutInfo { message, retry_after_secs }
}

fn barrier_timeout_response(info: BarrierTimeoutInfo) -> HttpResponse {
    HttpResponse::ServiceUnavailable()
        .insert_header(("Retry-After", info.retry_after_secs.to_string()))
        .json(serde_json::json!({
            "message": info.message,
            "code": "barrier_timeout",
            "type": "system",
            "link": "https://docs.meilisearch.com/errors#barrier_timeout"
        }))
}

#[cfg(test)]
mod tests {
    use actix_web::test::TestRequest;

    use crate::routes::{barrier_header_value, parse_barrier_header, SummarizedTaskView};
    use meilisearch_types::tasks::{Kind, Status};

    #[test]
    fn test_parse_barrier_header_single() {
        let req = TestRequest::default()
            .insert_header(("X-Meili-Barrier", "movies=4523"))
            .to_http_request();
        let barrier = parse_barrier_header(&req).unwrap().unwrap();
        assert_eq!(barrier.0.len(), 1);
        assert_eq!(barrier.0["movies"], 4523);
    }

    #[test]
    fn test_parse_barrier_header_multiple() {
        let req = TestRequest::default()
            .insert_header(("X-Meili-Barrier", "movies=4523,books=4520"))
            .to_http_request();
        let barrier = parse_barrier_header(&req).unwrap().unwrap();
        assert_eq!(barrier.0.len(), 2);
        assert_eq!(barrier.0["movies"], 4523);
        assert_eq!(barrier.0["books"], 4520);
    }

    #[test]
    fn test_parse_barrier_header_absent() {
        let req = TestRequest::default().to_http_request();
        let barrier = parse_barrier_header(&req).unwrap();
        assert!(barrier.is_none());
    }

    #[test]
    fn test_parse_barrier_header_malformed_missing_equals() {
        let req = TestRequest::default()
            .insert_header(("X-Meili-Barrier", "movies4523"))
            .to_http_request();
        let result = parse_barrier_header(&req);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_barrier_header_malformed_non_numeric() {
        let req = TestRequest::default()
            .insert_header(("X-Meili-Barrier", "movies=abc"))
            .to_http_request();
        let result = parse_barrier_header(&req);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_barrier_header_whitespace() {
        let req = TestRequest::default()
            .insert_header(("X-Meili-Barrier", "movies = 4523 , books = 4520"))
            .to_http_request();
        let barrier = parse_barrier_header(&req).unwrap().unwrap();
        assert_eq!(barrier.0.len(), 2);
        assert_eq!(barrier.0["movies"], 4523);
        assert_eq!(barrier.0["books"], 4520);
    }

    #[test]
    fn test_parse_barrier_header_empty_entries() {
        let req = TestRequest::default()
            .insert_header(("X-Meili-Barrier", "movies=4523,,books=4520"))
            .to_http_request();
        let barrier = parse_barrier_header(&req).unwrap().unwrap();
        assert_eq!(barrier.0.len(), 2);
        assert_eq!(barrier.0["movies"], 4523);
        assert_eq!(barrier.0["books"], 4520);
    }

    #[test]
    fn test_barrier_header_value_with_index() {
        let task = SummarizedTaskView {
            task_uid: 42,
            index_uid: Some("movies".to_string()),
            status: Status::Enqueued,
            kind: Kind::DocumentAdditionOrUpdate,
            enqueued_at: time::OffsetDateTime::now_utc(),
            custom_metadata: None,
        };
        let val = barrier_header_value(&task);
        assert_eq!(val, Some("movies=42".to_string()));
    }

    #[test]
    fn test_barrier_header_value_no_index() {
        let task = SummarizedTaskView {
            task_uid: 42,
            index_uid: None,
            status: Status::Enqueued,
            kind: Kind::DumpCreation,
            enqueued_at: time::OffsetDateTime::now_utc(),
            custom_metadata: None,
        };
        let val = barrier_header_value(&task);
        assert!(val.is_none());
    }
}
