// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::keys::RateLimitConfig;
use crate::rate_limiter_trait::RateLimiterTrait;

/// Tracks request counts for rate limiting in memory
#[derive(Debug, Clone)]
pub struct InMemoryRateLimiter {
    /// Map of API key ID to their request tracker
    trackers: Arc<Mutex<HashMap<Uuid, RequestTracker>>>,
}

#[derive(Debug)]
struct RequestTracker {
    /// Window start time
    window_start: Instant,
    /// Number of requests in current window
    request_count: u64,
    /// Configuration for this tracker
    config: RateLimitConfig,
}

impl InMemoryRateLimiter {
    pub fn new() -> Self {
        Self { trackers: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl Default for InMemoryRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRateLimiter {
    fn get_reset_time(&self, key_id: Uuid, config: &RateLimitConfig) -> Instant {
        let trackers = self.trackers.lock().unwrap();

        if let Some(tracker) = trackers.get(&key_id) {
            tracker.window_start + Duration::from_secs(config.window_seconds)
        } else {
            Instant::now() + Duration::from_secs(config.window_seconds)
        }
    }
}

#[async_trait]
impl RateLimiterTrait for InMemoryRateLimiter {
    async fn is_allowed(&self, key_id: Uuid, config: &RateLimitConfig) -> bool {
        let mut trackers = self.trackers.lock().unwrap();
        let now = Instant::now();

        let tracker = trackers.entry(key_id).or_insert_with(|| RequestTracker {
            window_start: now,
            request_count: 0,
            config: config.clone(),
        });

        // Check if we need to reset the window
        let window_duration = Duration::from_secs(config.window_seconds);
        if now.duration_since(tracker.window_start) >= window_duration {
            tracker.window_start = now;
            tracker.request_count = 0;
            tracker.config = config.clone();
        }

        // Check if request is allowed
        if tracker.request_count >= config.max_requests {
            false
        } else {
            tracker.request_count += 1;
            true
        }
    }

    async fn get_remaining(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        let trackers = self.trackers.lock().unwrap();

        if let Some(tracker) = trackers.get(&key_id) {
            let now = Instant::now();
            let window_duration = Duration::from_secs(config.window_seconds);

            // If window has expired, all requests are available
            if now.duration_since(tracker.window_start) >= window_duration {
                config.max_requests
            } else {
                config.max_requests.saturating_sub(tracker.request_count)
            }
        } else {
            config.max_requests
        }
    }

    async fn get_reset_unix(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        let reset_time = self.get_reset_time(key_id, config);

        // Convert Instant to Unix timestamp
        let now = Instant::now();
        let reset_duration = reset_time.duration_since(now);
        std::time::SystemTime::now()
            .checked_add(reset_duration)
            .unwrap_or(std::time::SystemTime::now())
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs()
    }

    async fn cleanup(&self) {
        let mut trackers = self.trackers.lock().unwrap();
        let now = Instant::now();

        // Remove trackers that haven't been used for more than their window duration
        trackers.retain(|_, tracker| {
            let window_duration = Duration::from_secs(tracker.config.window_seconds);
            now.duration_since(tracker.window_start) < window_duration * 2
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_allows_requests_within_limit() {
        let limiter = InMemoryRateLimiter::new();
        let key_id = Uuid::new_v4();
        let config = RateLimitConfig { max_requests: 5, window_seconds: 60 };

        // Should allow first 5 requests
        for _ in 0..5 {
            assert!(limiter.is_allowed(key_id, &config).await);
        }

        // 6th request should be denied
        assert!(!limiter.is_allowed(key_id, &config).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_remaining_requests() {
        let limiter = InMemoryRateLimiter::new();
        let key_id = Uuid::new_v4();
        let config = RateLimitConfig { max_requests: 5, window_seconds: 60 };

        assert_eq!(limiter.get_remaining(key_id, &config).await, 5);

        limiter.is_allowed(key_id, &config).await;
        assert_eq!(limiter.get_remaining(key_id, &config).await, 4);

        limiter.is_allowed(key_id, &config).await;
        assert_eq!(limiter.get_remaining(key_id, &config).await, 3);
    }
}
