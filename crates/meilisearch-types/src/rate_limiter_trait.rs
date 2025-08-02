// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use async_trait::async_trait;
use uuid::Uuid;

use crate::keys::RateLimitConfig;

/// Rate limit status information for HTTP headers
#[derive(Debug, Clone)]
pub struct RateLimitInfo {
    /// Maximum requests allowed in the window
    pub limit: u64,
    /// Remaining requests in current window
    pub remaining: u64,
    /// Unix timestamp when the rate limit window resets
    pub reset_unix: u64,
}

/// Trait for rate limiting implementations
#[async_trait]
pub trait RateLimiterTrait: Send + Sync {
    /// Check if a request is allowed for the given API key
    async fn is_allowed(&self, key_id: Uuid, config: &RateLimitConfig) -> bool;

    /// Get the remaining requests for a key
    async fn get_remaining(&self, key_id: Uuid, config: &RateLimitConfig) -> u64;

    /// Get the reset time for a key's rate limit window (as Unix timestamp)
    async fn get_reset_unix(&self, key_id: Uuid, config: &RateLimitConfig) -> u64;

    /// Get rate limit information for HTTP headers
    async fn get_rate_limit_info(&self, key_id: Uuid, config: &RateLimitConfig) -> RateLimitInfo {
        let remaining = self.get_remaining(key_id, config).await;
        let reset_unix = self.get_reset_unix(key_id, config).await;

        RateLimitInfo { limit: config.max_requests, remaining, reset_unix }
    }

    /// Clean up old trackers (optional for implementations)
    async fn cleanup(&self) {}
}
