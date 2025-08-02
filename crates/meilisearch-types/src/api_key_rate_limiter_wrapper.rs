// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::sync::Arc;
use uuid::Uuid;

use crate::keys::RateLimitConfig;
use crate::rate_limiter_trait::{RateLimitInfo, RateLimiterTrait};
use crate::redis_rate_limiter::FallbackRateLimiter;

/// Main RateLimiter wrapper that uses the trait-based implementation
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<FallbackRateLimiter>,
}

impl RateLimiter {
    /// Create a new rate limiter
    /// If MEILI_REDIS_RATE_LIMIT_URL is set, tries to use Redis
    /// Falls back to in-memory storage if Redis is not available
    pub async fn new() -> Self {
        let redis_url = std::env::var("MEILI_REDIS_RATE_LIMIT_URL").ok();

        Self { inner: Arc::new(FallbackRateLimiter::new(redis_url).await) }
    }

    /// Check if a request is allowed for the given API key (blocking version for compatibility)
    pub fn is_allowed(&self, key_id: Uuid, config: &RateLimitConfig) -> bool {
        // Create a tokio runtime for blocking context if needed
        let handle = tokio::runtime::Handle::try_current();

        if let Ok(handle) = handle {
            // We're already in a tokio runtime
            handle.block_on(self.inner.is_allowed(key_id, config))
        } else {
            // We're not in a tokio runtime, create one
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.inner.is_allowed(key_id, config))
        }
    }

    /// Check if a request is allowed for the given API key (async version)
    pub async fn is_allowed_async(&self, key_id: Uuid, config: &RateLimitConfig) -> bool {
        self.inner.is_allowed(key_id, config).await
    }

    /// Get the remaining requests for a key (blocking version for compatibility)
    pub fn get_remaining(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        let handle = tokio::runtime::Handle::try_current();

        if let Ok(handle) = handle {
            handle.block_on(self.inner.get_remaining(key_id, config))
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.inner.get_remaining(key_id, config))
        }
    }

    /// Get the remaining requests for a key (async version)
    pub async fn get_remaining_async(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        self.inner.get_remaining(key_id, config).await
    }

    /// Get the reset time for a key's rate limit window
    pub fn get_reset_time(&self, key_id: Uuid, config: &RateLimitConfig) -> std::time::Instant {
        // For compatibility, convert Unix timestamp back to Instant
        let reset_unix = self.get_reset_unix_blocking(key_id, config);
        let now_unix =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

        let duration_until_reset = reset_unix.saturating_sub(now_unix);
        std::time::Instant::now() + std::time::Duration::from_secs(duration_until_reset)
    }

    fn get_reset_unix_blocking(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        let handle = tokio::runtime::Handle::try_current();

        if let Ok(handle) = handle {
            handle.block_on(self.inner.get_reset_unix(key_id, config))
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.inner.get_reset_unix(key_id, config))
        }
    }

    /// Get rate limit information for HTTP headers (blocking version for compatibility)
    pub fn get_rate_limit_info(&self, key_id: Uuid, config: &RateLimitConfig) -> RateLimitInfo {
        let handle = tokio::runtime::Handle::try_current();

        if let Ok(handle) = handle {
            handle.block_on(self.inner.get_rate_limit_info(key_id, config))
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.inner.get_rate_limit_info(key_id, config))
        }
    }

    /// Get rate limit information for HTTP headers (async version)
    pub async fn get_rate_limit_info_async(
        &self,
        key_id: Uuid,
        config: &RateLimitConfig,
    ) -> RateLimitInfo {
        self.inner.get_rate_limit_info(key_id, config).await
    }

    /// Clean up old trackers that haven't been used recently
    pub fn cleanup(&self) {
        let handle = tokio::runtime::Handle::try_current();

        if let Ok(handle) = handle {
            handle.block_on(self.inner.cleanup())
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.inner.cleanup())
        }
    }

    /// Clean up old trackers (async version)
    pub async fn cleanup_async(&self) {
        self.inner.cleanup().await
    }
}
