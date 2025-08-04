// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use async_trait::async_trait;
#[cfg(feature = "redis-rate-limiter")]
use redis::aio::ConnectionManager;
#[cfg(feature = "redis-rate-limiter")]
use redis::{AsyncCommands, RedisError};
#[cfg(feature = "redis-rate-limiter")]
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::keys::RateLimitConfig;
use crate::rate_limiter_trait::{RateLimitInfo, RateLimiterTrait};

/// Redis-based rate limiter implementation
#[cfg(feature = "redis-rate-limiter")]
#[derive(Clone)]
pub struct RedisRateLimiter {
    connection: ConnectionManager,
    key_prefix: String,
}

#[cfg(feature = "redis-rate-limiter")]
impl RedisRateLimiter {
    /// Create a new Redis rate limiter
    pub async fn new(redis_url: &str, key_prefix: Option<String>) -> Result<Self, RedisError> {
        let client = redis::Client::open(redis_url)?;
        let connection = ConnectionManager::new(client).await?;

        Ok(Self {
            connection,
            key_prefix: key_prefix.unwrap_or_else(|| "meilisearch:rate_limit:".to_string()),
        })
    }

    fn get_redis_key(&self, key_id: Uuid) -> String {
        format!("{}{}", self.key_prefix, key_id)
    }
}

#[cfg(feature = "redis-rate-limiter")]
#[async_trait]
impl RateLimiterTrait for RedisRateLimiter {
    async fn is_allowed(&self, key_id: Uuid, config: &RateLimitConfig) -> bool {
        let redis_key = self.get_redis_key(key_id);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        // Use Redis sliding window implementation with Lua script for atomicity
        let lua_script = r#"
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local max_requests = tonumber(ARGV[3])
            
            -- Remove old entries outside the window
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            
            -- Count current requests in window
            local current_count = redis.call('ZCARD', key)
            
            if current_count < max_requests then
                -- Add the new request
                redis.call('ZADD', key, now, now)
                -- Set expiration for cleanup
                redis.call('EXPIRE', key, window)
                return 1
            else
                return 0
            end
        "#;

        let mut conn = self.connection.clone();
        let result: Result<i32, RedisError> = redis::Script::new(lua_script)
            .key(&redis_key)
            .arg(now)
            .arg(config.window_seconds)
            .arg(config.max_requests)
            .invoke_async(&mut conn)
            .await;

        match result {
            Ok(allowed) => allowed == 1,
            Err(_) => {
                // On error, fail open (allow the request) to avoid breaking the service
                true
            }
        }
    }

    async fn get_remaining(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        let redis_key = self.get_redis_key(key_id);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let mut conn = self.connection.clone();

        // Remove old entries and count current requests
        let _: Result<(), RedisError> =
            conn.zrembyscore(&redis_key, 0, (now - config.window_seconds) as f64).await;

        let current_count: Result<u64, RedisError> = conn.zcard(&redis_key).await;

        match current_count {
            Ok(count) => config.max_requests.saturating_sub(count),
            Err(_) => config.max_requests, // On error, return max requests
        }
    }

    async fn get_reset_unix(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        let redis_key = self.get_redis_key(key_id);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let mut conn = self.connection.clone();

        // Get the oldest entry in the sorted set
        let oldest_entry: Result<Vec<(String, f64)>, RedisError> =
            conn.zrangebyscore_limit(&redis_key, 0f64, "+inf", 0, 1).await;

        match oldest_entry {
            Ok(entries) if !entries.is_empty() => {
                // Reset time is when the oldest entry expires
                let oldest_timestamp = entries[0].1 as u64;
                oldest_timestamp + config.window_seconds
            }
            _ => {
                // No entries, reset time is end of current window
                now + config.window_seconds
            }
        }
    }

    async fn cleanup(&self) {
        // Redis handles cleanup automatically via key expiration
        // No manual cleanup needed
    }
}

/// Fallback rate limiter that can use either Redis or in-memory storage
pub struct FallbackRateLimiter {
    limiter: Box<dyn RateLimiterTrait>,
}

impl FallbackRateLimiter {
    /// Create a new fallback rate limiter
    /// Tries to use Redis if URL is provided and connection succeeds
    /// Falls back to in-memory storage otherwise
    #[cfg(feature = "redis-rate-limiter")]
    pub async fn new(redis_url: Option<String>) -> Self {
        if let Some(url) = redis_url {
            match RedisRateLimiter::new(&url, None).await {
                Ok(redis_limiter) => {
                    log::info!("Using Redis rate limiter at {}", url);
                    return Self { limiter: Box::new(redis_limiter) };
                }
                Err(e) => {
                    log::warn!(
                        "Failed to connect to Redis, falling back to in-memory rate limiter: {}",
                        e
                    );
                }
            }
        }

        log::info!("Using in-memory rate limiter");
        Self { limiter: Box::new(crate::in_memory_rate_limiter::InMemoryRateLimiter::new()) }
    }

    #[cfg(not(feature = "redis-rate-limiter"))]
    pub async fn new(_redis_url: Option<String>) -> Self {
        log::info!("Using in-memory rate limiter (Redis support not compiled)");
        Self { limiter: Box::new(crate::in_memory_rate_limiter::InMemoryRateLimiter::new()) }
    }
}

#[async_trait]
impl RateLimiterTrait for FallbackRateLimiter {
    async fn is_allowed(&self, key_id: Uuid, config: &RateLimitConfig) -> bool {
        self.limiter.is_allowed(key_id, config).await
    }

    async fn get_remaining(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        self.limiter.get_remaining(key_id, config).await
    }

    async fn get_reset_unix(&self, key_id: Uuid, config: &RateLimitConfig) -> u64 {
        self.limiter.get_reset_unix(key_id, config).await
    }

    async fn get_rate_limit_info(&self, key_id: Uuid, config: &RateLimitConfig) -> RateLimitInfo {
        self.limiter.get_rate_limit_info(key_id, config).await
    }

    async fn cleanup(&self) {
        self.limiter.cleanup().await
    }
}
