use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RateLimit {
    /// The name of the rate limit ("requests", "tokens", "input_tokens", "output_tokens").
    pub name: String,
    /// The maximum allowed value for the rate limit.
    pub limit: u32,
    /// The remaining value before the limit is reached.
    pub remaining: u32,
    /// Seconds until the rate limit resets.
    pub reset_seconds: f32,
}
