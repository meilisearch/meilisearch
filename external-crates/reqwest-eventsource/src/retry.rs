//! Helpers to handle connection delays when receiving errors

use crate::error::Error;
use std::time::Duration;

#[cfg(doc)]
use crate::event_source::{Event, EventSource};

/// Describes how an [`EventSource`] should retry on receiving an [`enum@Error`]
pub trait RetryPolicy {
    /// Submit a new retry delay based on the [`enum@Error`], last retry number and duration, if
    /// available. A policy may also return `None` if it does not want to retry
    fn retry(&self, error: &Error, last_retry: Option<(usize, Duration)>) -> Option<Duration>;

    /// Set a new reconnection time if received from an [`Event`]
    fn set_reconnection_time(&mut self, duration: Duration);
}

/// A [`RetryPolicy`] which backs off exponentially
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    /// The start of the backoff
    pub start: Duration,
    /// The factor of which to backoff by
    pub factor: f64,
    /// The maximum duration to delay
    pub max_duration: Option<Duration>,
    /// The maximum number of retries before giving up
    pub max_retries: Option<usize>,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff retry policy
    pub const fn new(
        start: Duration,
        factor: f64,
        max_duration: Option<Duration>,
        max_retries: Option<usize>,
    ) -> Self {
        Self {
            start,
            factor,
            max_duration,
            max_retries,
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn retry(&self, _error: &Error, last_retry: Option<(usize, Duration)>) -> Option<Duration> {
        if let Some((retry_num, last_duration)) = last_retry {
            if self.max_retries.is_none() || retry_num < self.max_retries.unwrap() {
                let duration = last_duration.mul_f64(self.factor);
                if let Some(max_duration) = self.max_duration {
                    Some(duration.min(max_duration))
                } else {
                    Some(duration)
                }
            } else {
                None
            }
        } else {
            Some(self.start)
        }
    }
    fn set_reconnection_time(&mut self, duration: Duration) {
        self.start = duration;
        if let Some(max_duration) = self.max_duration {
            self.max_duration = Some(max_duration.max(duration))
        }
    }
}

/// A [`RetryPolicy`] which always emits the same delay
#[derive(Debug, Clone)]
pub struct Constant {
    /// The delay to return
    pub delay: Duration,
    /// The maximum number of retries to return before giving up
    pub max_retries: Option<usize>,
}

impl Constant {
    /// Create a new constant retry policy
    pub const fn new(delay: Duration, max_retries: Option<usize>) -> Self {
        Self { delay, max_retries }
    }
}

impl RetryPolicy for Constant {
    fn retry(&self, _error: &Error, last_retry: Option<(usize, Duration)>) -> Option<Duration> {
        if let Some((retry_num, _)) = last_retry {
            if self.max_retries.is_none() || retry_num < self.max_retries.unwrap() {
                Some(self.delay)
            } else {
                None
            }
        } else {
            Some(self.delay)
        }
    }
    fn set_reconnection_time(&mut self, duration: Duration) {
        self.delay = duration;
    }
}

/// A [`RetryPolicy`] which never retries
#[derive(Debug, Clone, Copy, Default)]
pub struct Never;

impl RetryPolicy for Never {
    fn retry(&self, _error: &Error, _last_retry: Option<(usize, Duration)>) -> Option<Duration> {
        None
    }
    fn set_reconnection_time(&mut self, _duration: Duration) {}
}

/// The default [`RetryPolicy`] when initializing an [`EventSource`]
pub const DEFAULT_RETRY: ExponentialBackoff = ExponentialBackoff::new(
    Duration::from_millis(300),
    2.,
    Some(Duration::from_secs(5)),
    None,
);
