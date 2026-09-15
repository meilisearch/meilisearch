use std::sync::LazyLock;

pub static CONNECT_SECONDS: LazyLock<u64> =
    LazyLock::new(|| fetch_or_default("MEILI_EXPERIMENTAL_PROXY_CONNECT_TIMEOUT_SECONDS", 3));

pub static BACKOFF_SECONDS: LazyLock<u64> =
    LazyLock::new(|| fetch_or_default("MEILI_EXPERIMENTAL_PROXY_BACKOFF_TIMEOUT_SECONDS", 25));

pub static REQUEST_SECONDS: LazyLock<u64> =
    LazyLock::new(|| fetch_or_default("MEILI_EXPERIMENTAL_PROXY_REQUEST_TIMEOUT_SECONDS", 30));

fn fetch_or_default(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(timeout) => timeout.parse().unwrap_or_else(|_| {
            panic!("`{key}` environment variable is not parseable as an integer: {timeout}")
        }),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("`{key}` environment variable is not set to a integer")
        }
    }
}
