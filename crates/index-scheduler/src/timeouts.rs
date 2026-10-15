use std::sync::LazyLock;

use meilisearch_types::milli::fetch_from_env_or;

pub static CONNECT_SECONDS: LazyLock<u64> =
    LazyLock::new(|| fetch_from_env_or("MEILI_EXPERIMENTAL_PROXY_CONNECT_TIMEOUT_SECONDS", 3));

pub static BACKOFF_SECONDS: LazyLock<u64> =
    LazyLock::new(|| fetch_from_env_or("MEILI_EXPERIMENTAL_PROXY_BACKOFF_TIMEOUT_SECONDS", 25));

pub static REQUEST_SECONDS: LazyLock<u64> =
    LazyLock::new(|| fetch_from_env_or("MEILI_EXPERIMENTAL_PROXY_REQUEST_TIMEOUT_SECONDS", 30));
