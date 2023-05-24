use lazy_static::lazy_static;
use prometheus::{
    opts, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
};

/// Create evenly distributed buckets
fn create_buckets<const N: usize>() -> [f64; N] {
    let mut array = [0.0; N];

    for i in 0..N {
        array[i] = ((i + 1) as f64) / N as f64;
    }

    array
}

lazy_static! {
    pub static ref HTTP_RESPONSE_TIME_CUSTOM_BUCKETS: [f64; 100] = create_buckets();
    pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("http_requests_total", "HTTP requests total"),
        &["method", "path"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_DB_SIZE_BYTES: IntGauge =
        register_int_gauge!(opts!("meilisearch_db_size_bytes", "Meilisearch Db Size In Bytes"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_INDEX_COUNT: IntGauge =
        register_int_gauge!(opts!("meilisearch_index_count", "Meilisearch Index Count"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_INDEX_DOCS_COUNT: IntGaugeVec = register_int_gauge_vec!(
        opts!("meilisearch_index_docs_count", "Meilisearch Index Docs Count"),
        &["index"]
    )
    .expect("Can't create a metric");
    pub static ref HTTP_RESPONSE_TIME_SECONDS: HistogramVec = register_histogram_vec!(
        "http_response_time_seconds",
        "HTTP response times",
        &["method", "path"],
        HTTP_RESPONSE_TIME_CUSTOM_BUCKETS.to_vec()
    )
    .expect("Can't create a metric");
}
