use lazy_static::lazy_static;
use prometheus::{
    opts, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
};

lazy_static! {
    pub static ref MEILISEARCH_BUILD_INFO: IntGaugeVec = register_int_gauge_vec!(
          opts!(
            "meilisearch_build_info",
            "A metric with a constant '1' value labelled by version from which meilisearch was built"),
          & ["revision", "version"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("meilisearch_http_requests_total", "Meilisearch HTTP requests total"),
        &["method", "path", "status"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_DEGRADED_SEARCH_REQUESTS: IntGauge = register_int_gauge!(opts!(
        "meilisearch_degraded_search_requests",
        "Meilisearch number of degraded search requests"
    ))
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_DB_SIZE_BYTES: IntGauge =
        register_int_gauge!(opts!("meilisearch_db_size_bytes", "Meilisearch DB Size In Bytes"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_USED_DB_SIZE_BYTES: IntGauge = register_int_gauge!(opts!(
        "meilisearch_used_db_size_bytes",
        "Meilisearch Used DB Size In Bytes"
    ))
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_INDEX_COUNT: IntGauge =
        register_int_gauge!(opts!("meilisearch_index_count", "Meilisearch Index Count"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_INDEX_DOCS_COUNT: IntGaugeVec = register_int_gauge_vec!(
        opts!("meilisearch_index_docs_count", "Meilisearch Index Docs Count"),
        &["index"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_HTTP_RESPONSE_TIME_SECONDS: HistogramVec = register_histogram_vec!(
        "meilisearch_http_response_time_seconds",
        "Meilisearch HTTP response times",
        &["method", "path"],
        vec![0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_NB_TASKS: IntGaugeVec = register_int_gauge_vec!(
        opts!("meilisearch_nb_tasks", "Meilisearch Number of tasks"),
        &["kind", "value"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_LAST_UPDATE: IntGauge =
        register_int_gauge!(opts!("meilisearch_last_update", "Meilisearch Last Update"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_IS_INDEXING: IntGauge =
        register_int_gauge!(opts!("meilisearch_is_indexing", "Meilisearch Is Indexing"))
            .expect("Can't create a metric");
}
