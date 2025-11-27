use lazy_static::lazy_static;
use prometheus::{
    opts, register_gauge, register_gauge_vec, register_histogram_vec, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, Gauge, GaugeVec, HistogramVec, IntCounterVec,
    IntGauge, IntGaugeVec,
};

lazy_static! {
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
    pub static ref MEILISEARCH_CHAT_SEARCHES_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!(
            "meilisearch_chat_searches_total",
            "Total number of searches performed by the chat route"
        ),
        &["type"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_CHAT_PROMPT_TOKENS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("meilisearch_chat_prompt_tokens_total", "Total number of prompt tokens consumed"),
        &["workspace", "model"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_CHAT_COMPLETION_TOKENS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            opts!(
                "meilisearch_chat_completion_tokens_total",
                "Total number of completion tokens consumed"
            ),
            &["workspace", "model"]
        )
        .expect("Can't create a metric");
    pub static ref MEILISEARCH_CHAT_TOKENS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!(
            "meilisearch_chat_tokens_total",
            "Total number of tokens consumed (prompt + completion)"
        ),
        &["workspace", "model"]
    )
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
    pub static ref MEILISEARCH_BATCH_RUNNING_PROGRESS_TRACE: GaugeVec = register_gauge_vec!(
        opts!("meilisearch_batch_running_progress_trace", "The currently running progress trace"),
        &["batch_uid", "step_name"]
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_LAST_FINISHED_BATCHES_PROGRESS_TRACE_MS: IntGaugeVec =
        register_int_gauge_vec!(
            opts!(
                "meilisearch_last_finished_batches_progress_trace_ms",
                "The last few batches progress trace in milliseconds"
            ),
            &["batch_uid", "step_name"]
        )
        .expect("Can't create a metric");
    pub static ref MEILISEARCH_LAST_UPDATE: IntGauge =
        register_int_gauge!(opts!("meilisearch_last_update", "Meilisearch Last Update"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_IS_INDEXING: IntGauge =
        register_int_gauge!(opts!("meilisearch_is_indexing", "Meilisearch Is Indexing"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_SEARCH_QUEUE_SIZE: IntGauge = register_int_gauge!(opts!(
        "meilisearch_search_queue_size",
        "Meilisearch Search Queue Size"
    ))
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_SEARCHES_RUNNING: IntGauge =
        register_int_gauge!(opts!("meilisearch_searches_running", "Meilisearch Searches Running"))
            .expect("Can't create a metric");
    pub static ref MEILISEARCH_SEARCHES_WAITING_TO_BE_PROCESSED: IntGauge =
        register_int_gauge!(opts!(
            "meilisearch_searches_waiting_to_be_processed",
            "Meilisearch Searches Being Processed"
        ))
        .expect("Can't create a metric");
    pub static ref MEILISEARCH_TASK_QUEUE_LATENCY_SECONDS: Gauge = register_gauge!(
        "meilisearch_task_queue_latency_seconds",
        "Meilisearch Task Queue Latency in Seconds",
    )
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_TASK_QUEUE_MAX_SIZE: IntGauge = register_int_gauge!(opts!(
        "meilisearch_task_queue_max_size",
        "Meilisearch Task Queue Max Size",
    ))
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_TASK_QUEUE_USED_SIZE: IntGauge = register_int_gauge!(opts!(
        "meilisearch_task_queue_used_size",
        "Meilisearch Task Queue Used Size"
    ))
    .expect("Can't create a metric");
    pub static ref MEILISEARCH_TASK_QUEUE_SIZE_UNTIL_STOP_REGISTERING: IntGauge =
        register_int_gauge!(opts!(
            "meilisearch_task_queue_size_until_stop_registering",
            "Meilisearch Task Queue Size Until Stop Registering",
        ))
        .expect("Can't create a metric");
    pub static ref MEILISEARCH_PERSONALIZED_SEARCH_REQUESTS: IntGauge = register_int_gauge!(opts!(
        "meilisearch_personalized_search_requests",
        "Meilisearch number of search requests with personalization"
    ))
    .expect("Can't create a metric");
}
