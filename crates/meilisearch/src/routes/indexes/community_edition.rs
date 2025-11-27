pub mod proxy {

    use std::fs::File;

    use actix_web::HttpRequest;
    use index_scheduler::IndexScheduler;

    use crate::error::MeilisearchHttpError;

    pub enum Body<T: serde::Serialize> {
        NdJsonPayload,
        Inline(T),
        None,
    }

    impl Body<()> {
        pub fn with_ndjson_payload(_file: File) -> Self {
            Self::NdJsonPayload
        }

        pub fn none() -> Self {
            Self::None
        }
    }

    pub const PROXY_ORIGIN_REMOTE_HEADER: &str = "Meili-Proxy-Origin-Remote";
    pub const PROXY_ORIGIN_TASK_UID_HEADER: &str = "Meili-Proxy-Origin-TaskUid";

    pub async fn proxy<T: serde::Serialize>(
        _index_scheduler: &IndexScheduler,
        _index_uid: &str,
        _req: &HttpRequest,
        _network: meilisearch_types::network::Network,
        _body: Body<T>,
        _task: &meilisearch_types::tasks::Task,
    ) -> Result<(), MeilisearchHttpError> {
        Ok(())
    }
}
