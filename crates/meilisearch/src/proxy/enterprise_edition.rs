// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;

use actix_http::uri::PathAndQuery;
use actix_web::http::header::CONTENT_TYPE;
use actix_web::HttpRequest;
use bytes::Bytes;
use http_client::reqwest::{ClientBuilder, StatusCode};
use index_scheduler::{IndexScheduler, ReqwestRequestWrapper};
use meilisearch_types::error::ResponseError;
use meilisearch_types::network::{route, Remote};
use meilisearch_types::tasks::network::headers::{GetHeader, SetHeader};
use meilisearch_types::tasks::network::{
    DbTaskNetwork, ImportData, ImportMetadata, Origin, TaskNetwork,
};
use meilisearch_types::tasks::{Task, TaskId};
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::error::MeilisearchHttpError;
use crate::proxy::{Body, ProxyError, ReqwestErrorWithoutUrl};
use crate::routes::SummarizedTaskView;

mod timeouts {
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
}

impl<T, F> Body<T, F>
where
    T: serde::Serialize,
    F: FnMut(&str, &Remote, &mut T),
{
    pub fn into_bytes_iter(
        self,
        remotes: impl IntoIterator<Item = (String, Remote)>,
    ) -> Result<
        impl Iterator<Item = (Option<Bytes>, (String, Remote))>,
        meilisearch_types::milli::Error,
    > {
        let bytes = match self {
            Body::NdJsonPayload(file) => {
                Some(Bytes::from_owner(unsafe { memmap2::Mmap::map(&file)? }))
            }

            Body::Inline(payload) => {
                Some(Bytes::copy_from_slice(&serde_json::to_vec(&payload).unwrap()))
            }

            Body::None => None,

            Body::Generated(mut initial, mut f) => {
                return Ok(either::Right(remotes.into_iter().map(move |(name, remote)| {
                    f(&name, &remote, &mut initial);
                    let bytes =
                        Some(Bytes::copy_from_slice(&serde_json::to_vec(&initial).unwrap()));
                    (bytes, (name, remote))
                })));
            }
        };
        Ok(either::Left(std::iter::repeat(bytes).zip(remotes)))
    }

    pub fn into_bytes(
        self,
        remote_name: &str,
        remote: &Remote,
    ) -> Result<Option<Bytes>, meilisearch_types::milli::Error> {
        Ok(match self {
            Body::NdJsonPayload(file) => {
                Some(Bytes::from_owner(unsafe { memmap2::Mmap::map(&file)? }))
            }

            Body::Inline(payload) => {
                Some(Bytes::copy_from_slice(&serde_json::to_vec(&payload).unwrap()))
            }

            Body::None => None,

            Body::Generated(mut initial, mut f) => {
                f(remote_name, remote, &mut initial);
                Some(Bytes::copy_from_slice(&serde_json::to_vec(&initial).unwrap()))
            }
        })
    }
}

/// Parses the header to determine if this task is a duplicate and originates with a remote.
///
/// If not, checks whether this remote is the leader and return `MeilisearchHttpError::NotLeader` if not.
///
/// If there is no leader, returns `Ok(None)`
///
/// # Errors
///
/// - `MeiliearchHttpError::NotLeader`: if the following are true simultaneously:
///     1. The task originates with the current node
///     2. There's a declared `leader`
///     3. The declared leader is **not** the current node
/// - `MeilisearchHttpError::InvalidHeaderValue`: if headers cannot be parsed as a task network.
/// - `MeilisearchHttpError::InconsistentTaskNetwork`: if only some of the headers are present.
pub fn task_network_and_check_leader_and_version(
    req: &HttpRequest,
    network: &meilisearch_types::network::Network,
) -> Result<Option<TaskNetwork>, MeilisearchHttpError> {
    let task_network =
        match (origin_from_req(req)?, import_data_from_req(req)?, import_metadata_from_req(req)?) {
            (Some(network_change), Some(import_from), Some(metadata)) => {
                TaskNetwork::Import { import_from, network_change, metadata }
            }
            (Some(origin), None, None) => TaskNetwork::Origin { origin },
            (None, None, None) => {
                match (network.leader.as_deref(), network.local.as_deref()) {
                    // 1. Always allowed if there is no leader
                    (None, _) => return Ok(None),
                    // 2. Allowed if the leader is self
                    (Some(leader), Some(this)) if leader == this => (),
                    // 3. Any other change is disallowed
                    (Some(leader), _) => {
                        return Err(MeilisearchHttpError::NotLeader { leader: leader.to_string() })
                    }
                }

                TaskNetwork::Remotes {
                    remote_tasks: Default::default(),
                    network_version: network.version,
                }
            }
            // all good cases were matched, so this is always an error
            (origin, import_from, metadata) => {
                return Err(MeilisearchHttpError::InconsistentTaskNetworkHeaders {
                    is_missing_origin: origin.is_none(),
                    is_missing_import: import_from.is_none(),
                    is_missing_import_metadata: metadata.is_none(),
                })
            }
        };

    if task_network.network_version() < network.version {
        return Err(MeilisearchHttpError::NetworkVersionTooOld {
            received: task_network.network_version(),
            expected_at_least: network.version,
        });
    }

    Ok(Some(task_network))
}

/// Updates the task description and, if necessary, proxies the passed request to the network and update the task description.
///
/// This function reads the custom headers from the request to determine if must proxy the request or if the request
/// has already been proxied.
///
/// - when it must proxy the request, the endpoint, method and query params are retrieved from the passed `req`, then the `body` is
///   sent to all remotes of the `network` (except `self`). The response from the remotes are collected to update the passed `task`
///   with the task ids from the task queues of the remotes.
/// - when the request has already been proxied, the custom headers contains information about the remote that created the initial task.
///   This information is copied to the passed task.
///
/// # Returns
///
/// The updated task. The task is read back from the database to avoid erasing concurrent changes.
pub async fn proxy<T, F>(
    index_scheduler: &IndexScheduler,
    index_uid: Option<&str>,
    req: &HttpRequest,
    mut task_network: DbTaskNetwork,
    network: meilisearch_types::network::Network,
    body: Body<T, F>,
    task: &Task,
) -> Result<Task, MeilisearchHttpError>
where
    T: serde::Serialize,
    F: FnMut(&str, &Remote, &mut T),
{
    if let DbTaskNetwork::Remotes { remote_tasks, network_version } = &mut task_network {
        let network_version = *network_version;
        let this = network
            .local
            .as_deref()
            .expect("inconsistent `network.leader` and `network.self`")
            .to_owned();

        let content_type = match &body {
            // for file bodies, force x-ndjson
            Body::NdJsonPayload(_) => Some(b"application/x-ndjson".as_slice()),
            // otherwise get content type from request
            _ => req.headers().get(CONTENT_TYPE).map(|h| h.as_bytes()),
        };

        let mut in_flight_remote_queries = BTreeMap::new();
        let client = ClientBuilder::new()
            .prepare(|inner| {
                inner.connect_timeout(std::time::Duration::from_secs(*timeouts::CONNECT_SECONDS))
            })
            .build_with_policies(index_scheduler.ip_policy().clone(), Default::default())
            .unwrap();

        let method = from_old_http_method(req.method());

        // send payload to all remotes
        for (body, (node_name, node)) in body
            .into_bytes_iter(network.remotes.into_iter().filter(|(name, _)| name.as_str() != this))
            .map_err(|err| {
                MeilisearchHttpError::from_milli(err, index_uid.map(ToOwned::to_owned))
            })?
        {
            tracing::trace!(node_name, "proxying task to remote");

            let client = client.clone();
            let api_key = node.write_api_key;
            let this = this.clone();
            let task_uid = task.uid;
            let method = method.clone();
            let path_and_query = req.uri().path_and_query().map(|paq| paq.as_str()).unwrap_or("/");

            in_flight_remote_queries.insert(
                node_name,
                tokio::spawn({
                    let url = format!("{}{}", node.url, path_and_query);

                    let content_type = content_type.map(|b| b.to_owned());

                    let backoff = backoff::ExponentialBackoffBuilder::new()
                        .with_max_elapsed_time(Some(std::time::Duration::from_secs(
                            *timeouts::BACKOFF_SECONDS,
                        )))
                        .build();

                    backoff::future::retry(backoff, move || {
                        let url = url.clone();
                        let client = client.clone();
                        let this = this.clone();
                        let content_type = content_type.clone();

                        let body = body.clone();
                        let api_key = api_key.clone();
                        let method = method.clone();

                        async move {
                            try_proxy(
                                method,
                                &url,
                                content_type.as_deref(),
                                network_version,
                                api_key.as_deref(),
                                &client,
                                &this,
                                task_uid,
                                body,
                            )
                            .await
                        }
                    })
                }),
            );
        }

        // wait for all in-flight queries to finish and collect their results
        for (node_name, handle) in in_flight_remote_queries {
            match handle.await {
                Ok(Ok(res)) => {
                    let task_uid = res.task_uid;

                    remote_tasks.insert(node_name, Ok(task_uid).into());
                }
                Ok(Err(error)) => {
                    remote_tasks.insert(node_name, Err(error.as_response_error()).into());
                }
                Err(panic) => match panic.try_into_panic() {
                    Ok(panic) => {
                        let msg = match panic.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match panic.downcast_ref::<String>() {
                                Some(s) => &s[..],
                                None => "Box<dyn Any>",
                            },
                        };
                        remote_tasks.insert(
                            node_name,
                            Err(ResponseError::from_msg(
                                msg.to_string(),
                                meilisearch_types::error::Code::Internal,
                            ))
                            .into(),
                        );
                    }
                    Err(_) => {
                        tracing::error!("proxy task was unexpectedly cancelled")
                    }
                },
            }
        }
    }

    Ok(index_scheduler.set_task_network(task.uid, task_network)?)
}

pub async fn send_request<T, F, U>(
    path_and_query: &str,
    method: http_client::reqwest::Method,
    content_type: Option<String>,
    body: Body<T, F>,
    remote_name: &str,
    remote: &Remote,
    ip_policy: http_client::policy::IpPolicy,
) -> Result<U, ProxyError>
where
    T: serde::Serialize,
    F: FnMut(&str, &Remote, &mut T),
    U: DeserializeOwned,
{
    let content_type = match &body {
        // for file bodies, force x-ndjson
        Body::NdJsonPayload(_) => Some("application/x-ndjson".into()),
        // otherwise get content type from request
        _ => content_type,
    };

    let body = body.into_bytes(remote_name, remote).map_err(Box::new)?;

    let client = ClientBuilder::new()
        .prepare(|inner| {
            inner.connect_timeout(std::time::Duration::from_secs(*timeouts::CONNECT_SECONDS))
        })
        .build_with_policies(ip_policy, Default::default())
        .unwrap();

    let url = format!("{}{}", remote.url, path_and_query);

    // send payload to remote
    tracing::trace!(remote_name, "sending request to remote");
    let api_key = remote.write_api_key.clone();

    let backoff = backoff::ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(Some(std::time::Duration::from_secs(*timeouts::BACKOFF_SECONDS)))
        .build();

    backoff::future::retry(backoff, move || {
        let url = url.clone();
        let client = client.clone();
        let content_type = content_type.clone();

        let body = body.clone();
        let api_key = api_key.clone();
        let method = method.clone();

        async move {
            let request = client.request(method, url).prepare(|request| {
                let request =
                    request.timeout(std::time::Duration::from_secs(*timeouts::REQUEST_SECONDS));
                let request = if let Some(body) = body { request.body(body) } else { request };
                let request = if let Some(api_key) = api_key {
                    request.bearer_auth(api_key)
                } else {
                    request
                };
                let request = if let Some(content_type) = content_type {
                    request.header(CONTENT_TYPE.as_str(), content_type)
                } else {
                    request
                };
                request
            });

            let response = request.send().await;
            let response = match response {
                Ok(response) => response,
                Err(http_client::reqwest::Error::Reqwest(error)) if error.is_timeout() => {
                    return Err(backoff::Error::transient(ProxyError::Timeout))
                }
                Err(error) => {
                    return Err(backoff::Error::transient(ProxyError::CouldNotSendRequest(
                        ReqwestErrorWithoutUrl::new(error),
                    )))
                }
            };

            handle_response(response).await
        }
    })
    .await
}

async fn handle_response<U>(
    response: http_client::reqwest::Response,
) -> Result<U, backoff::Error<ProxyError>>
where
    U: DeserializeOwned,
{
    match response.status() {
        status_code if status_code.is_success() => (),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            return Err(backoff::Error::Permanent(ProxyError::AuthenticationError))
        }
        status_code if status_code.is_client_error() => {
            let response = parse_error(response).await;
            return Err(backoff::Error::Permanent(ProxyError::BadRequest {
                status_code,
                response,
            }));
        }
        status_code if status_code.is_server_error() => {
            let response = parse_error(response).await;
            return Err(backoff::Error::transient(ProxyError::RemoteError {
                status_code,
                response,
            }));
        }
        status_code => {
            tracing::warn!(
                status_code = status_code.as_u16(),
                "remote replied with unexpected status code"
            );
        }
    }
    let response: U = match parse_response(response).await {
        Ok(response) => response,
        Err(response) => {
            return Err(backoff::Error::permanent(ProxyError::CouldNotParseResponse { response }))
        }
    };
    Ok(response)
}

fn from_old_http_method(method: &actix_http::Method) -> http_client::reqwest::Method {
    match method {
        &actix_http::Method::CONNECT => http_client::reqwest::Method::CONNECT,
        &actix_http::Method::DELETE => http_client::reqwest::Method::DELETE,
        &actix_http::Method::GET => http_client::reqwest::Method::GET,
        &actix_http::Method::HEAD => http_client::reqwest::Method::HEAD,
        &actix_http::Method::OPTIONS => http_client::reqwest::Method::OPTIONS,
        &actix_http::Method::PATCH => http_client::reqwest::Method::PATCH,
        &actix_http::Method::POST => http_client::reqwest::Method::POST,
        &actix_http::Method::PUT => http_client::reqwest::Method::PUT,
        &actix_http::Method::TRACE => http_client::reqwest::Method::TRACE,
        method => http_client::reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_proxy(
    method: http_client::reqwest::Method,
    url: &str,
    content_type: Option<&[u8]>,
    network_version: Uuid,
    api_key: Option<&str>,
    client: &http_client::reqwest::Client,
    this: &str,
    task_uid: TaskId,
    body: Option<Bytes>,
) -> Result<SummarizedTaskView, backoff::Error<ProxyError>> {
    let request = client.request(method, url).prepare(|request| {
        let request = request.timeout(std::time::Duration::from_secs(*timeouts::REQUEST_SECONDS));
        let request = if let Some(body) = body { request.body(body) } else { request };
        let request =
            if let Some(api_key) = api_key { request.bearer_auth(api_key) } else { request };

        let request = if let Some(content_type) = content_type {
            request.header(CONTENT_TYPE.as_str(), content_type)
        } else {
            request
        };
        request
    });
    let ReqwestRequestWrapper(request) = ReqwestRequestWrapper(request)
        .set_origin_task_uid(task_uid)
        .set_origin_network_version(network_version)
        .set_origin_remote(this);

    let response = request.send().await;
    let response = match response {
        Ok(response) => response,
        Err(error) if error.is_timeout() => {
            return Err(backoff::Error::transient(ProxyError::Timeout))
        }
        Err(error) => {
            return Err(backoff::Error::transient(ProxyError::CouldNotSendRequest(
                ReqwestErrorWithoutUrl::new(error),
            )))
        }
    };

    handle_response(response).await
}

async fn parse_error(
    response: http_client::reqwest::Response,
) -> Result<String, ReqwestErrorWithoutUrl> {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return Err(ReqwestErrorWithoutUrl::new(error.into())),
    };

    Ok(parse_bytes_as_error(&bytes))
}

fn parse_bytes_as_error(bytes: &[u8]) -> String {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => value.to_string(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

async fn parse_response<T: DeserializeOwned>(
    response: http_client::reqwest::Response,
) -> Result<T, Result<String, ReqwestErrorWithoutUrl>> {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return Err(Err(ReqwestErrorWithoutUrl::new(error.into()))),
    };

    match serde_json::from_slice::<T>(&bytes) {
        Ok(value) => Ok(value),
        Err(_) => Err(Ok(parse_bytes_as_error(&bytes))),
    }
}

struct ResponseWrapper<'a>(&'a HttpRequest);
impl<'a> meilisearch_types::tasks::network::headers::GetHeader for ResponseWrapper<'a> {
    type Error = actix_http::header::ToStrError;

    fn get_header(&self, name: &str) -> Result<Option<&str>, Self::Error> {
        self.0.headers().get(name).map(|value| value.to_str()).transpose()
    }
}

pub fn origin_from_req(req: &HttpRequest) -> Result<Option<Origin>, MeilisearchHttpError> {
    let req = ResponseWrapper(req);
    let (remote_name, task_uid, network_version) = match (
        req.get_origin_remote()?,
        req.get_origin_task_uid()?,
        req.get_origin_network_version()?,
    ) {
        (None, None, _) => return Ok(None),
        (None, Some(_), _) => {
            return Err(MeilisearchHttpError::InconsistentOriginHeaders { is_remote_missing: true })
        }
        (Some(_), None, _) => {
            return Err(MeilisearchHttpError::InconsistentOriginHeaders {
                is_remote_missing: false,
            })
        }
        (Some(remote_name), Some(task_uid), network_version) => {
            (remote_name, task_uid, network_version)
        }
    };

    let network_version = network_version.unwrap_or_else(Uuid::nil);

    Ok(Some(Origin { remote_name: remote_name.into_owned(), task_uid, network_version }))
}

pub fn import_data_from_req(req: &HttpRequest) -> Result<Option<ImportData>, MeilisearchHttpError> {
    let req = ResponseWrapper(req);
    let (remote_name, index_name, document_count) =
        match (req.get_import_remote()?, req.get_import_index()?, req.get_import_docs()?) {
            (None, None, None) => return Ok(None),
            (Some(remote_name), index_name, Some(documents)) => {
                (remote_name, index_name, documents)
            }
            // catch-all pattern that has to contain an inconsistency since we already matched (None, None, None) and (Some, Some, Some)
            (remote_name, index_name, documents) => {
                return Err(MeilisearchHttpError::InconsistentImportHeaders {
                    is_remote_missing: remote_name.is_none(),
                    is_index_missing: index_name.is_none(),
                    is_docs_missing: documents.is_none(),
                })
            }
        };

    Ok(Some(ImportData {
        remote_name: remote_name.to_string(),
        index_name: index_name.map(|index_name| index_name.to_string()),
        document_count,
    }))
}

pub fn import_metadata_from_req(
    req: &HttpRequest,
) -> Result<Option<ImportMetadata>, MeilisearchHttpError> {
    let req = ResponseWrapper(req);
    let (index_count, task_key, total_index_documents) = match (
        req.get_import_index_count()?,
        req.get_import_task_key()?,
        req.get_import_index_docs()?,
    ) {
        (None, None, None) => return Ok(None),
        (Some(index_count), task_key, Some(total_index_documents)) => {
            (index_count, task_key, total_index_documents)
        }
        // catch-all pattern that has to contain an inconsistency since we already matched (None, None, None) and (Some, Some, Some)
        (index_count, task_key, total_index_documents) => {
            return Err(MeilisearchHttpError::InconsistentImportMetadataHeaders {
                is_index_count_missing: index_count.is_none(),
                is_task_key_missing: task_key.is_none(),
                is_total_index_documents_missing: total_index_documents.is_none(),
            })
        }
    };

    Ok(Some(ImportMetadata { index_count, task_key, total_index_documents }))
}
