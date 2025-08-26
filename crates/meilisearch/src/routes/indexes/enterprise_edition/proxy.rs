// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::collections::BTreeMap;
use std::fs::File;

use actix_web::http::header::CONTENT_TYPE;
use actix_web::HttpRequest;
use bytes::Bytes;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::{Origin, RemoteTask, TaskNetwork};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::MeilisearchHttpError;
use crate::routes::indexes::enterprise_edition::proxy::error::{
    ProxyDocumentChangeError, ReqwestErrorWithoutUrl,
};
use crate::routes::SummarizedTaskView;

pub enum Body<T: serde::Serialize> {
    NdJsonPayload(File),
    Inline(T),
    None,
}

impl Body<()> {
    pub fn with_ndjson_payload(file: File) -> Self {
        Self::NdJsonPayload(file)
    }

    pub fn none() -> Self {
        Self::None
    }
}

/// If necessary, proxies the passed request to the network and update the task description.
///
/// This function reads the custom headers from the request to determine if must proxy the request or if the request
/// has already been proxied.
///
/// - when it must proxy the request, the endpoint, method and query params are retrieved from the passed `req`, then the `body` is
///   sent to all remotes of the `network` (except `self`). The response from the remotes are collected to update the passed `task`
///   with the task ids from the task queues of the remotes.
/// - when the request has already been proxied, the custom headers contains information about the remote that created the initial task.
///   This information is copied to the passed task.
pub async fn proxy<T: serde::Serialize>(
    index_scheduler: &IndexScheduler,
    index_uid: &str,
    req: &HttpRequest,
    network: meilisearch_types::enterprise_edition::network::Network,
    body: Body<T>,
    task: &meilisearch_types::tasks::Task,
) -> Result<(), MeilisearchHttpError> {
    match origin_from_req(req)? {
        Some(origin) => {
            index_scheduler.set_task_network(task.uid, TaskNetwork::Origin { origin })?
        }
        None => {
            let this = network
                .local
                .as_deref()
                .expect("inconsistent `network.sharding` and `network.self`")
                .to_owned();

            let content_type = match &body {
                // for file bodies, force x-ndjson
                Body::NdJsonPayload(_) => Some(b"application/x-ndjson".as_slice()),
                // otherwise get content type from request
                _ => req.headers().get(CONTENT_TYPE).map(|h| h.as_bytes()),
            };

            let body = match body {
                Body::NdJsonPayload(file) => Some(Bytes::from_owner(unsafe {
                    memmap2::Mmap::map(&file).map_err(|err| {
                        MeilisearchHttpError::from_milli(err.into(), Some(index_uid.to_owned()))
                    })?
                })),

                Body::Inline(payload) => {
                    Some(Bytes::copy_from_slice(&serde_json::to_vec(&payload).unwrap()))
                }

                Body::None => None,
            };

            let mut in_flight_remote_queries = BTreeMap::new();
            let client = reqwest::ClientBuilder::new()
                .connect_timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap();

            let method = from_old_http_method(req.method());

            // send payload to all remotes
            for (node_name, node) in
                network.remotes.into_iter().filter(|(name, _)| name.as_str() != this)
            {
                let body = body.clone();
                let client = client.clone();
                let api_key = node.write_api_key;
                let this = this.clone();
                let method = method.clone();
                let path_and_query =
                    req.uri().path_and_query().map(|paq| paq.as_str()).unwrap_or("/");

                in_flight_remote_queries.insert(
                    node_name,
                    tokio::spawn({
                        let url = format!("{}{}", node.url, path_and_query);

                        let url_encoded_this = urlencoding::encode(&this).into_owned();
                        let url_encoded_task_uid = task.uid.to_string(); // it's url encoded i promize

                        let content_type = content_type.map(|b| b.to_owned());

                        let backoff = backoff::ExponentialBackoffBuilder::new()
                            .with_max_elapsed_time(Some(std::time::Duration::from_secs(25)))
                            .build();

                        backoff::future::retry(backoff, move || {
                            let url = url.clone();
                            let client = client.clone();
                            let url_encoded_this = url_encoded_this.clone();
                            let url_encoded_task_uid = url_encoded_task_uid.clone();
                            let content_type = content_type.clone();

                            let body = body.clone();
                            let api_key = api_key.clone();
                            let method = method.clone();

                            async move {
                                try_proxy(
                                    method,
                                    &url,
                                    content_type.as_deref(),
                                    api_key.as_deref(),
                                    &client,
                                    &url_encoded_this,
                                    &url_encoded_task_uid,
                                    body,
                                )
                                .await
                            }
                        })
                    }),
                );
            }

            // wait for all in-flight queries to finish and collect their results
            let mut remote_tasks: BTreeMap<String, RemoteTask> = BTreeMap::new();
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

            // edit details to contain the return values from the remotes
            index_scheduler.set_task_network(task.uid, TaskNetwork::Remotes { remote_tasks })?;
        }
    }

    Ok(())
}

fn from_old_http_method(method: &actix_http::Method) -> reqwest::Method {
    match method {
        &actix_http::Method::CONNECT => reqwest::Method::CONNECT,
        &actix_http::Method::DELETE => reqwest::Method::DELETE,
        &actix_http::Method::GET => reqwest::Method::GET,
        &actix_http::Method::HEAD => reqwest::Method::HEAD,
        &actix_http::Method::OPTIONS => reqwest::Method::OPTIONS,
        &actix_http::Method::PATCH => reqwest::Method::PATCH,
        &actix_http::Method::POST => reqwest::Method::POST,
        &actix_http::Method::PUT => reqwest::Method::PUT,
        &actix_http::Method::TRACE => reqwest::Method::TRACE,
        method => reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_proxy(
    method: reqwest::Method,
    url: &str,
    content_type: Option<&[u8]>,
    api_key: Option<&str>,
    client: &reqwest::Client,
    url_encoded_this: &str,
    url_encoded_task_uid: &str,
    body: Option<Bytes>,
) -> Result<SummarizedTaskView, backoff::Error<ProxyDocumentChangeError>> {
    let request = client.request(method, url).timeout(std::time::Duration::from_secs(30));
    let request = if let Some(body) = body { request.body(body) } else { request };
    let request = if let Some(api_key) = api_key { request.bearer_auth(api_key) } else { request };
    let request = request.header(PROXY_ORIGIN_TASK_UID_HEADER, url_encoded_task_uid);
    let request = request.header(PROXY_ORIGIN_REMOTE_HEADER, url_encoded_this);
    let request = if let Some(content_type) = content_type {
        request.header(CONTENT_TYPE.as_str(), content_type)
    } else {
        request
    };

    let response = request.send().await;
    let response = match response {
        Ok(response) => response,
        Err(error) if error.is_timeout() => {
            return Err(backoff::Error::transient(ProxyDocumentChangeError::Timeout))
        }
        Err(error) => {
            return Err(backoff::Error::transient(ProxyDocumentChangeError::CouldNotSendRequest(
                ReqwestErrorWithoutUrl::new(error),
            )))
        }
    };

    match response.status() {
        status_code if status_code.is_success() => (),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            return Err(backoff::Error::Permanent(ProxyDocumentChangeError::AuthenticationError))
        }
        status_code if status_code.is_client_error() => {
            let response = parse_error(response).await;
            return Err(backoff::Error::Permanent(ProxyDocumentChangeError::BadRequest {
                status_code,
                response,
            }));
        }
        status_code if status_code.is_server_error() => {
            let response = parse_error(response).await;
            return Err(backoff::Error::transient(ProxyDocumentChangeError::RemoteError {
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

    let response = match parse_response(response).await {
        Ok(response) => response,
        Err(response) => {
            return Err(backoff::Error::transient(
                ProxyDocumentChangeError::CouldNotParseResponse { response },
            ))
        }
    };

    Ok(response)
}

async fn parse_error(response: reqwest::Response) -> Result<String, ReqwestErrorWithoutUrl> {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return Err(ReqwestErrorWithoutUrl::new(error)),
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
    response: reqwest::Response,
) -> Result<T, Result<String, ReqwestErrorWithoutUrl>> {
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return Err(Err(ReqwestErrorWithoutUrl::new(error))),
    };

    match serde_json::from_slice::<T>(&bytes) {
        Ok(value) => Ok(value),
        Err(_) => Err(Ok(parse_bytes_as_error(&bytes))),
    }
}

mod error {
    use meilisearch_types::error::ResponseError;
    use reqwest::StatusCode;

    #[derive(Debug, thiserror::Error)]
    pub enum ProxyDocumentChangeError {
        #[error("{0}")]
        CouldNotSendRequest(ReqwestErrorWithoutUrl),
        #[error("could not authenticate against the remote host\n  - hint: check that the remote instance was registered with a valid API key having the `documents.add` action")]
        AuthenticationError,
        #[error(
            "could not parse response from the remote host as a document addition response{}\n  - hint: check that the remote instance is a Meilisearch instance running the same version",
            response_from_remote(response)
        )]
        CouldNotParseResponse { response: Result<String, ReqwestErrorWithoutUrl> },
        #[error("remote host responded with code {}{}\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance", status_code.as_u16(), response_from_remote(response))]
        BadRequest { status_code: StatusCode, response: Result<String, ReqwestErrorWithoutUrl> },
        #[error("remote host did not answer before the deadline")]
        Timeout,
        #[error("remote host responded with code {}{}", status_code.as_u16(), response_from_remote(response))]
        RemoteError { status_code: StatusCode, response: Result<String, ReqwestErrorWithoutUrl> },
    }

    impl ProxyDocumentChangeError {
        pub fn as_response_error(&self) -> ResponseError {
            use meilisearch_types::error::Code;
            let message = self.to_string();
            let code = match self {
                ProxyDocumentChangeError::CouldNotSendRequest(_) => Code::RemoteCouldNotSendRequest,
                ProxyDocumentChangeError::AuthenticationError => Code::RemoteInvalidApiKey,
                ProxyDocumentChangeError::BadRequest { .. } => Code::RemoteBadRequest,
                ProxyDocumentChangeError::Timeout => Code::RemoteTimeout,
                ProxyDocumentChangeError::RemoteError { .. } => Code::RemoteRemoteError,
                ProxyDocumentChangeError::CouldNotParseResponse { .. } => Code::RemoteBadResponse,
            };
            ResponseError::from_msg(message, code)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error(transparent)]
    pub struct ReqwestErrorWithoutUrl(reqwest::Error);
    impl ReqwestErrorWithoutUrl {
        pub fn new(inner: reqwest::Error) -> Self {
            Self(inner.without_url())
        }
    }

    fn response_from_remote(response: &Result<String, ReqwestErrorWithoutUrl>) -> String {
        match response {
            Ok(response) => {
                format!(":\n  - response from remote: {}", response)
            }
            Err(error) => {
                format!(":\n  - additionally, could not retrieve response from remote: {error}")
            }
        }
    }
}

pub const PROXY_ORIGIN_REMOTE_HEADER: &str = "Meili-Proxy-Origin-Remote";
pub const PROXY_ORIGIN_TASK_UID_HEADER: &str = "Meili-Proxy-Origin-TaskUid";

pub fn origin_from_req(req: &HttpRequest) -> Result<Option<Origin>, MeilisearchHttpError> {
    let (remote_name, task_uid) = match (
        req.headers().get(PROXY_ORIGIN_REMOTE_HEADER),
        req.headers().get(PROXY_ORIGIN_TASK_UID_HEADER),
    ) {
        (None, None) => return Ok(None),
        (None, Some(_)) => {
            return Err(MeilisearchHttpError::InconsistentOriginHeaders { is_remote_missing: true })
        }
        (Some(_), None) => {
            return Err(MeilisearchHttpError::InconsistentOriginHeaders {
                is_remote_missing: false,
            })
        }
        (Some(remote_name), Some(task_uid)) => (
            urlencoding::decode(remote_name.to_str().map_err(|err| {
                MeilisearchHttpError::InvalidHeaderValue {
                    header_name: PROXY_ORIGIN_REMOTE_HEADER,
                    msg: format!("while parsing remote name as UTF-8: {err}"),
                }
            })?)
            .map_err(|err| MeilisearchHttpError::InvalidHeaderValue {
                header_name: PROXY_ORIGIN_REMOTE_HEADER,
                msg: format!("while URL-decoding remote name: {err}"),
            })?,
            urlencoding::decode(task_uid.to_str().map_err(|err| {
                MeilisearchHttpError::InvalidHeaderValue {
                    header_name: PROXY_ORIGIN_TASK_UID_HEADER,
                    msg: format!("while parsing task UID as UTF-8: {err}"),
                }
            })?)
            .map_err(|err| MeilisearchHttpError::InvalidHeaderValue {
                header_name: PROXY_ORIGIN_TASK_UID_HEADER,
                msg: format!("while URL-decoding task UID: {err}"),
            })?,
        ),
    };

    let task_uid: usize =
        task_uid.parse().map_err(|err| MeilisearchHttpError::InvalidHeaderValue {
            header_name: PROXY_ORIGIN_TASK_UID_HEADER,
            msg: format!("while parsing the task UID as an integer: {err}"),
        })?;

    Ok(Some(Origin { remote_name: remote_name.into_owned(), task_uid }))
}
