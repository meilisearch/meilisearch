use std::convert::Infallible;
use std::io::Write;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use actix_web::web::{Bytes, Data};
use actix_web::{web, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::{DeserializeError, Deserr, ErrorKind, MergeWithError, ValuePointerRef};
use futures_util::Stream;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use serde::Serialize;
use tokio::sync::mpsc;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;
use utoipa::{OpenApi, ToSchema};

use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::{LogRouteHandle, LogStderrHandle};

#[derive(OpenApi)]
#[openapi(
    paths(get_logs, cancel_logs, update_stderr_target),
    tags((
        name = "Logs",
        description = "Everything about retrieving or customizing logs.
Currently [experimental](https://www.meilisearch.com/docs/learn/experimental/overview).",
        external_docs(url = "https://www.meilisearch.com/docs/learn/experimental/log_customization"),
    )),
)]
pub struct LogsApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("stream")
            .route(web::post().to(SeqHandler(get_logs)))
            .route(web::delete().to(SeqHandler(cancel_logs))),
    )
    .service(web::resource("stderr").route(web::post().to(SeqHandler(update_stderr_target))));
}

#[derive(Debug, Default, Clone, Copy, Deserr, Serialize, PartialEq, Eq, ToSchema)]
#[deserr(rename_all = camelCase)]
#[schema(rename_all = "camelCase")]
pub enum LogMode {
    /// Output the logs in a human readable form.
    #[default]
    Human,
    /// Output the logs in json.
    Json,
    /// Output the logs in the firefox profiler format. They can then be loaded and visualized at https://profiler.firefox.com/
    Profile,
}

/// Simple wrapper around the `Targets` from `tracing_subscriber` to implement `MergeWithError` on it.
#[derive(Clone, Debug)]
struct MyTargets(Targets);

/// Simple wrapper around the `ParseError` from `tracing_subscriber` to implement `MergeWithError` on it.
#[derive(Debug, thiserror::Error)]
enum MyParseError {
    #[error(transparent)]
    ParseError(#[from] tracing_subscriber::filter::ParseError),
    #[error(
        "Empty string is not a valid target. If you want to get no logs use `OFF`. Usage: `info`, `meilisearch=info`, or you can write multiple filters in one target: `index_scheduler=info,milli=trace`"
    )]
    Example,
}

impl FromStr for MyTargets {
    type Err = MyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(MyParseError::Example)
        } else {
            Ok(MyTargets(Targets::from_str(s).map_err(MyParseError::ParseError)?))
        }
    }
}

impl MergeWithError<MyParseError> for DeserrJsonError<BadRequest> {
    fn merge(
        _self_: Option<Self>,
        other: MyParseError,
        merge_location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        Self::error::<Infallible>(
            None,
            ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields, validate = validate_get_logs -> DeserrJsonError<InvalidSettingsTypoTolerance>)]
#[schema(rename_all = "camelCase")]
pub struct GetLogs {
    /// Lets you specify which parts of the code you want to inspect and is formatted like that: code_part=log_level,code_part=log_level
    /// - If the `code_part` is missing, then the `log_level` will be applied to everything.
    /// - If the `log_level` is missing, then the `code_part` will be selected in `info` log level.
    #[deserr(default = "info".parse().unwrap(), try_from(&String) = MyTargets::from_str -> DeserrJsonError<BadRequest>)]
    #[schema(value_type = String, default = "info", example = json!("milli=trace,index_scheduler,actix_web=off"))]
    target: MyTargets,

    /// Lets you customize the format of the logs.
    #[deserr(default, error = DeserrJsonError<BadRequest>)]
    #[schema(default = LogMode::default)]
    mode: LogMode,

    /// A boolean to indicate if you want to profile the memory as well. This is only useful while using the `profile` mode.
    /// Be cautious, though; it slows down the engine a lot.
    #[deserr(default = false, error = DeserrJsonError<BadRequest>)]
    #[schema(default = false)]
    profile_memory: bool,
}

fn validate_get_logs<E: DeserializeError>(
    logs: GetLogs,
    location: ValuePointerRef,
) -> Result<GetLogs, E> {
    if logs.profile_memory && logs.mode != LogMode::Profile {
        Err(deserr::take_cf_content(E::error::<Infallible>(
            None,
            ErrorKind::Unexpected {
                msg: format!("`profile_memory` can only be used while profiling code and is not compatible with the {:?} mode.", logs.mode),
            },
            location,
        )))
    } else {
        Ok(logs)
    }
}

struct LogWriter {
    sender: mpsc::UnboundedSender<Vec<u8>>,
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sender.send(buf.to_vec()).map_err(std::io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct HandleGuard {
    /// We need to keep an handle on the logs to make it available again when the streamer is dropped
    logs: Arc<LogRouteHandle>,
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        if let Err(e) = self.logs.modify(|layer| *layer.inner_mut() = None) {
            tracing::error!("Could not free the logs route: {e}");
        }
    }
}

fn byte_stream(
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    guard: HandleGuard,
) -> impl futures_util::Stream<Item = Result<Bytes, ResponseError>> {
    futures_util::stream::unfold((receiver, guard), move |(mut receiver, guard)| async move {
        let vec = receiver.recv().await;

        vec.map(From::from).map(Ok).map(|a| (a, (receiver, guard)))
    })
}

type PinnedByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ResponseError>>>>;

fn make_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>(
    opt: &GetLogs,
    logs: Data<LogRouteHandle>,
) -> (Box<dyn Layer<S> + Send + Sync>, PinnedByteStream) {
    let guard = HandleGuard { logs: logs.into_inner() };
    match opt.mode {
        LogMode::Human => {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(move || LogWriter { sender: sender.clone() })
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE);

            let stream = byte_stream(receiver, guard);
            (Box::new(fmt_layer) as Box<dyn Layer<S> + Send + Sync>, Box::pin(stream))
        }
        LogMode::Json => {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_writer(move || LogWriter { sender: sender.clone() })
                .json()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE);

            let stream = byte_stream(receiver, guard);
            (Box::new(fmt_layer) as Box<dyn Layer<S> + Send + Sync>, Box::pin(stream))
        }
        LogMode::Profile => {
            let (trace, layer) = tracing_trace::Trace::new(opt.profile_memory);

            let stream = entry_stream(trace, guard);

            (Box::new(layer) as Box<dyn Layer<S> + Send + Sync>, Box::pin(stream))
        }
    }
}

fn entry_stream(
    trace: tracing_trace::Trace,
    guard: HandleGuard,
) -> impl Stream<Item = Result<Bytes, ResponseError>> {
    let receiver = trace.into_receiver();
    let entry_buf = Vec::new();

    futures_util::stream::unfold(
        (receiver, entry_buf, guard),
        move |(mut receiver, mut entry_buf, guard)| async move {
            let mut bytes = Vec::new();

            while bytes.len() < 8192 {
                entry_buf.clear();

                let Ok(count) = tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    receiver.recv_many(&mut entry_buf, 100),
                )
                .await
                else {
                    break;
                };

                if count == 0 {
                    if !bytes.is_empty() {
                        break;
                    }

                    // channel closed, exit
                    return None;
                }

                for entry in &entry_buf {
                    if let Err(error) = serde_json::to_writer(&mut bytes, entry) {
                        tracing::error!(
                            error = &error as &dyn std::error::Error,
                            "deserializing entry"
                        );
                        return Some((
                            Err(ResponseError::from_msg(
                                format!("error deserializing entry: {error}"),
                                Code::Internal,
                            )),
                            (receiver, entry_buf, guard),
                        ));
                    }
                }
            }

            Some((Ok(bytes.into()), (receiver, entry_buf, guard)))
        },
    )
}

/// Retrieve logs
///
/// Stream logs over HTTP. The format of the logs depends on the configuration specified in the payload.
/// The logs are sent as multi-part, and the stream never stops, so make sure your clients correctly handle that.
/// To make the server stop sending you logs, you can call the `DELETE /logs/stream` route.
///
/// There can only be one listener at a timeand an error will be returned if you call this route while it's being used by another client.
#[utoipa::path(
    post,
    path = "/stream",
    tag = "Logs",
    security(("Bearer" = ["metrics.get", "metrics.*", "*"])),
    request_body = GetLogs,
    responses(
        (status = OK, description = "Logs are being returned", body = String, content_type = "application/json", example = json!(
            r#"
2024-10-08T13:35:02.643750Z  WARN HTTP request{method=GET host="localhost:7700" route=/metrics query_parameters= user_agent=HTTPie/3.2.3 status_code=400 error=Getting metrics requires enabling the `metrics` experimental feature. See https://github.com/meilisearch/product/discussions/625}: tracing_actix_web::middleware: Error encountered while processing the incoming HTTP request: ResponseError { code: 400, message: "Getting metrics requires enabling the `metrics` experimental feature. See https://github.com/meilisearch/product/discussions/625", error_code: "feature_not_enabled", error_type: "invalid_request", error_link: "https://docs.meilisearch.com/errors#feature_not_enabled" }
2024-10-08T13:35:02.644191Z  INFO HTTP request{method=GET host="localhost:7700" route=/metrics query_parameters= user_agent=HTTPie/3.2.3 status_code=400 error=Getting metrics requires enabling the `metrics` experimental feature. See https://github.com/meilisearch/product/discussions/625}: meilisearch: close time.busy=1.66ms time.idle=658µs
2024-10-08T13:35:18.564152Z  INFO HTTP request{method=PATCH host="localhost:7700" route=/experimental-features query_parameters= user_agent=curl/8.6.0 status_code=200}: meilisearch: close time.busy=1.17ms time.idle=127µs
2024-10-08T13:35:23.094987Z  INFO HTTP request{method=GET host="localhost:7700" route=/metrics query_parameters= user_agent=HTTPie/3.2.3 status_code=200}: meilisearch: close time.busy=2.12ms time.idle=595µs
"#
        )),
        (status = 400, description = "The route is already being used", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The `/logs/stream` route is currently in use by someone else.",
                "code": "bad_request",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#bad_request"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn get_logs(
    index_scheduler: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, Data<IndexScheduler>>,
    logs: Data<LogRouteHandle>,
    body: AwebJson<GetLogs, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_logs_route()?;

    let opt = body.into_inner();
    let mut stream = None;

    logs.modify(|layer| match layer.inner_mut() {
        None => {
            // there is no one getting logs
            *layer.filter_mut() = opt.target.0.clone();
            let (new_layer, new_stream) = make_layer(&opt, logs.clone());

            *layer.inner_mut() = Some(new_layer);
            stream = Some(new_stream);
        }
        Some(_) => {
            // there is already someone getting logs
        }
    })
    .unwrap();

    if let Some(stream) = stream {
        Ok(HttpResponse::Ok().streaming(stream))
    } else {
        Err(MeilisearchHttpError::AlreadyUsedLogRoute.into())
    }
}

/// Stop retrieving logs
///
/// Call this route to make the engine stops sending logs through the `POST /logs/stream` route.
#[utoipa::path(
    delete,
    path = "/stream",
    tag = "Logs",
    security(("Bearer" = ["metrics.get", "metrics.*", "*"])),
    responses(
        (status = NO_CONTENT, description = "Logs are being returned"),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn cancel_logs(
    index_scheduler: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, Data<IndexScheduler>>,
    logs: Data<LogRouteHandle>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_logs_route()?;

    if let Err(e) = logs.modify(|layer| *layer.inner_mut() = None) {
        tracing::error!("Could not free the logs route: {e}");
    }

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct UpdateStderrLogs {
    /// Lets you specify which parts of the code you want to inspect and is formatted like that: code_part=log_level,code_part=log_level
    /// - If the `code_part` is missing, then the `log_level` will be applied to everything.
    /// - If the `log_level` is missing, then the `code_part` will be selected in `info` log level.
    #[deserr(default = "info".parse().unwrap(), try_from(&String) = MyTargets::from_str -> DeserrJsonError<BadRequest>)]
    #[schema(value_type = String, default = "info", example = json!("milli=trace,index_scheduler,actix_web=off"))]
    target: MyTargets,
}

/// Update target of the console logs
///
/// This route lets you specify at runtime the level of the console logs outputted on stderr.
#[utoipa::path(
    post,
    path = "/stderr",
    tag = "Logs",
    request_body = UpdateStderrLogs,
    security(("Bearer" = ["metrics.get", "metrics.*", "*"])),
    responses(
        (status = NO_CONTENT, description = "The console logs have been updated"),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn update_stderr_target(
    index_scheduler: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, Data<IndexScheduler>>,
    logs: Data<LogStderrHandle>,
    body: AwebJson<UpdateStderrLogs, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_logs_route()?;

    let opt = body.into_inner();

    logs.modify(|layer| {
        *layer.filter_mut() = opt.target.0.clone();
    })
    .unwrap();

    Ok(HttpResponse::NoContent().finish())
}
