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
use tokio::sync::mpsc::{self};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::Layer;

use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::LogRouteHandle;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::post().to(SeqHandler(get_logs)))
            .route(web::delete().to(SeqHandler(cancel_logs))),
    );
}

#[derive(Debug, Default, Clone, Copy, Deserr)]
#[deserr(rename_all = lowercase)]
pub enum LogMode {
    #[default]
    Fmt,
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
        "Empty string is not a valid target. If you want to get no logs use `OFF`. Usage: `info`, `info:meilisearch`, or you can write multiple filters in one target: `index_scheduler=info,milli=trace`"
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

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct GetLogs {
    #[deserr(default = "info".parse().unwrap(), try_from(&String) = MyTargets::from_str -> DeserrJsonError<BadRequest>)]
    target: MyTargets,

    #[deserr(default, error = DeserrJsonError<BadRequest>)]
    mode: LogMode,
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
        LogMode::Fmt => {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_line_number(true)
                .with_writer(move || LogWriter { sender: sender.clone() })
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE);

            let stream = byte_stream(receiver, guard);
            (Box::new(fmt_layer) as Box<dyn Layer<S> + Send + Sync>, Box::pin(stream))
        }
        LogMode::Profile => {
            let (trace, layer) = tracing_trace::Trace::new();

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
