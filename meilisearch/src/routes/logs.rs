use std::fmt;
use std::io::Write;
use std::pin::Pin;
use std::str::FromStr;
use std::task::Poll;

use actix_web::web::{Bytes, Data};
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use futures_util::{pin_mut, FutureExt};
use meilisearch_auth::AuthController;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use tokio::pin;
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(get_logs))));
}

#[derive(Debug, Default, Clone, Copy, Deserr)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct GetLogs {
    #[deserr(default, error = DeserrJsonError<BadRequest>)]
    pub level: LogLevel,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Error => f.write_str("error"),
            LogLevel::Warn => f.write_str("warn"),
            LogLevel::Info => f.write_str("info"),
            LogLevel::Debug => f.write_str("debug"),
            LogLevel::Trace => f.write_str("trace"),
        }
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

struct LogStreamer {
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    // We just need to hold the guard until the struct is dropped
    #[allow(unused)]
    subscriber: tracing::subscriber::DefaultGuard,
}

impl futures_util::Stream for LogStreamer {
    type Item = Result<Bytes, ResponseError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let future = self.get_mut().receiver.recv();
        pin_mut!(future);

        match future.poll_unpin(cx) {
            std::task::Poll::Ready(recv) => match recv {
                Some(buf) => {
                    // let bytes = Bytes::copy_from_slice(buf.as_slice());
                    Poll::Ready(Some(Ok(buf.into())))
                }
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

pub async fn get_logs(
    _auth_controller: GuardedData<ActionPolicy<{ actions::METRICS_ALL }>, Data<AuthController>>,
    body: AwebJson<GetLogs, DeserrJsonError>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let opt = body.into_inner();

    // #[cfg(not(feature = "stats_alloc"))]
    // let (mut trace, layer) = tracing_trace::Trace::new(file);
    // #[cfg(feature = "stats_alloc")]
    // let (mut trace, layer) = tracing_trace::Trace::with_stats_alloc(file, &ALLOC);

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(move || LogWriter { sender: sender.clone() })
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE)
        .with_filter(
            tracing_subscriber::filter::LevelFilter::from_str(&opt.level.to_string()).unwrap(),
        );

    let subscriber = tracing_subscriber::registry().with(layer);
    // .with(
    //     layer.with_filter(
    //         tracing_subscriber::filter::Targets::new()
    //             .with_target("indexing::", tracing::Level::TRACE),
    //     ),
    // );

    let subscriber = tracing::subscriber::set_default(subscriber);

    Ok(HttpResponse::Ok().streaming(LogStreamer { receiver, subscriber }))
}
