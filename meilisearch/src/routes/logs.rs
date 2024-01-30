use std::fmt;
use std::io::Write;
use std::ops::ControlFlow;
use std::str::FromStr;

use actix_web::web::{Bytes, Data};
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use meilisearch_auth::AuthController;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::instrument::WithSubscriber;
use tracing_subscriber::Layer;

use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::LogRouteHandle;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(get_logs))));
}

#[derive(Debug, Default, Clone, Copy, Deserr)]
#[deserr(rename_all = lowercase)]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Default, Clone, Copy, Deserr)]
#[deserr(rename_all = lowercase)]
pub enum LogMode {
    #[default]
    Fmt,
    Profile,
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct GetLogs {
    #[deserr(default, error = DeserrJsonError<BadRequest>)]
    pub level: LogLevel,

    #[deserr(default, error = DeserrJsonError<BadRequest>)]
    pub mode: LogMode,
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
}

impl LogStreamer {
    pub fn into_stream(self) -> impl futures_util::Stream<Item = Result<Bytes, ResponseError>> {
        futures_util::stream::unfold(self, move |mut this| async move {
            let vec = this.receiver.recv().await;

            vec.map(From::from).map(Ok).map(|a| (a, this))
        })
    }
}

pub fn make_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>(
    opt: &GetLogs,
    sender: UnboundedSender<Vec<u8>>,
) -> Box<dyn Layer<S> + Send + Sync> {
    match opt.mode {
        LogMode::Fmt => {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_line_number(true)
                .with_writer(move || LogWriter { sender: sender.clone() })
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE);

            Box::new(fmt_layer) as Box<dyn Layer<S> + Send + Sync>
        }
        LogMode::Profile => {
            let (mut trace, layer) =
                tracing_trace::Trace::new(LogWriter { sender: sender.clone() });

            tokio::task::spawn(async move {
                loop {
                    match tokio::time::timeout(std::time::Duration::from_secs(1), trace.receive())
                        .await
                    {
                        Ok(Ok(ControlFlow::Continue(()))) => continue,
                        Ok(Ok(ControlFlow::Break(_))) => break,
                        // the other half of the channel was dropped
                        Ok(Err(_)) => break,
                        Err(_) => trace.flush().unwrap(),
                    }
                }
                while trace.try_receive().is_ok() {}
                trace.flush().unwrap();
            });

            Box::new(layer) as Box<dyn Layer<S> + Send + Sync>
        }
    }
}

pub async fn get_logs(
    _auth_controller: GuardedData<ActionPolicy<{ actions::METRICS_ALL }>, Data<AuthController>>,
    logs: Data<LogRouteHandle>,
    body: AwebJson<GetLogs, DeserrJsonError>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let opt = body.into_inner();

    // #[cfg(not(feature = "stats_alloc"))]
    // let (mut trace, layer) = tracing_trace::Trace::new(file);
    // #[cfg(feature = "stats_alloc")]
    // let (mut trace, layer) = tracing_trace::Trace::with_stats_alloc(file, &ALLOC);

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    // let fmt_layer = tracing_subscriber::fmt::layer()
    //     .with_line_number(true)
    //     .with_writer(move || LogWriter { sender: sender.clone() })
    //     .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE)
    //     .with_filter(
    //         tracing_subscriber::filter::LevelFilter::from_str(&opt.level.to_string()).unwrap(),
    //     );
    // let subscriber = tracing_subscriber::registry().with(fmt_layer);
    // let subscriber = Box::new(subscriber) as Box<dyn Layer<S> + Send + Sync>;

    let mut was_available = false;

    logs.modify(|layer| match layer.inner_mut() {
        None => {
            // there is no one getting logs
            was_available = true;
            match opt.mode {
                LogMode::Fmt => {
                    *layer.filter_mut() =
                        tracing_subscriber::filter::LevelFilter::from_str(&opt.level.to_string())
                            .unwrap();
                }
                LogMode::Profile => {
                    *layer.filter_mut() =
                        tracing_subscriber::filter::LevelFilter::from_str(&opt.level.to_string())
                            .unwrap();
                    // *layer.filter_mut() = tracing_subscriber::filter::Targets::new()
                    //     .with_target("indexing::", tracing::Level::TRACE)
                    //     .with_filter(
                    //         tracing_subscriber::filter::LevelFilter::from_str(
                    //             &opt.level.to_string(),
                    //         )
                    //         .unwrap(),
                    //     )
                }
            }
            let new_layer = make_layer(&opt, sender);

            *layer.inner_mut() = Some(new_layer)
        }
        Some(_) => {
            // there is already someone getting logs
            was_available = false;
        }
    })
    .unwrap();

    if was_available {
        Ok(HttpResponse::Ok().streaming(LogStreamer { receiver }.into_stream()))
    } else {
        Err(MeilisearchHttpError::AlreadyUsedLogRoute.into())
    }
}
