use actix_web::body::MessageBody;
use actix_web::{dev, Error};
use opentelemetry::global::ObjectSafeSpan;
use opentelemetry::trace::{SpanKind, Status, Tracer};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_stdout::SpanExporter;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub fn init_tracer() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = TracerProvider::builder()
        .with_simple_exporter(SpanExporter::default())
        .build();
    global::set_tracer_provider(provider);
}

pub struct OpenTelemetryMiddleware;

impl<S, B> dev::Transform<S, dev::ServiceRequest> for OpenTelemetryMiddleware
where
    S: dev::Service<
        dev::ServiceRequest,
        Response = dev::ServiceResponse<B>,
        Error = Error,
    > + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = dev::ServiceResponse<B>;
    type Error = Error;
    type Transform = OpenTelemetryMiddlewareService<S>;
    type InitError = ();
    type Future = std::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        std::future::ready(Ok(OpenTelemetryMiddlewareService { service }))
    }
}

pub struct OpenTelemetryMiddlewareService<S> {
    service: S,
}

impl<S, B> dev::Service<dev::ServiceRequest> for OpenTelemetryMiddlewareService<S>
where
    S: dev::Service<
        dev::ServiceRequest,
        Response = dev::ServiceResponse<B>,
        Error = Error,
    > + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = dev::ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: dev::ServiceRequest) -> Self::Future {
        let tracer = global::tracer("meilisearch");
        let path = req.path().to_owned();
        let method = req.method().to_string();

        let mut span = tracer
            .span_builder(format!("{} {}", method, path))
            .with_kind(SpanKind::Server)
            .with_attributes(vec![
                KeyValue::new("http.method", method),
                KeyValue::new("http.path", path),
            ])
            .start(&tracer);

        let fut = self.service.call(req);
        Box::pin(async move {
            let res = fut.await;
            match &res {
                Ok(response) => {
                    span.set_attribute(KeyValue::new(
                        "http.status_code",
                        response.status().as_u16() as i64,
                    ));
                    span.set_status(Status::Ok);
                }
                Err(e) => {
                    span.set_status(Status::error(e.to_string()));
                }
            }
            res
        })
    }
}
