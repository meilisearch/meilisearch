use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use actix_service::{Service, Transform};
use actix_web::{dev::ServiceRequest, dev::ServiceResponse, Error};
use actix_web::{get, HttpResponse};
use futures::future::{err, ok, Future, Ready};
use log::error;
use meilisearch_core::ProcessedUpdateResult;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::Data;

pub mod document;
pub mod health;
pub mod index;
pub mod key;
pub mod search;
pub mod setting;
pub mod stats;
pub mod stop_words;
pub mod synonym;

#[derive(Default, Deserialize)]
pub struct IndexParam {
    index_uid: String,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

#[get("/")]
pub async fn load_html() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../public/interface.html").to_string())
}

#[get("/bulma.min.css")]
pub async fn load_css() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css; charset=utf-8")
        .body(include_str!("../../public/bulma.min.css").to_string())
}

pub fn index_update_callback(index_uid: &str, data: &Data, status: ProcessedUpdateResult) {
    if status.error.is_some() {
        return;
    }

    if let Some(index) = data.db.open_index(&index_uid) {
        let db = &data.db;
        let mut writer = match db.main_write_txn() {
            Ok(writer) => writer,
            Err(e) => {
                error!("Impossible to get write_txn; {}", e);
                return;
            }
        };

        if let Err(e) = data.compute_stats(&mut writer, &index_uid) {
            error!("Impossible to compute stats; {}", e)
        }

        if let Err(e) = data.set_last_update(&mut writer) {
            error!("Impossible to update last_update; {}", e)
        }

        if let Err(e) = index.main.put_updated_at(&mut writer) {
            error!("Impossible to update updated_at; {}", e)
        }

        if let Err(e) = writer.commit() {
            error!("Impossible to get write_txn; {}", e);
        }
    }
}

#[derive(Clone)]
pub enum Authentication {
    Public,
    Private,
    Admin,
}

impl<S: 'static, B> Transform<S> for Authentication
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = LoggingMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(LoggingMiddleware {
            acl: (*self).clone(),
            service: Rc::new(RefCell::new(service)),
        })
    }
}

pub struct LoggingMiddleware<S> {
    acl: Authentication,
    service: Rc<RefCell<S>>,
}

impl<S, B> Service for LoggingMiddleware<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        let mut svc = self.service.clone();
        let data = req.app_data::<Data>().unwrap();

        if data.api_keys.master.is_none() {
            return Box::pin(svc.call(req));
        }

        let auth_header = match req.headers().get("X-Meili-API-Key") {
            Some(auth) => match auth.to_str() {
                Ok(auth) => auth,
                Err(_) => return Box::pin(err(ResponseError::MissingAuthorizationHeader.into())),
            },
            None => {
                return Box::pin(err(ResponseError::MissingAuthorizationHeader.into()));
            }
        };

        let authenticated = match self.acl {
            Authentication::Admin => data.api_keys.master.as_deref() == Some(auth_header),
            Authentication::Private => {
                data.api_keys.master.as_deref() == Some(auth_header)
                    || data.api_keys.private.as_deref() == Some(auth_header)
            }
            Authentication::Public => {
                data.api_keys.master.as_deref() == Some(auth_header)
                    || data.api_keys.private.as_deref() == Some(auth_header)
                    || data.api_keys.public.as_deref() == Some(auth_header)
            }
        };

        if authenticated {
            Box::pin(svc.call(req))
        } else {
            Box::pin(err(
                ResponseError::InvalidToken(auth_header.to_string()).into()
            ))
        }
    }
}
