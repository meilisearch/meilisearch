use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use actix_service::{Service, Transform};
use actix_web::{dev::ServiceRequest, dev::ServiceResponse, web};
use futures::future::{err, ok, Future, Ready};
use actix_web::error::ResponseError as _;
use actix_web::dev::Body;

use crate::error::{Error, ResponseError};
use crate::Data;

#[derive(Clone)]
pub enum Authentication {
    Public,
    Private,
    Admin,
}

impl<S: 'static> Transform<S> for Authentication
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<Body>, Error = actix_web::Error>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<Body>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = LoggingMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(LoggingMiddleware {
            acl: self.clone(),
            service: Rc::new(RefCell::new(service)),
        })
    }
}

pub struct LoggingMiddleware<S> {
    acl: Authentication,
    service: Rc<RefCell<S>>,
}

#[allow(clippy::type_complexity)]
impl<S> Service for LoggingMiddleware<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<Body>, Error = actix_web::Error> + 'static,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<Body>;
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        let mut svc = self.service.clone();
        // This unwrap is left because this error should never appear. If that's the case, then
        // it means that actix-web has an issue or someone changes the type `Data`.
        let data = req.app_data::<web::Data<Data>>().unwrap();

        if data.api_keys.master.is_none() {
            return Box::pin(svc.call(req));
        }

        let auth_header = match req.headers().get("X-Meili-API-Key") {
            Some(auth) => match auth.to_str() {
                Ok(auth) => auth,
                Err(_) => {
                    let error = ResponseError::from(Error::MissingAuthorizationHeader).error_response();
                    let (request, _) = req.into_parts();
                    return Box::pin(ok(ServiceResponse::new(request, error)))
                }
            },
            None => {
                return Box::pin(err(ResponseError::from(Error::MissingAuthorizationHeader).into()));
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
            let error = ResponseError::from(Error::InvalidToken(auth_header.to_string())).error_response();
            let (request, _) = req.into_parts();
            return Box::pin(ok(ServiceResponse::new(request, error)))
        }
    }
}
