use std::pin::Pin;
use std::task::{Context, Poll};

use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::web;
use actix_web::body::Body;
use futures::ready;
use futures::future::{ok, Future, Ready};
use actix_web::ResponseError as _;
use pin_project::pin_project;

use crate::Data;
use crate::error::{Error, ResponseError};

#[derive(Clone, Copy)]
pub enum Authentication {
    Public,
    Private,
    Admin,
}

impl<S: 'static> Transform<S, ServiceRequest> for Authentication
where
    S: Service<ServiceRequest, Response = ServiceResponse<Body>, Error = actix_web::Error>,
{
    type Response = ServiceResponse<Body>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = LoggingMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(LoggingMiddleware {
            acl: *self,
            service,
        })
    }
}

pub struct LoggingMiddleware<S> {
    acl: Authentication,
    service: S,
}

#[allow(clippy::type_complexity)]
impl<S> Service<ServiceRequest> for LoggingMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Body>, Error = actix_web::Error>,
{
    type Response = ServiceResponse<Body>;
    type Error = actix_web::Error;
    type Future = AuthenticationFuture<S>;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let data = req.app_data::<web::Data<Data>>().unwrap();

        if data.api_keys().master.is_none() {
            return AuthenticationFuture::Authenticated(self.service.call(req))
        }

        let auth_header = match req.headers().get("X-Meili-API-Key") {
            Some(auth) => match auth.to_str() {
                Ok(auth) => auth,
                Err(_) => {
                    return AuthenticationFuture::NoHeader(Some(req))
                }
            },
            None => {
                return AuthenticationFuture::NoHeader(Some(req))
            }
        };

        let authenticated = match self.acl {
            Authentication::Admin => data.api_keys().master.as_deref() == Some(auth_header),
            Authentication::Private => {
                data.api_keys().master.as_deref() == Some(auth_header)
                    || data.api_keys().private.as_deref() == Some(auth_header)
            }
            Authentication::Public => {
                data.api_keys().master.as_deref() == Some(auth_header)
                    || data.api_keys().private.as_deref() == Some(auth_header)
                    || data.api_keys().public.as_deref() == Some(auth_header)
            }
        };

        if authenticated {
            AuthenticationFuture::Authenticated(self.service.call(req))
        } else {
            AuthenticationFuture::Refused(Some(req))
        }
    }
}

#[pin_project(project = AuthProj)]
pub enum AuthenticationFuture<S>
where
    S: Service<ServiceRequest>,
{
    Authenticated(#[pin] S::Future),
    NoHeader(Option<ServiceRequest>),
    Refused(Option<ServiceRequest>),
}

impl<S> Future for AuthenticationFuture<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<Body>, Error = actix_web::Error>,
{
    type Output = Result<ServiceResponse<Body>, actix_web::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) ->Poll<Self::Output> {
        let this = self.project();
        match this {
            AuthProj::Authenticated(fut) => {
                match ready!(fut.poll(cx)) {
                    Ok(resp) => Poll::Ready(Ok(resp)),
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            AuthProj::NoHeader(req) => {
                match req.take() {
                    Some(req) => {
                        let response = ResponseError::from(Error::MissingAuthorizationHeader);
                        let response = response.error_response();
                        let response = req.into_response(response);
                        Poll::Ready(Ok(response))
                    }
                    // https://doc.rust-lang.org/nightly/std/future/trait.Future.html#panics
                    None => unreachable!("poll called again on ready future"),
                }
            }
            AuthProj::Refused(req) => {
                match req.take() {
                    Some(req) => {
                        let bad_token = req.headers()
                            .get("X-Meili-API-Key")
                            .map(|h| h.to_str().map(String::from).unwrap_or_default())
                            .unwrap_or_default();
                        let response = ResponseError::from(Error::InvalidToken(bad_token));
                        let response = response.error_response();
                        let response = req.into_response(response);
                        Poll::Ready(Ok(response))
                    }
                    // https://doc.rust-lang.org/nightly/std/future/trait.Future.html#panics
                    None => unreachable!("poll called again on ready future"),
                }
            }
        }
    }
}
