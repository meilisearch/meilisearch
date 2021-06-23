use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::any::{Any, TypeId};

use actix_web::FromRequest;
use futures::future::err;
use futures::future::{Ready, ok};

use crate::error::{AuthenticationError, ResponseError};

pub struct Public;

impl Policy for Public {
    fn authenticate(&self, _token: &[u8]) -> bool {
        true
    }
}

pub struct GuardedData<T, D> {
    data: D,
    _marker: PhantomData<T>,
}

impl<T, D> Deref for GuardedData<T, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub trait Policy {
    fn authenticate(&self, token: &[u8]) -> bool;
}

pub struct Policies {
    inner: HashMap<TypeId, Box<dyn Any>>,
}

impl Policies {
    pub fn new() -> Self {
        Self { inner: HashMap::new() }
    }

    pub fn insert<S: Policy + 'static>(&mut self, policy: S) {
        self.inner.insert(TypeId::of::<S>(), Box::new(policy));
    }

    pub fn get<S: Policy + 'static>(&self) -> Option<&S> {
        self.inner
            .get(&TypeId::of::<S>())
            .and_then(|p| p.downcast_ref::<S>())
    }
}

impl Default for Policies {
    fn default() -> Self {
        Self::new()
    }
}

pub enum AuthConfig {
    NoAuth,
    Auth(Policies),
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::NoAuth
    }
}

impl<P: Policy + 'static, D: 'static + Clone> FromRequest for GuardedData<P, D> {
    type Config = AuthConfig;

    type Error = ResponseError;

    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_http::Payload,
    ) -> Self::Future {
        match req.app_data::<Self::Config>() {
            Some(config) => match config {
                AuthConfig::NoAuth => match req.app_data::<D>().cloned() {
                    Some(data) => ok(Self {
                        data,
                        _marker: PhantomData,
                    }),
                    None => todo!("Data not configured"),
                },
                AuthConfig::Auth(policies) => match policies.get::<P>() {
                    Some(policy) => match req.headers().get("x-meili-api-key") {
                        Some(token) => {
                            if policy.authenticate(token.as_bytes()) {
                                match req.app_data::<D>().cloned() {
                                    Some(data) => ok(Self {
                                        data,
                                        _marker: PhantomData,
                                    }),
                                    None => todo!("Data not configured"),
                                }
                            } else {
                                err(AuthenticationError::InvalidToken(String::from("hello")).into())
                            }
                        }
                        None => err(AuthenticationError::MissingAuthorizationHeader.into()),
                    },
                    None => todo!("no policy found"),
                },
            },
            None => todo!(),
        }
    }
}
