mod error;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;

use actix_web::FromRequest;
use futures::future::err;
use futures::future::{ok, Ready};

use crate::error::ResponseError;
use error::AuthenticationError;

macro_rules! create_policies {
    ($($name:ident), *) => {
        pub mod policies {
            use std::collections::HashSet;
            use crate::extractors::authentication::Policy;

            $(
                #[derive(Debug, Default)]
                pub struct $name {
                    inner: HashSet<Vec<u8>>
                }

                impl $name {
                    pub fn new() -> Self {
                        Self { inner: HashSet::new() }
                    }

                    pub fn add(&mut self, token: Vec<u8>) {
                        self.inner.insert(token);
                    }
                }

                impl Policy for $name {
                    fn authenticate(&self, token: &[u8]) -> bool {
                        self.inner.contains(token)
                    }
                }
            )*
        }
    };
}

create_policies!(Public, Private, Admin);

/// Instanciate a `Policies`, filled with the given policies.
macro_rules! init_policies {
    ($($name:ident), *) => {
        {
            let mut policies = crate::extractors::authentication::Policies::new();
            $(
                let policy = $name::new();
                policies.insert(policy);
            )*
            policies
        }
    };
}

/// Adds user to all specified policies.
macro_rules! create_users {
    ($policies:ident, $($user:expr => { $($policy:ty), * }), *) => {
        {
            $(
                $(
                    $policies.get_mut::<$policy>().map(|p| p.add($user.to_owned()));
                )*
            )*
        }
    };
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

#[derive(Debug)]
pub struct Policies {
    inner: HashMap<TypeId, Box<dyn Any>>,
}

impl Policies {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn insert<S: Policy + 'static>(&mut self, policy: S) {
        self.inner.insert(TypeId::of::<S>(), Box::new(policy));
    }

    pub fn get<S: Policy + 'static>(&self) -> Option<&S> {
        self.inner
            .get(&TypeId::of::<S>())
            .and_then(|p| p.downcast_ref::<S>())
    }

    pub fn get_mut<S: Policy + 'static>(&mut self) -> Option<&mut S> {
        self.inner
            .get_mut(&TypeId::of::<S>())
            .and_then(|p| p.downcast_mut::<S>())
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
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        match req.app_data::<Self::Config>() {
            Some(config) => match config {
                AuthConfig::NoAuth => match req.app_data::<D>().cloned() {
                    Some(data) => ok(Self {
                        data,
                        _marker: PhantomData,
                    }),
                    None => err(AuthenticationError::IrretrievableState.into()),
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
                                    None => err(AuthenticationError::IrretrievableState.into()),
                                }
                            } else {
                                let token = token.to_str().unwrap_or("unknown").to_string();
                                err(AuthenticationError::InvalidToken(token).into())
                            }
                        }
                        None => err(AuthenticationError::MissingAuthorizationHeader.into()),
                    },
                    None => err(AuthenticationError::UnknownPolicy.into()),
                },
            },
            None => err(AuthenticationError::IrretrievableState.into()),
        }
    }
}
