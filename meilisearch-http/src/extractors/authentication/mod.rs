mod error;

use std::marker::PhantomData;
use std::ops::Deref;

use actix_web::FromRequest;
use futures::future::err;
use futures::future::{ok, Ready};
use meilisearch_error::ResponseError;

use error::AuthenticationError;
use meilisearch_auth::{AuthController, AuthFilter};

pub struct GuardedData<T, D> {
    data: D,
    filters: AuthFilter,
    _marker: PhantomData<T>,
}

impl<T, D> GuardedData<T, D> {
    pub fn filters(&self) -> &AuthFilter {
        &self.filters
    }
}

impl<T, D> Deref for GuardedData<T, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<P: Policy + 'static, D: 'static + Clone> FromRequest for GuardedData<P, D> {
    type Config = ();

    type Error = ResponseError;

    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        match req.app_data::<AuthController>().cloned() {
            Some(auth) => match req
                .headers()
                .get("Authorization")
                .map(|type_token| type_token.to_str().unwrap_or_default().splitn(2, ' '))
            {
                Some(mut type_token) => match type_token.next() {
                    Some("Bearer") => {
                        // TODO: find a less hardcoded way?
                        let index = req.match_info().get("index_uid");
                        let token = type_token.next().unwrap_or("unknown");
                        match P::authenticate(auth, token, index) {
                            Some(filters) => match req.app_data::<D>().cloned() {
                                Some(data) => ok(Self {
                                    data,
                                    filters,
                                    _marker: PhantomData,
                                }),
                                None => err(AuthenticationError::IrretrievableState.into()),
                            },
                            None => {
                                let token = token.to_string();
                                err(AuthenticationError::InvalidToken(token).into())
                            }
                        }
                    }
                    _otherwise => err(AuthenticationError::MissingAuthorizationHeader.into()),
                },
                None => match P::authenticate(auth, "", None) {
                    Some(filters) => match req.app_data::<D>().cloned() {
                        Some(data) => ok(Self {
                            data,
                            filters,
                            _marker: PhantomData,
                        }),
                        None => err(AuthenticationError::IrretrievableState.into()),
                    },
                    None => err(AuthenticationError::MissingAuthorizationHeader.into()),
                },
            },
            None => err(AuthenticationError::IrretrievableState.into()),
        }
    }
}

pub trait Policy {
    fn authenticate(auth: AuthController, token: &str, index: Option<&str>) -> Option<AuthFilter>;
}

pub mod policies {
    use crate::extractors::authentication::Policy;
    use meilisearch_auth::{Action, AuthController, AuthFilter};
    // reexport actions in policies in order to be used in routes configuration.
    pub use meilisearch_auth::actions;

    pub struct MasterPolicy;

    impl Policy for MasterPolicy {
        fn authenticate(
            auth: AuthController,
            token: &str,
            _index: Option<&str>,
        ) -> Option<AuthFilter> {
            if let Some(master_key) = auth.get_master_key() {
                if master_key == token {
                    return Some(AuthFilter::default());
                }
            }

            None
        }
    }

    pub struct ActionPolicy<const A: u8>;

    impl<const A: u8> Policy for ActionPolicy<A> {
        fn authenticate(
            auth: AuthController,
            token: &str,
            index: Option<&str>,
        ) -> Option<AuthFilter> {
            // authenticate if token is the master key.
            if auth.get_master_key().map_or(true, |mk| mk == token) {
                return Some(AuthFilter::default());
            }

            // authenticate if token is allowed.
            if let Some(action) = Action::from_repr(A) {
                let index = index.map(|i| i.as_bytes());
                if let Ok(true) = auth.authenticate(token.as_bytes(), action, index) {
                    return auth.get_key_filters(token).ok();
                }
            }

            None
        }
    }
}
