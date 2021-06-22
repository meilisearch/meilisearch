use std::any::{Any, TypeId};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::Deref;

use log::debug;
use actix_web::{web, FromRequest, HttpResponse};
use futures::future::{err, ok, Ready};
use serde::Deserialize;
use serde_json::Value;

use crate::error::{AuthenticationError, ResponseError};
use crate::index::{default_crop_length, SearchQuery, DEFAULT_SEARCH_LIMIT};
use crate::routes::IndexParam;
use crate::Data;

struct Public;

impl Policy for Public {
    fn authenticate(&self, _token: &[u8]) -> bool {
        true
    }
}

struct GuardedData<T, D> {
    data: D,
    _marker: PhantomData<T>,
}

trait Policy {
    fn authenticate(&self, token: &[u8]) -> bool;
}

struct Policies {
    inner: HashMap<TypeId, Box<dyn Any>>,
}

impl Policies {
    fn new() -> Self {
        Self { inner: HashMap::new() }
    }

    fn insert<S: Policy + 'static>(&mut self, policy: S) {
        self.inner.insert(TypeId::of::<S>(), Box::new(policy));
    }

    fn get<S: Policy + 'static>(&self) -> Option<&S> {
        self.inner
            .get(&TypeId::of::<S>())
            .and_then(|p| p.downcast_ref::<S>())
    }
}

enum AuthConfig {
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

impl<T, D> Deref for GuardedData<T, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub fn services(cfg: &mut web::ServiceConfig) {
    let mut policies = Policies::new();
    policies.insert(Public);
    cfg.service(
        web::resource("/indexes/{index_uid}/search")
            .app_data(AuthConfig::Auth(policies))
            .route(web::get().to(search_with_url_query))
            .route(web::post().to(search_with_post)),
    );
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQueryGet {
    q: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
    attributes_to_crop: Option<String>,
    #[serde(default = "default_crop_length")]
    crop_length: usize,
    attributes_to_highlight: Option<String>,
    filter: Option<String>,
    #[serde(default = "Default::default")]
    matches: bool,
    facets_distribution: Option<String>,
}

impl From<SearchQueryGet> for SearchQuery {
    fn from(other: SearchQueryGet) -> Self {
        let attributes_to_retrieve = other
            .attributes_to_retrieve
            .map(|attrs| attrs.split(',').map(String::from).collect::<BTreeSet<_>>());

        let attributes_to_crop = other
            .attributes_to_crop
            .map(|attrs| attrs.split(',').map(String::from).collect::<Vec<_>>());

        let attributes_to_highlight = other
            .attributes_to_highlight
            .map(|attrs| attrs.split(',').map(String::from).collect::<HashSet<_>>());

        let facets_distribution = other
            .facets_distribution
            .map(|attrs| attrs.split(',').map(String::from).collect::<Vec<_>>());

        let filter = match other.filter {
            Some(f) => match serde_json::from_str(&f) {
                Ok(v) => Some(v),
                _ => Some(Value::String(f)),
            },
            None => None,
        };

        Self {
            q: other.q,
            offset: other.offset,
            limit: other.limit.unwrap_or(DEFAULT_SEARCH_LIMIT),
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length: other.crop_length,
            attributes_to_highlight,
            filter,
            matches: other.matches,
            facets_distribution,
        }
    }
}

async fn search_with_url_query(
    data: GuardedData<Public, Data>,
    path: web::Path<IndexParam>,
    params: web::Query<SearchQueryGet>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let query = params.into_inner().into();
    let search_result = data.search(path.into_inner().index_uid, query).await?;
    debug!("returns: {:?}", search_result);
    Ok(HttpResponse::Ok().json(search_result))
}

async fn search_with_post(
    data: GuardedData<Public, Data>,
    path: web::Path<IndexParam>,
    params: web::Json<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    debug!("search called with params: {:?}", params);
    let search_result = data
        .search(path.into_inner().index_uid, params.into_inner())
        .await?;
    debug!("returns: {:?}", search_result);
    Ok(HttpResponse::Ok().json(search_result))
}
