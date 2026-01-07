use std::ops::Deref;

pub use ureq::{
    AsSendBody, Body, BodyBuilder, BodyWithConfig, Error, Proxy, ProxyBuilder, ProxyProtocol,
    RequestBuilder, RequestExt, ResponseExt, SendBody, Timeout, http, middleware, tls, typestate,
    unversioned
};

use crate::policy::IpPolicy;

pub mod config;
mod resolver;

#[derive(Debug, Clone)]
pub struct Agent {
    inner: ::ureq::Agent,
}

impl Agent {
    pub fn new_with_config(config: config::Config, ip_policy: IpPolicy) -> Self {
        Self {
            inner: ::ureq::Agent::with_parts(
                config.0,
                unversioned::transport::DefaultConnector::default(),
                resolver::ExternalRequestResolver::new(ip_policy),
            ),
        }
    }
}

impl Deref for Agent {
    type Target = ::ureq::Agent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
