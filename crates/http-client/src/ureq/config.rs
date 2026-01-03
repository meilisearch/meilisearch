use std::ops::Deref;

pub use ureq::config::{AutoHeaderValue, IpFamily, RedirectAuthHeaders, Timeouts};

use crate::policy::Policy;

type InternalConfig = ::ureq::config::ConfigBuilder<::ureq::typestate::AgentScope>;

#[derive(Debug, Default, Clone)]
pub struct Config(pub(super) ::ureq::config::Config);

pub struct ConfigBuilder(InternalConfig);

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder(::ureq::config::Config::builder())
    }

    pub fn new_agent(&self, ip_policy: Policy) -> super::Agent {
        super::Agent::new_with_config(self.clone(), ip_policy)
    }
}

impl Deref for Config {
    type Target = ::ureq::config::Config;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ConfigBuilder {
    pub fn build(self) -> Config {
        Config(self.0.build())
    }

    pub fn prepare<F>(self, f: F) -> Self
    where
        F: FnOnce(InternalConfig) -> InternalConfig,
    {
        Self(f(self.0))
    }
}
