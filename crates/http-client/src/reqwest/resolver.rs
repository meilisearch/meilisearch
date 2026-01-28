use ::reqwest::dns::{Name, Resolve, Resolving};
use hyper_util::client::legacy::connect::dns::GaiResolver as HyperGaiResolver;
use tower_service::Service;

#[derive(Debug)]
pub struct ExternalRequestResolver {
    resolver: HyperGaiResolver,
    ip_policy: IpPolicy,
}

impl ExternalRequestResolver {
    pub fn new(ip_policy: IpPolicy) -> Self {
        Self { resolver: HyperGaiResolver::new(), ip_policy }
    }
}

use std::error::Error as StdError;

use crate::policy::IpPolicy;
type BoxError = Box<dyn StdError + Send + Sync>;

impl Resolve for ExternalRequestResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let mut this = self.resolver.clone();

        // HACK: reqwest's `Name` actually contains a hyper's `Name`,
        // but won't give us access, so we have to copy the name 😢
        // The unwrap cannot fire because the Name has already been validated.
        // (also, at time of writing, there is no actual validation)
        let name = name.as_str().parse().unwrap();

        let ip_policy = self.ip_policy.clone();

        Box::pin(async move {
            let addrs = this.call(name).await.map_err(|err| -> BoxError { Box::new(err) as _ })?;

            // HACK: as the returned `addrs` is an opaque iterator,
            // we must consume it check the individual IPs.
            let addrs: Result<Vec<_>, _> = addrs
                .map(|addr| -> Result<std::net::SocketAddr, BoxError> {
                    ip_policy.check_socket_addr(addr).map_err(Box::new)?;
                    Ok(addr)
                })
                .collect();

            let addrs = addrs?;

            Ok(Box::new(addrs.into_iter()) as _)
        })
    }
}
