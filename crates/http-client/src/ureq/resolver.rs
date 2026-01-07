use ::ureq::unversioned::resolver::{DefaultResolver, Resolver};

use crate::policy::IpPolicy;

#[derive(Debug)]
pub struct ExternalRequestResolver {
    inner: DefaultResolver,
    ip_policy: IpPolicy,
}

impl ExternalRequestResolver {
    pub fn new(ip_policy: IpPolicy) -> Self {
        Self { inner: DefaultResolver::default(), ip_policy }
    }
}

impl Resolver for ExternalRequestResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ureq::unversioned::resolver::ResolvedSocketAddrs, ureq::Error> {
        let resolved = self.inner.resolve(uri, config, timeout)?;
        for socket_addr in &resolved {
            if let Err(_) = self.ip_policy.check_socket_addr(*socket_addr) {
                return Err(ureq::Error::BadUri("Rejected URI".into()));
            }
        }
        return Ok(resolved);
    }
}
