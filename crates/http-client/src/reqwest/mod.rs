pub use ::reqwest::{
    Body, Client as DangerousClient, IntoUrl, Method, NoProxy, Proxy, Request, Response,
    ResponseBuilderExt, StatusCode, Upgraded, Url, Version, header, redirect, retry, tls,
};

pub mod error;
mod request;
mod resolver;

pub use error::{Error, Result};
pub use request::RequestBuilder;

use crate::policy::Policy;

#[derive(Clone)]
pub struct Client {
    inner: ::reqwest::Client,
    ip_policy: Policy,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder(::reqwest::Client::builder())
    }

    /// Convenience method to make a `GET` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn get<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.get(url))
    }

    /// Convenience method to make a `POST` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn post<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.post(url))
    }

    /// Convenience method to make a `PUT` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn put<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.put(url))
    }

    /// Convenience method to make a `PATCH` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn patch<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.patch(url))
    }

    /// Convenience method to make a `DELETE` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn delete<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.delete(url))
    }

    /// Convenience method to make a `HEAD` request to a URL.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn head<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.head(url))
    }

    /// Start building a `Request` with the `Method` and `Url`.
    ///
    /// Returns a `RequestBuilder`, which will allow setting headers and
    /// the request body before sending.
    ///
    /// # Errors
    ///
    /// This method fails whenever the supplied `Url` cannot be parsed.
    #[inline(always)]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        RequestBuilder::from_client_inner(&self, self.inner.request(method, url))
    }

    /// Executes a `Request`.
    ///
    /// A `Request` can be built manually with `Request::new()` or obtained
    /// from a RequestBuilder with `RequestBuilder::build()`.
    ///
    /// You should prefer to use the `RequestBuilder` and
    /// `RequestBuilder::send()`.
    ///
    /// # Errors
    ///
    /// This method fails if there was an error while sending request,
    /// redirect loop was detected or redirect limit was exhausted.
    #[inline(always)]
    pub fn execute(&self, request: Request) -> impl Future<Output = Result<Response>> {
        async {
            self.ip_policy.check_ip_in_hostname(request.url())?;
            Ok(self.inner.execute(request).await?)
        }
    }
}

#[must_use]
#[derive(Debug, Default)]
pub struct ClientBuilder(::reqwest::ClientBuilder);

impl ClientBuilder {
    /// Constructs a new `ClientBuilder`.
    ///
    /// This is the same as `Client::builder()`.
    pub fn new() -> Self {
        Self(::reqwest::ClientBuilder::new())
    }
}

impl ClientBuilder {
    /// Returns a `Client` that uses this `ClientBuilder` configuration.
    ///
    /// # Errors
    ///
    /// This method fails if a TLS backend cannot be initialized, or the resolver
    /// cannot load the system configuration.
    pub fn build(self) -> Result<Client> {
        self.build_with_policy(Policy::deny_all_local_ips(), redirect::Policy::default())
    }

    pub fn build_with_policy(
        self,
        ip_policy: Policy,
        redirect_policy: redirect::Policy,
    ) -> Result<Client> {
        let redirect_policy = {
            let ip_policy = ip_policy.clone();
            redirect::Policy::custom(move |attempt| {
                if let Err(err) = ip_policy.check_ip_in_hostname(attempt.url()) {
                    return attempt.error(err);
                }

                redirect_policy.redirect(attempt)
            })
        };
        let builder = self
            .0
            .dns_resolver2(resolver::ExternalRequestResolver::new(ip_policy.clone()))
            .redirect(redirect_policy);
        Ok(Client { inner: builder.build()?, ip_policy })
    }

    pub fn danger_build_no_ip_policy(self) -> Result<DangerousClient, error::ReqwestError> {
        self.0.build()
    }

    /// Returns a `ClientBuilder` modified with the modifications done to the internal builder.
    ///
    /// This spares this crate from redeclaring all methods from the internal builder as wrapper methods,
    /// while providing protection against accidental misuses of the internal builder.
    pub fn prepare<F>(self, f: F) -> ClientBuilder
    where
        F: FnOnce(::reqwest::ClientBuilder) -> ::reqwest::ClientBuilder,
    {
        Self(f(self.0))
    }

    pub fn get(&self) -> &::reqwest::ClientBuilder {
        &self.0
    }
}
