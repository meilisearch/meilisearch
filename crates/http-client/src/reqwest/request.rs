use ::reqwest::{Request, RequestBuilder as InnerRequestBuilder};
use reqwest::Response;

use crate::policy::IpPolicy;

/// A builder to construct the properties of a `Request`.
///
/// To construct a `RequestBuilder`, refer to the `Client` documentation.
#[must_use = "RequestBuilder does nothing until you 'send' it"]
#[derive(Debug)]
pub struct RequestBuilder {
    pub(crate) inner: InnerRequestBuilder,
    pub(crate) ip_policy: IpPolicy,
}

impl RequestBuilder {
    /// Assemble a builder starting from an existing `Client` and a `Request`.
    pub fn from_parts(client: crate::reqwest::Client, request: Request) -> RequestBuilder {
        let crate::reqwest::Client { inner, ip_policy } = client;
        let inner = InnerRequestBuilder::from_parts(inner, request);
        Self { inner, ip_policy }
    }

    pub(crate) fn from_client_inner(
        client: &crate::reqwest::Client,
        inner: InnerRequestBuilder,
    ) -> RequestBuilder {
        Self { inner, ip_policy: client.ip_policy.clone() }
    }

    /// Build a `Request`, which can be inspected, modified and executed with
    /// `Client::execute()`.
    pub fn build(self) -> crate::reqwest::Result<Request> {
        Ok(self.inner.build()?)
    }

    /// Build a `Request`, which can be inspected, modified and executed with
    /// `Client::execute()`.
    ///
    /// This is similar to [`RequestBuilder::build()`], but also returns the
    /// embedded `Client`.
    pub fn build_split(self) -> (crate::reqwest::Client, crate::reqwest::Result<Request>) {
        let (client, request) = self.inner.build_split();
        let client = crate::reqwest::Client { inner: client, ip_policy: self.ip_policy };
        let request = request.map_err(crate::reqwest::Error::from);
        (client, request)
    }

    /// Constructs the Request and sends it to the target URL, returning a
    /// future Response.
    ///
    /// # Errors
    ///
    /// This method fails if there was an error while sending request,
    /// redirect loop was detected or redirect limit was exhausted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use reqwest::Error;
    /// #
    /// # async fn run() -> Result<(), Error> {
    /// let response = reqwest::Client::new()
    ///     .get("https://hyper.rs")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send(self) -> impl Future<Output = crate::reqwest::Result<Response>> {
        async {
            let (client, request) = self.build_split();
            let request = request?;
            Ok(client.execute(request).await?)
        }
    }

    /// Attempt to clone the RequestBuilder.
    ///
    /// `None` is returned if the RequestBuilder can not be cloned,
    /// i.e. if the request body is a stream.
    ///
    /// # Examples
    ///
    /// ```
    /// # use reqwest::Error;
    /// #
    /// # fn run() -> Result<(), Error> {
    /// let client = reqwest::Client::new();
    /// let builder = client.post("http://httpbin.org/post")
    ///     .body("from a &str!");
    /// let clone = builder.try_clone();
    /// assert!(clone.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_clone(&self) -> Option<RequestBuilder> {
        self.inner.try_clone().map(|inner| Self { inner, ip_policy: self.ip_policy.clone() })
    }

    /// Returns a `RequestBuilder` modified with the modifications done to the internal builder.
    ///
    /// This spares this crate from redeclaring all methods from the internal builder as wrapper methods,
    /// while providing protection against accidental misuses of the internal builder.
    ///
    /// # Warning
    ///
    /// Do not directly send the `InnerRequestBuilder` inside of the closure
    pub fn prepare<F>(self, f: F) -> RequestBuilder
    where
        F: FnOnce(InnerRequestBuilder) -> InnerRequestBuilder,
    {
        Self { inner: f(self.inner), ip_policy: self.ip_policy }
    }
}
