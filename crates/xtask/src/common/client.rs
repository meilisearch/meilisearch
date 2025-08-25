use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Client {
    base_url: Option<String>,
    client: reqwest::Client,
}

impl Client {
    pub fn new(
        base_url: Option<String>,
        api_key: Option<&str>,
        timeout: Option<std::time::Duration>,
    ) -> anyhow::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(api_key) = api_key {
            headers.append(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}"))
                    .context("Invalid authorization header")?,
            );
        }

        let client = reqwest::ClientBuilder::new().default_headers(headers);
        let client = if let Some(timeout) = timeout { client.timeout(timeout) } else { client };
        let client = client.build()?;
        Ok(Self { base_url, client })
    }

    pub fn request(&self, method: reqwest::Method, route: &str) -> reqwest::RequestBuilder {
        if let Some(base_url) = &self.base_url {
            if route.is_empty() {
                self.client.request(method, base_url)
            } else {
                self.client.request(method, format!("{}/{}", base_url, route))
            }
        } else {
            self.client.request(method, route)
        }
    }

    pub fn get(&self, route: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, route)
    }

    pub fn put(&self, route: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::PUT, route)
    }

    pub fn post(&self, route: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, route)
    }

    pub fn delete(&self, route: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::DELETE, route)
    }

    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Method {
    Get,
    Post,
    Patch,
    Delete,
    Put,
}

impl From<Method> for reqwest::Method {
    fn from(value: Method) -> Self {
        match value {
            Method::Get => Self::GET,
            Method::Post => Self::POST,
            Method::Patch => Self::PATCH,
            Method::Delete => Self::DELETE,
            Method::Put => Self::PUT,
        }
    }
}
