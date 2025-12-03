//! URL fetching for embedding extraction.
//!
//! This module provides functionality to fetch content from URLs during embedding extraction.
//! The fetched content is converted to base64 and made available as virtual fields for
//! template rendering, without being persisted in the database.

use std::collections::BTreeMap;
use std::io::Read;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;

use super::settings::{FetchOptions, FetchOutputFormat, FetchUrlMapping};
use crate::progress::UrlFetcherStats;
use crate::vector::error::EmbedError;

/// Default timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 10_000;

/// Default maximum content size (10 MB).
const DEFAULT_MAX_SIZE: usize = 10 * 1024 * 1024;

/// Default number of retries.
const DEFAULT_RETRIES: u32 = 2;

/// Resolved fetch configuration for a single URL field.
#[derive(Debug, Clone)]
pub struct ResolvedFetchMapping {
    /// The document field containing the URL to fetch.
    pub input: String,
    /// The virtual field name for the fetched content.
    pub output: String,
    /// Request timeout in milliseconds.
    pub timeout_ms: u64,
    /// Maximum content size in bytes.
    pub max_size: usize,
    /// Number of retry attempts.
    pub retries: u32,
    /// Output format for the fetched content.
    pub output_format: FetchOutputFormat,
}

impl ResolvedFetchMapping {
    /// Create a resolved mapping from a FetchUrlMapping and default FetchOptions.
    pub fn from_mapping(mapping: &FetchUrlMapping, defaults: &FetchOptions) -> Self {
        let timeout_ms = mapping.timeout.or(defaults.timeout).unwrap_or(DEFAULT_TIMEOUT_MS);

        let max_size = mapping
            .max_size
            .as_ref()
            .or(defaults.max_size.as_ref())
            .map(|s| parse_size(s))
            .unwrap_or(DEFAULT_MAX_SIZE);

        let retries = mapping.retries.or(defaults.retries).unwrap_or(DEFAULT_RETRIES);

        let output_format =
            mapping.output_format.or(defaults.output_format).unwrap_or(FetchOutputFormat::Base64);

        Self {
            input: mapping.input.clone(),
            output: mapping.output.clone(),
            timeout_ms,
            max_size,
            retries,
            output_format,
        }
    }
}

/// Parse a human-readable size string (e.g., "10MB", "5KB") into bytes.
fn parse_size(size_str: &str) -> usize {
    let size_str = size_str.trim().to_uppercase();

    // Try to find where the numeric part ends
    let numeric_end =
        size_str.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(size_str.len());

    let (num_str, unit) = size_str.split_at(numeric_end);
    let num: f64 = num_str.parse().unwrap_or(10.0);
    let unit = unit.trim();

    let multiplier = match unit {
        "B" | "" => 1,
        "KB" | "K" => 1024,
        "MB" | "M" => 1024 * 1024,
        "GB" | "G" => 1024 * 1024 * 1024,
        _ => 1024 * 1024, // Default to MB
    };

    (num * multiplier as f64) as usize
}

/// Guess the MIME type from a URL based on its file extension.
fn guess_mime_type(url: &str) -> &'static str {
    // Extract the path from the URL and get the extension
    let path = url.split('?').next().unwrap_or(url);
    let ext = path.rsplit('.').next().unwrap_or("").to_lowercase();

    match ext.as_str() {
        // Images
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "ico" => "image/x-icon",
        "tiff" | "tif" => "image/tiff",
        "avif" => "image/avif",
        // Documents
        "pdf" => "application/pdf",
        // Audio
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        // Video
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        // Default to binary
        _ => "application/octet-stream",
    }
}

/// URL Fetcher for downloading content and converting to base64.
#[derive(Debug, Clone)]
pub struct UrlFetcher {
    client: ureq::Agent,
    allowed_domains: Vec<String>,
    stats: Option<Arc<UrlFetcherStats>>,
}

impl UrlFetcher {
    /// Create a new URL fetcher with the given options.
    pub fn new(options: &FetchOptions) -> Self {
        let timeout = Duration::from_millis(options.timeout.unwrap_or(DEFAULT_TIMEOUT_MS));

        let client = ureq::AgentBuilder::new()
            .timeout(timeout)
            .max_idle_connections(10)
            .max_idle_connections_per_host(5)
            .build();

        Self { client, allowed_domains: options.allowed_domains.clone(), stats: None }
    }

    /// Create a new URL fetcher with the given options and statistics tracking.
    pub fn with_stats(options: &FetchOptions, stats: Arc<UrlFetcherStats>) -> Self {
        let timeout = Duration::from_millis(options.timeout.unwrap_or(DEFAULT_TIMEOUT_MS));

        let client = ureq::AgentBuilder::new()
            .timeout(timeout)
            .max_idle_connections(10)
            .max_idle_connections_per_host(5)
            .build();

        Self { client, allowed_domains: options.allowed_domains.clone(), stats: Some(stats) }
    }

    /// Record a successful fetch in stats.
    fn record_success(&self) {
        if let Some(stats) = &self.stats {
            stats.total_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a failed fetch in stats.
    fn record_error(&self, error_msg: &str) {
        if let Some(stats) = &self.stats {
            stats.total_count.fetch_add(1, Ordering::Relaxed);
            let mut guard = stats.errors.write().unwrap_or_else(|p| p.into_inner());
            guard.0 = Some(error_msg.to_string());
            guard.1 += 1;
        }
    }

    /// Check if a URL is allowed based on domain restrictions.
    fn is_domain_allowed(&self, url: &str) -> Result<bool, EmbedError> {
        // If no domains specified, nothing is allowed
        if self.allowed_domains.is_empty() {
            return Ok(false);
        }

        // If wildcard is present, all domains are allowed
        if self.allowed_domains.iter().any(|d| d == "*") {
            return Ok(true);
        }

        // Parse the URL to get the host
        let parsed = url::Url::parse(url)
            .map_err(|e| EmbedError::url_fetch_error(format!("Invalid URL: {}", e)))?;

        let host =
            parsed.host_str().ok_or_else(|| EmbedError::url_fetch_error("URL has no host"))?;

        // Check against allowed domains
        for allowed in &self.allowed_domains {
            if allowed.starts_with("*.") {
                // Wildcard subdomain match
                let suffix = &allowed[1..]; // Remove the leading *
                if host.ends_with(suffix) || host == &allowed[2..] {
                    return Ok(true);
                }
            } else if host == allowed {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Check if an IP address is private/internal (SSRF protection).
    fn is_private_ip(addr: &IpAddr) -> bool {
        match addr {
            IpAddr::V4(ip) => {
                ip.is_loopback()
                    || ip.is_private()
                    || ip.is_link_local()
                    || ip.is_broadcast()
                    || ip.is_unspecified()
                    // 169.254.0.0/16 (link-local)
                    || (ip.octets()[0] == 169 && ip.octets()[1] == 254)
            }
            IpAddr::V6(ip) => {
                ip.is_loopback() || ip.is_unspecified()
                // Note: More comprehensive IPv6 private range checking could be added
            }
        }
    }

    /// Validate that the URL doesn't resolve to a private IP (SSRF protection).
    fn validate_not_private(&self, url: &str) -> Result<(), EmbedError> {
        let parsed = url::Url::parse(url)
            .map_err(|e| EmbedError::url_fetch_error(format!("Invalid URL: {}", e)))?;

        let host =
            parsed.host_str().ok_or_else(|| EmbedError::url_fetch_error("URL has no host"))?;

        let port = parsed.port().unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });

        // Try to resolve the hostname
        let addr_str = format!("{}:{}", host, port);
        if let Ok(addrs) = addr_str.to_socket_addrs() {
            for addr in addrs {
                if Self::is_private_ip(&addr.ip()) {
                    return Err(EmbedError::url_fetch_error(format!(
                        "URL resolves to private IP address: {}",
                        addr.ip()
                    )));
                }
            }
        }

        Ok(())
    }

    /// Fetch a URL and return its content formatted according to the mapping's output_format.
    pub fn fetch_as_base64(
        &self,
        url: &str,
        mapping: &ResolvedFetchMapping,
    ) -> Result<String, EmbedError> {
        // Check domain allowlist
        if !self.is_domain_allowed(url)? {
            return Err(EmbedError::url_fetch_error(format!(
                "Domain not in allowed list for URL: {}",
                url
            )));
        }

        // SSRF protection: validate not resolving to private IP
        self.validate_not_private(url)?;

        let mut last_error = None;

        for attempt in 0..=mapping.retries {
            match self.do_fetch(url, mapping.max_size) {
                Ok((content, content_type)) => {
                    // Convert to base64
                    let base64 = base64::engine::general_purpose::STANDARD.encode(&content);

                    // Format according to output_format
                    let result = match mapping.output_format {
                        FetchOutputFormat::Base64 => base64,
                        FetchOutputFormat::DataUri => {
                            // Use the content type from the response, or guess from URL
                            let mime_type =
                                content_type.unwrap_or_else(|| guess_mime_type(url).to_string());
                            format!("data:{};base64,{}", mime_type, base64)
                        }
                    };
                    self.record_success();
                    return Ok(result);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < mapping.retries {
                        // Exponential backoff
                        let delay = Duration::from_millis(10u64.pow(attempt + 1));
                        std::thread::sleep(delay);
                    }
                }
            }
        }

        let error =
            last_error.unwrap_or_else(|| EmbedError::url_fetch_error("Unknown fetch error"));
        self.record_error(&error.to_string());
        Err(error)
    }

    /// Perform the actual HTTP fetch. Returns (content, content_type).
    fn do_fetch(
        &self,
        url: &str,
        max_size: usize,
    ) -> Result<(Vec<u8>, Option<String>), EmbedError> {
        let response = self
            .client
            .get(url)
            .call()
            .map_err(|e| EmbedError::url_fetch_error(format!("HTTP request failed: {}", e)))?;

        // Check Content-Length if available
        if let Some(len) = response.header("Content-Length") {
            if let Ok(len) = len.parse::<usize>() {
                if len > max_size {
                    return Err(EmbedError::url_fetch_error(format!(
                        "Content too large: {} bytes (max: {} bytes)",
                        len, max_size
                    )));
                }
            }
        }

        // Get content type from response
        let content_type = response.header("Content-Type").map(|s| {
            // Strip charset and other parameters, keep just the mime type
            s.split(';').next().unwrap_or(s).trim().to_string()
        });

        // Read the response body with size limit
        let mut reader = response.into_reader().take(max_size as u64 + 1);
        let mut content = Vec::new();
        reader
            .read_to_end(&mut content)
            .map_err(|e| EmbedError::url_fetch_error(format!("Failed to read response: {}", e)))?;

        if content.len() > max_size {
            return Err(EmbedError::url_fetch_error(format!(
                "Content too large: {} bytes (max: {} bytes)",
                content.len(),
                max_size
            )));
        }

        Ok((content, content_type))
    }

    /// Fetch multiple URLs from a document and return a map of virtual field names to base64 content.
    pub fn fetch_document_urls(
        &self,
        document: &serde_json::Value,
        mappings: &[ResolvedFetchMapping],
    ) -> BTreeMap<String, Result<String, EmbedError>> {
        let mut results = BTreeMap::new();

        for mapping in mappings {
            // Get the URL from the document
            let url = document.get(&mapping.input).and_then(|v| v.as_str());

            match url {
                Some(url) if !url.is_empty() => {
                    let result = self.fetch_as_base64(url, mapping);
                    results.insert(mapping.output.clone(), result);
                }
                Some(_) | None => {
                    // URL field is empty or missing - skip silently
                }
            }
        }

        results
    }
}

/// Create resolved fetch mappings from settings.
pub fn resolve_fetch_mappings(
    fetch_url: &[FetchUrlMapping],
    fetch_options: &FetchOptions,
) -> Vec<ResolvedFetchMapping> {
    fetch_url.iter().map(|m| ResolvedFetchMapping::from_mapping(m, fetch_options)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("10MB"), 10 * 1024 * 1024);
        assert_eq!(parse_size("5KB"), 5 * 1024);
        assert_eq!(parse_size("1GB"), 1024 * 1024 * 1024);
        assert_eq!(parse_size("100B"), 100);
        assert_eq!(parse_size("10M"), 10 * 1024 * 1024);
        assert_eq!(parse_size("5K"), 5 * 1024);
    }

    #[test]
    fn test_is_private_ip() {
        use std::net::Ipv4Addr;

        assert!(UrlFetcher::is_private_ip(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert!(UrlFetcher::is_private_ip(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(UrlFetcher::is_private_ip(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(UrlFetcher::is_private_ip(&IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(!UrlFetcher::is_private_ip(&IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn test_domain_allowed() {
        let options = FetchOptions {
            allowed_domains: vec!["example.com".to_string(), "*.cdn.example.com".to_string()],
            ..Default::default()
        };
        let fetcher = UrlFetcher::new(&options);

        assert!(fetcher.is_domain_allowed("https://example.com/image.jpg").unwrap());
        assert!(fetcher.is_domain_allowed("https://images.cdn.example.com/image.jpg").unwrap());
        assert!(!fetcher.is_domain_allowed("https://other.com/image.jpg").unwrap());
    }

    #[test]
    fn test_wildcard_all_domains() {
        let options = FetchOptions { allowed_domains: vec!["*".to_string()], ..Default::default() };
        let fetcher = UrlFetcher::new(&options);

        assert!(fetcher.is_domain_allowed("https://example.com/image.jpg").unwrap());
        assert!(fetcher.is_domain_allowed("https://any-domain.org/image.jpg").unwrap());
    }

    #[test]
    fn test_empty_allowed_domains() {
        let options = FetchOptions { allowed_domains: vec![], ..Default::default() };
        let fetcher = UrlFetcher::new(&options);

        assert!(!fetcher.is_domain_allowed("https://example.com/image.jpg").unwrap());
    }

    #[test]
    fn test_guess_mime_type() {
        assert_eq!(guess_mime_type("https://example.com/image.jpg"), "image/jpeg");
        assert_eq!(guess_mime_type("https://example.com/image.jpeg"), "image/jpeg");
        assert_eq!(guess_mime_type("https://example.com/image.png"), "image/png");
        assert_eq!(guess_mime_type("https://example.com/image.gif"), "image/gif");
        assert_eq!(guess_mime_type("https://example.com/image.webp"), "image/webp");
        assert_eq!(guess_mime_type("https://example.com/doc.pdf"), "application/pdf");
        assert_eq!(guess_mime_type("https://example.com/image.PNG"), "image/png");
        assert_eq!(guess_mime_type("https://example.com/image.jpg?width=100"), "image/jpeg");
        assert_eq!(guess_mime_type("https://example.com/unknown"), "application/octet-stream");
    }
}
