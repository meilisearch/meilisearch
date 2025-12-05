//! URL fetching for embedding extraction.
//!
//! This module provides functionality to fetch content from URLs during embedding extraction.
//! The fetched content is converted to base64 and made available as virtual fields for
//! template rendering, without being persisted in the database.
//!
//! ## Nested Path Support
//!
//! The `input` field supports nested JSON paths:
//! - Simple field: `imageUrl`
//! - Nested field: `media.image.url`
//! - Array index: `images[0].url`
//! - Array wildcard: `images[].url` - fetches URLs from ALL array elements

use std::collections::BTreeMap;
use std::io::Read;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;

use super::settings::{FetchOutputFormat, FetchUrlMapping};
use crate::progress::UrlFetcherStats;
use crate::vector::error::EmbedError;

/// A segment of a JSON path.
#[derive(Debug, Clone, PartialEq)]
enum PathSegment {
    /// A field name, e.g., "images" in "images.url"
    Field(String),
    /// A specific array index, e.g., [0] in "images[0].url"
    ArrayIndex(usize),
    /// Array wildcard, e.g., [] in "images[].url" - matches all elements
    ArrayWildcard,
}

/// Parse a path string into segments.
///
/// Supported formats:
/// - `field` -> [Field("field")]
/// - `field.subfield` -> [Field("field"), Field("subfield")]
/// - `field[0].subfield` -> [Field("field"), ArrayIndex(0), Field("subfield")]
/// - `field[].subfield` -> [Field("field"), ArrayWildcard, Field("subfield")]
fn parse_path(path: &str) -> Vec<PathSegment> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '.' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Field(current.clone()));
                    current.clear();
                }
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Field(current.clone()));
                    current.clear();
                }
                // Parse array index or wildcard
                let mut index_str = String::new();
                while let Some(&next_c) = chars.peek() {
                    if next_c == ']' {
                        chars.next();
                        break;
                    }
                    index_str.push(chars.next().unwrap());
                }
                if index_str.is_empty() {
                    segments.push(PathSegment::ArrayWildcard);
                } else if let Ok(index) = index_str.parse::<usize>() {
                    segments.push(PathSegment::ArrayIndex(index));
                }
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        segments.push(PathSegment::Field(current));
    }

    segments
}

/// Extract values from a JSON document using a parsed path.
/// Returns a vector of values (multiple if array wildcard is used).
fn extract_values<'a>(
    value: &'a serde_json::Value,
    segments: &[PathSegment],
) -> Vec<&'a serde_json::Value> {
    if segments.is_empty() {
        return vec![value];
    }

    let (segment, rest) = segments.split_first().unwrap();

    match segment {
        PathSegment::Field(name) => {
            if let Some(child) = value.get(name) {
                extract_values(child, rest)
            } else {
                vec![]
            }
        }
        PathSegment::ArrayIndex(index) => {
            if let Some(child) = value.get(index) {
                extract_values(child, rest)
            } else {
                vec![]
            }
        }
        PathSegment::ArrayWildcard => {
            if let Some(arr) = value.as_array() {
                arr.iter().flat_map(|item| extract_values(item, rest)).collect()
            } else {
                vec![]
            }
        }
    }
}

/// Extract string URLs from a document using a path expression.
/// Returns multiple URLs if array wildcards are used.
///
/// # Examples
///
/// ```ignore
/// // Simple field
/// extract_urls(&doc, "imageUrl") // -> ["https://..."]
///
/// // Nested field
/// extract_urls(&doc, "media.image.url") // -> ["https://..."]
///
/// // Array with index
/// extract_urls(&doc, "images[0].url") // -> ["https://..."]
///
/// // Array wildcard (all elements)
/// extract_urls(&doc, "images[].url") // -> ["https://...", "https://...", ...]
/// ```
pub fn extract_urls(document: &serde_json::Value, path: &str) -> Vec<String> {
    let segments = parse_path(path);
    let values = extract_values(document, &segments);
    values.into_iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect()
}

/// Check if a path contains an array wildcard.
pub fn path_has_array_wildcard(path: &str) -> bool {
    parse_path(path).iter().any(|s| matches!(s, PathSegment::ArrayWildcard))
}

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
    /// Create a resolved mapping from a FetchUrlMapping.
    pub fn from_mapping(mapping: &FetchUrlMapping) -> Self {
        let timeout_ms = mapping.timeout.unwrap_or(DEFAULT_TIMEOUT_MS);
        let max_size = mapping.max_size.as_ref().map(|s| parse_size(s)).unwrap_or(DEFAULT_MAX_SIZE);
        let retries = mapping.retries.unwrap_or(DEFAULT_RETRIES);
        let output_format = mapping.output_format.unwrap_or(FetchOutputFormat::DataUri);

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
    /// Create a new URL fetcher with the given mapping configuration.
    pub fn new(mapping: &FetchUrlMapping) -> Self {
        let timeout = Duration::from_millis(mapping.timeout.unwrap_or(DEFAULT_TIMEOUT_MS));

        let client = ureq::AgentBuilder::new()
            .timeout(timeout)
            .max_idle_connections(10)
            .max_idle_connections_per_host(5)
            .build();

        Self { client, allowed_domains: mapping.allowed_domains.clone(), stats: None }
    }

    /// Create a new URL fetcher with the given mapping configuration and statistics tracking.
    pub fn with_stats(mapping: &FetchUrlMapping, stats: Arc<UrlFetcherStats>) -> Self {
        let timeout = Duration::from_millis(mapping.timeout.unwrap_or(DEFAULT_TIMEOUT_MS));

        let client = ureq::AgentBuilder::new()
            .timeout(timeout)
            .max_idle_connections(10)
            .max_idle_connections_per_host(5)
            .build();

        Self { client, allowed_domains: mapping.allowed_domains.clone(), stats: Some(stats) }
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
        let mapping = FetchUrlMapping {
            input: "imageUrl".to_string(),
            output: "imageBase64".to_string(),
            allowed_domains: vec!["example.com".to_string(), "*.cdn.example.com".to_string()],
            max_size: None,
            timeout: None,
            retries: None,
            output_format: None,
        };
        let fetcher = UrlFetcher::new(&mapping);

        assert!(fetcher.is_domain_allowed("https://example.com/image.jpg").unwrap());
        assert!(fetcher.is_domain_allowed("https://images.cdn.example.com/image.jpg").unwrap());
        assert!(!fetcher.is_domain_allowed("https://other.com/image.jpg").unwrap());
    }

    #[test]
    fn test_wildcard_all_domains() {
        let mapping = FetchUrlMapping {
            input: "imageUrl".to_string(),
            output: "imageBase64".to_string(),
            allowed_domains: vec!["*".to_string()],
            max_size: None,
            timeout: None,
            retries: None,
            output_format: None,
        };
        let fetcher = UrlFetcher::new(&mapping);

        assert!(fetcher.is_domain_allowed("https://example.com/image.jpg").unwrap());
        assert!(fetcher.is_domain_allowed("https://any-domain.org/image.jpg").unwrap());
    }

    #[test]
    fn test_empty_allowed_domains() {
        let mapping = FetchUrlMapping {
            input: "imageUrl".to_string(),
            output: "imageBase64".to_string(),
            allowed_domains: vec![],
            max_size: None,
            timeout: None,
            retries: None,
            output_format: None,
        };
        let fetcher = UrlFetcher::new(&mapping);

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

    // Path parsing tests
    #[test]
    fn test_parse_path_simple() {
        let segments = parse_path("imageUrl");
        assert_eq!(segments, vec![PathSegment::Field("imageUrl".to_string())]);
    }

    #[test]
    fn test_parse_path_nested() {
        let segments = parse_path("media.image.url");
        assert_eq!(
            segments,
            vec![
                PathSegment::Field("media".to_string()),
                PathSegment::Field("image".to_string()),
                PathSegment::Field("url".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_path_array_index() {
        let segments = parse_path("images[0].url");
        assert_eq!(
            segments,
            vec![
                PathSegment::Field("images".to_string()),
                PathSegment::ArrayIndex(0),
                PathSegment::Field("url".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_path_array_wildcard() {
        let segments = parse_path("images[].url");
        assert_eq!(
            segments,
            vec![
                PathSegment::Field("images".to_string()),
                PathSegment::ArrayWildcard,
                PathSegment::Field("url".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_path_complex() {
        let segments = parse_path("data.items[].nested[0].value");
        assert_eq!(
            segments,
            vec![
                PathSegment::Field("data".to_string()),
                PathSegment::Field("items".to_string()),
                PathSegment::ArrayWildcard,
                PathSegment::Field("nested".to_string()),
                PathSegment::ArrayIndex(0),
                PathSegment::Field("value".to_string()),
            ]
        );
    }

    #[test]
    fn test_extract_urls_simple() {
        let doc = serde_json::json!({
            "imageUrl": "https://example.com/image.jpg"
        });
        let urls = extract_urls(&doc, "imageUrl");
        assert_eq!(urls, vec!["https://example.com/image.jpg"]);
    }

    #[test]
    fn test_extract_urls_nested() {
        let doc = serde_json::json!({
            "media": {
                "image": {
                    "url": "https://example.com/nested.jpg"
                }
            }
        });
        let urls = extract_urls(&doc, "media.image.url");
        assert_eq!(urls, vec!["https://example.com/nested.jpg"]);
    }

    #[test]
    fn test_extract_urls_array_index() {
        let doc = serde_json::json!({
            "images": [
                { "url": "https://example.com/img0.jpg" },
                { "url": "https://example.com/img1.jpg" }
            ]
        });
        let urls = extract_urls(&doc, "images[0].url");
        assert_eq!(urls, vec!["https://example.com/img0.jpg"]);

        let urls = extract_urls(&doc, "images[1].url");
        assert_eq!(urls, vec!["https://example.com/img1.jpg"]);
    }

    #[test]
    fn test_extract_urls_array_wildcard() {
        let doc = serde_json::json!({
            "images": [
                { "url": "https://example.com/img0.jpg" },
                { "url": "https://example.com/img1.jpg" },
                { "url": "https://example.com/img2.jpg" }
            ]
        });
        let urls = extract_urls(&doc, "images[].url");
        assert_eq!(
            urls,
            vec![
                "https://example.com/img0.jpg",
                "https://example.com/img1.jpg",
                "https://example.com/img2.jpg"
            ]
        );
    }

    #[test]
    fn test_extract_urls_real_world_example() {
        // This matches the user's actual use case with Images[].Filename
        let doc = serde_json::json!({
            "Id": 4501,
            "TitleEn": "Product Name",
            "Images": [
                {
                    "Filename": "https://cdn.example.com/product/image1.jpg",
                    "MediaType": 0,
                    "IsThumbnail": true
                },
                {
                    "Filename": "https://cdn.example.com/product/image2.jpg",
                    "MediaType": 0,
                    "IsThumbnail": false
                }
            ]
        });
        let urls = extract_urls(&doc, "Images[].Filename");
        assert_eq!(
            urls,
            vec![
                "https://cdn.example.com/product/image1.jpg",
                "https://cdn.example.com/product/image2.jpg"
            ]
        );
    }

    #[test]
    fn test_extract_urls_missing_field() {
        let doc = serde_json::json!({
            "name": "test"
        });
        let urls = extract_urls(&doc, "imageUrl");
        assert!(urls.is_empty());
    }

    #[test]
    fn test_extract_urls_empty_array() {
        let doc = serde_json::json!({
            "images": []
        });
        let urls = extract_urls(&doc, "images[].url");
        assert!(urls.is_empty());
    }

    #[test]
    fn test_path_has_array_wildcard() {
        assert!(!path_has_array_wildcard("imageUrl"));
        assert!(!path_has_array_wildcard("media.image.url"));
        assert!(!path_has_array_wildcard("images[0].url"));
        assert!(path_has_array_wildcard("images[].url"));
        assert!(path_has_array_wildcard("data.items[].nested.value"));
    }

    /// Integration test that fetches a real image from picsum.photos and verifies
    /// the base64 content is correctly extracted and can be added to a document.
    ///
    /// Run with: cargo test -p milli test_fetch_real_image_to_document -- --ignored
    #[test]
    #[ignore] // Requires network access
    fn test_fetch_real_image_to_document() {
        use crate::vector::settings::FetchOutputFormat;

        // Create a document with an image URL
        let mut document = serde_json::json!({
            "id": 1,
            "title": "Test Product",
            "imageUrl": "https://picsum.photos/200"
        });

        // Extract the URL from the document
        let urls = extract_urls(&document, "imageUrl");
        assert_eq!(urls.len(), 1);
        let url = &urls[0];

        // Create a fetch mapping with picsum.photos allowed
        let fetch_mapping = FetchUrlMapping {
            input: "imageUrl".to_string(),
            output: "imageBase64".to_string(),
            allowed_domains: vec!["picsum.photos".to_string(), "*.picsum.photos".to_string()],
            timeout: Some(30_000),
            max_size: Some("10MB".to_string()),
            retries: Some(2),
            output_format: Some(FetchOutputFormat::DataUri),
        };
        let fetcher = UrlFetcher::new(&fetch_mapping);

        // Create a resolved mapping
        let mapping = ResolvedFetchMapping::from_mapping(&fetch_mapping);

        // Fetch the image
        let result = fetcher.fetch_as_base64(url, &mapping);
        assert!(result.is_ok(), "Failed to fetch image: {:?}", result.err());

        let base64_content = result.unwrap();

        // Verify it's a valid data URI for an image
        assert!(
            base64_content.starts_with("data:image/"),
            "Expected data URI starting with 'data:image/', got: {}...",
            &base64_content[..50.min(base64_content.len())]
        );
        assert!(base64_content.contains(";base64,"), "Expected base64 encoding marker");

        // Extract just the base64 part and verify it's valid
        let base64_part = base64_content.split(";base64,").nth(1).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD.decode(base64_part);
        assert!(decoded.is_ok(), "Invalid base64 content");

        let image_bytes = decoded.unwrap();
        assert!(!image_bytes.is_empty(), "Image content should not be empty");

        // Verify it looks like an image (JPEG or PNG magic bytes)
        let is_jpeg = image_bytes.starts_with(&[0xFF, 0xD8, 0xFF]);
        let is_png = image_bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47]);
        assert!(is_jpeg || is_png, "Content should be JPEG or PNG image");

        // Add the base64 content to the document as a virtual field
        document["imageBase64"] = serde_json::Value::String(base64_content.clone());

        // Verify the document now has the base64 content
        assert!(document.get("imageBase64").is_some());
        assert_eq!(document["imageBase64"].as_str().unwrap(), &base64_content);

        println!("Successfully fetched image from picsum.photos");
        println!("Image size: {} bytes", image_bytes.len());
        println!("Base64 length: {} chars", base64_content.len());
        println!(
            "Document now contains: {:?}",
            document.as_object().unwrap().keys().collect::<Vec<_>>()
        );
    }
}
