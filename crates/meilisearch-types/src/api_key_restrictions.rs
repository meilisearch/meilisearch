// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::net::IpAddr;

/// Check if the request is allowed by the given restrictions.
pub fn is_request_allowed(
    allowed_ips: Option<&Vec<String>>,
    allowed_referrers: Option<&Vec<String>>,
    ip: Option<IpAddr>,
    referrer: Option<&str>,
) -> bool {
    if let Some(list) = allowed_ips {
        let ip = match ip {
            Some(i) => i,
            None => return false,
        };
        if !list.iter().any(|net| ip_in(net, ip)) {
            return false;
        }
    }

    if let Some(list) = allowed_referrers {
        let referer = match referrer {
            Some(r) => r,
            None => return false,
        };
        if !list.iter().any(|p| wildcard_match(p, referer)) {
            return false;
        }
    }

    true
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let mut rest = value;
    let mut first = true;
    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }
        if let Some(pos) = rest.find(part) {
            if first && !pattern.starts_with('*') && pos != 0 {
                return false;
            }
            rest = &rest[pos + part.len()..];
            first = false;
        } else {
            return false;
        }
    }
    if !pattern.ends_with('*') && !rest.is_empty() {
        return false;
    }
    true
}

fn ip_in(pattern: &str, ip: IpAddr) -> bool {
    pattern.parse::<ipnet::IpNet>().map(|net| net.contains(&ip)).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_wildcard_match() {
        // Exact match
        assert!(wildcard_match("example.com", "example.com"));
        assert!(!wildcard_match("example.com", "other.com"));

        // Wildcard at the beginning
        assert!(wildcard_match("*.example.com", "www.example.com"));
        assert!(wildcard_match("*.example.com", "api.example.com"));
        assert!(!wildcard_match("*.example.com", "example.com"));
        assert!(!wildcard_match("*.example.com", "example.org"));

        // Wildcard at the end
        assert!(wildcard_match("example.*", "example.com"));
        assert!(wildcard_match("example.*", "example.org"));
        assert!(!wildcard_match("example.*", "other.com"));

        // Multiple wildcards
        assert!(wildcard_match("*.example.*", "www.example.com"));
        assert!(wildcard_match("*.example.*", "api.example.org"));
        assert!(!wildcard_match("*.example.*", "example.com"));

        // Just wildcard
        assert!(wildcard_match("*", "anything.goes"));
        assert!(wildcard_match("*", ""));

        // Complex patterns
        assert!(wildcard_match("https://*.example.com/*", "https://api.example.com/v1"));
        assert!(!wildcard_match("https://*.example.com/*", "http://api.example.com/v1"));
    }

    #[test]
    fn test_ip_in() {
        let ip4 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
        let ip6 = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));

        // IPv4 tests
        assert!(ip_in("192.168.1.0/24", ip4));
        assert!(!ip_in("192.168.2.0/24", ip4));
        assert!(ip_in("192.168.1.100/32", ip4));
        assert!(!ip_in("192.168.1.101/32", ip4));
        assert!(ip_in("0.0.0.0/0", ip4)); // Any IPv4

        // IPv6 tests
        assert!(ip_in("2001:db8::/32", ip6));
        assert!(!ip_in("2001:db9::/32", ip6));
        assert!(ip_in("2001:db8::1/128", ip6));
        assert!(!ip_in("2001:db8::2/128", ip6));

        // Invalid patterns
        assert!(!ip_in("invalid", ip4));
        assert!(!ip_in("", ip4));
    }

    #[test]
    fn test_is_request_allowed_ips() {
        let ip1 = Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
        let ip2 = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));

        // No restrictions
        assert!(is_request_allowed(None, None, ip1, None));
        assert!(is_request_allowed(None, None, None, None));

        // IP restrictions only
        let allowed_ips = vec!["192.168.1.0/24".to_string()];
        assert!(is_request_allowed(Some(&allowed_ips), None, ip1, None));
        assert!(!is_request_allowed(Some(&allowed_ips), None, ip2, None));
        assert!(!is_request_allowed(Some(&allowed_ips), None, None, None));

        // Multiple IP ranges
        let allowed_ips = vec!["192.168.1.0/24".to_string(), "10.0.0.0/24".to_string()];
        assert!(is_request_allowed(Some(&allowed_ips), None, ip1, None));
        assert!(is_request_allowed(Some(&allowed_ips), None, ip2, None));
    }

    #[test]
    fn test_is_request_allowed_referrers() {
        let ref1 = Some("https://www.example.com/page");
        let ref2 = Some("https://api.example.com/v1");
        let ref3 = Some("https://other.org");

        // No restrictions
        assert!(is_request_allowed(None, None, None, ref1));
        assert!(is_request_allowed(None, None, None, None));

        // Referrer restrictions only
        let allowed_refs = vec!["*.example.com*".to_string()];
        assert!(is_request_allowed(None, Some(&allowed_refs), None, ref1));
        assert!(is_request_allowed(None, Some(&allowed_refs), None, ref2));
        assert!(!is_request_allowed(None, Some(&allowed_refs), None, ref3));
        assert!(!is_request_allowed(None, Some(&allowed_refs), None, None));
    }

    #[test]
    fn test_is_request_allowed_combined() {
        let ip = Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)));
        let ref1 = Some("https://www.example.com");

        // Both restrictions must pass
        let allowed_ips = vec!["192.168.1.0/24".to_string()];
        let allowed_refs = vec!["*.example.com".to_string()];

        // Both pass
        assert!(is_request_allowed(Some(&allowed_ips), Some(&allowed_refs), ip, ref1));

        // IP fails
        let ip2 = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert!(!is_request_allowed(Some(&allowed_ips), Some(&allowed_refs), ip2, ref1));

        // Referrer fails
        let ref2 = Some("https://other.org");
        assert!(!is_request_allowed(Some(&allowed_ips), Some(&allowed_refs), ip, ref2));

        // Both fail
        assert!(!is_request_allowed(Some(&allowed_ips), Some(&allowed_refs), ip2, ref2));
    }
}
