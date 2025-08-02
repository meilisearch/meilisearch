// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn add_api_key_with_ip_restrictions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": "restricted-key",
        "description": "API key with IP restrictions",
        "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": ["search"],
        "expiresAt": "2050-11-13T00:00:00Z",
        "allowed_ips": ["192.168.1.0/24", "10.0.0.1/32"]
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(code, 201);
    assert_eq!(response["allowed_ips"], json!(["192.168.1.0/24", "10.0.0.1/32"]));
}

#[actix_rt::test]
async fn add_api_key_with_referrer_restrictions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": "restricted-key",
        "description": "API key with referrer restrictions",
        "uid": "5bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": ["search"],
        "expiresAt": "2050-11-13T00:00:00Z",
        "allowed_referrers": ["*.example.com", "https://trusted.org/*"]
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(code, 201);
    assert_eq!(response["allowed_referrers"], json!(["*.example.com", "https://trusted.org/*"]));
}

#[actix_rt::test]
async fn add_api_key_with_both_restrictions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": "restricted-key",
        "description": "API key with IP and referrer restrictions",
        "uid": "6bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": ["search"],
        "expiresAt": "2050-11-13T00:00:00Z",
        "allowed_ips": ["192.168.1.0/24"],
        "allowed_referrers": ["*.example.com"]
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(code, 201);
    assert_eq!(response["allowed_ips"], json!(["192.168.1.0/24"]));
    assert_eq!(response["allowed_referrers"], json!(["*.example.com"]));
}

// TODO: Add tests for actual authentication with IP/referrer restrictions once the Server test infrastructure supports it
