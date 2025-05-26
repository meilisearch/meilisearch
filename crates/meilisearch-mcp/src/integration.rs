use crate::registry::McpToolRegistry;
use crate::server::{McpServer, MeilisearchClient};
use crate::Error;
use actix_web::{web, HttpResponse};
use meilisearch::routes::MeilisearchApi;
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use serde_json::Value;
use std::sync::Arc;
use utoipa::OpenApi;

pub struct MeilisearchMcpClient {
    base_url: String,
    client: reqwest::Client,
}

impl MeilisearchMcpClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl MeilisearchClient for MeilisearchMcpClient {
    async fn call_endpoint(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        auth_header: Option<String>,
    ) -> Result<Value, Error> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "DELETE" => self.client.delete(&url),
            "PATCH" => self.client.patch(&url),
            _ => return Err(Error::Protocol(format!("Unsupported method: {}", method))),
        };

        if let Some(auth) = auth_header {
            request = request.header("Authorization", auth);
        }

        if let Some(body) = body {
            request = request.json(&body);
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::Internal(e.into()))?;

        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| Error::Internal(e.into()))
        } else {
            let status = response.status();
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error response".to_string());
            
            Err(Error::Meilisearch(format!(
                "Request failed with status {}: {}",
                status, error_body
            )))
        }
    }
}

pub fn create_mcp_server_from_openapi() -> McpServer {
    // Get the OpenAPI specification from Meilisearch
    let openapi = MeilisearchApi::openapi();
    
    // Create registry from OpenAPI
    let registry = McpToolRegistry::from_openapi(&openapi);
    
    // Create MCP server
    McpServer::new(registry)
}

pub fn configure_mcp_route(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/mcp")
            .route(web::get().to(crate::server::mcp_sse_handler))
            .route(web::post().to(mcp_post_handler))
    );
}

async fn mcp_post_handler(
    req_body: web::Json<crate::protocol::McpRequest>,
    server: web::Data<McpServer>,
) -> Result<HttpResponse, actix_web::Error> {
    let response = server.handle_request(req_body.into_inner()).await;
    Ok(HttpResponse::Ok().json(response))
}

pub fn inject_mcp_server(app_data: &mut web::Data<()>) -> web::Data<McpServer> {
    let server = create_mcp_server_from_openapi();
    web::Data::new(server)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mcp_server() {
        let server = create_mcp_server_from_openapi();
        // Server should be created successfully
        assert!(true);
    }
}