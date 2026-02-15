use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use serde_json::Value;
use utoipa::OpenApi;

/// HTTP methods supported in OpenAPI specifications.
const HTTP_METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

#[derive(Parser)]
#[command(name = "openapi-generator")]
#[command(about = "Generate OpenAPI specification for Meilisearch")]
struct Cli {
    /// Output file path (default: meilisearch-openapi.json)
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Pretty print the JSON output
    #[arg(short, long)]
    pretty: bool,

    /// Check that all routes have a summary (useful for CI)
    #[arg(long)]
    check_summaries: bool,

    /// Check for duplicate routes and path issues (useful for CI)
    #[arg(long)]
    check_paths: bool,

    /// Check that parameters have descriptions, 2xx responses have examples, and schema properties have descriptions (useful for CI)
    #[arg(long)]
    check_docs: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate the OpenAPI specification
    let openapi = MeilisearchApi::openapi();

    // Convert to serde_json::Value for modification
    let openapi_value: Value = serde_json::to_value(&openapi)?;

    // Check that all routes have summaries if requested
    if cli.check_summaries {
        check_all_routes_have_summaries(&openapi_value)?;
    }

    // Check for path issues (duplicates, malformed paths) if requested
    if cli.check_paths {
        check_path_issues(&openapi_value)?;
    }

    // Check documentation (param descriptions, response examples, schema properties) if requested
    if cli.check_docs {
        check_docs(&openapi_value)?;
    }

    // Determine output path
    let output_path = cli.output.unwrap_or_else(|| PathBuf::from("meilisearch-openapi.json"));

    // Serialize to JSON
    let json = if cli.pretty {
        serde_json::to_string_pretty(&openapi_value)?
    } else {
        serde_json::to_string(&openapi_value)?
    };

    // Write to file
    std::fs::write(&output_path, json)?;

    println!("OpenAPI specification written to: {}", output_path.display());

    Ok(())
}

/// Checks that all routes have a summary field.
///
/// Returns an error if any route is missing a summary.
fn check_all_routes_have_summaries(openapi: &Value) -> Result<()> {
    let paths = openapi
        .get("paths")
        .and_then(|p| p.as_object())
        .context("OpenAPI spec missing 'paths' object")?;

    let mut missing_summaries: Vec<String> = Vec::new();

    for (path, path_item) in paths.iter() {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };

        for method in HTTP_METHODS {
            let Some(operation) = path_item.get(*method) else {
                continue;
            };

            let has_summary =
                operation.get("summary").and_then(|s| s.as_str()).is_some_and(|s| !s.is_empty());

            if !has_summary {
                missing_summaries.push(format!("{} {}", method.to_uppercase(), path));
            }
        }
    }

    if missing_summaries.is_empty() {
        println!("All routes have summaries.");
        Ok(())
    } else {
        missing_summaries.sort();
        eprintln!("The following routes are missing a summary:");
        for route in &missing_summaries {
            eprintln!("  - {}", route);
        }
        eprintln!("\nTo fix this, add a doc-comment (///) above the route handler function.");
        eprintln!("The first line becomes the summary, subsequent lines become the description.");
        eprintln!("\nExample:");
        eprintln!("  /// List webhooks");
        eprintln!("  ///");
        eprintln!("  /// Get the list of all registered webhooks.");
        eprintln!("  #[utoipa::path(...)]");
        eprintln!("  async fn get_webhooks(...) {{ ... }}");
        anyhow::bail!("{} route(s) missing summary", missing_summaries.len());
    }
}

/// Checks for path issues in the OpenAPI specification.
///
/// This function validates that:
/// 1. All paths start with `/`
/// 2. No paths contain double slashes `//`
/// 3. No duplicate paths exist (after normalizing slashes)
///
/// Returns an error if any issues are found.
fn check_path_issues(openapi: &Value) -> Result<()> {
    let paths = openapi
        .get("paths")
        .and_then(|p| p.as_object())
        .context("OpenAPI spec missing 'paths' object")?;

    let mut issues: Vec<String> = Vec::new();
    let mut normalized_paths: HashMap<String, String> = HashMap::new();

    for path in paths.keys() {
        // Check 1: Path must start with /
        if !path.starts_with('/') {
            issues.push(format!("Path does not start with '/': {}", path));
        }

        // Check 2: Path must not contain //
        if path.contains("//") {
            issues.push(format!("Path contains double slashes '//': {}", path));
        }

        // Check 3: Check for duplicates after normalization
        // Normalize by: removing leading/trailing slashes, collapsing multiple slashes
        let normalized = normalize_path(path);
        if let Some(existing) = normalized_paths.get(&normalized) {
            if existing != path {
                issues.push(format!(
                    "Duplicate routes detected (same path after normalization):\n    - {}\n    - {}",
                    existing, path
                ));
            }
        } else {
            normalized_paths.insert(normalized, path.clone());
        }
    }

    if issues.is_empty() {
        println!("All paths are valid (no duplicates or malformed paths).");
        Ok(())
    } else {
        eprintln!("Path issues found in OpenAPI specification:\n");
        for issue in &issues {
            eprintln!("  - {}", issue);
        }
        eprintln!();
        anyhow::bail!("{} path issue(s) found", issues.len());
    }
}

/// Resolves a `$ref` like `#/components/schemas/Foo` against the OpenAPI root.
fn resolve_ref<'a>(openapi: &'a Value, r#ref: &str) -> Option<&'a Value> {
    let r#ref = r#ref.strip_prefix("#/")?;
    let mut current = openapi;
    for part in r#ref.split('/') {
        current = current.get(part)?;
    }
    Some(current)
}

/// Returns the properties object of a schema, resolving `$ref` if needed.
fn get_schema_properties<'a>(
    openapi: &'a Value,
    schema: &'a Value,
) -> Option<&'a serde_json::Map<String, Value>> {
    let schema = if let Some(r#ref) = schema.get("$ref").and_then(|r| r.as_str()) {
        resolve_ref(openapi, r#ref)?
    } else {
        schema
    };
    schema.get("properties").and_then(|p| p.as_object())
}

/// Returns true if the response object has an example (content.application/json.example or .examples).
fn response_has_example(response: &Value) -> bool {
    let content = match response.get("content").and_then(|c| c.get("application/json")) {
        Some(c) => c,
        None => return false,
    };
    if content.get("example").is_some() {
        return true;
    }
    if let Some(examples) = content.get("examples").and_then(|e| e.as_object()) {
        if !examples.is_empty() {
            return true;
        }
    }
    false
}

/// Returns true if the response has a JSON body (content.application/json present).
fn response_has_body(response: &Value) -> bool {
    response.get("content").and_then(|c| c.get("application/json")).is_some()
}

/// Returns true if the path has at least one parameter whose name contains "uid" (case insensitive).
/// E.g. `{indexUid}`, `{taskUid}`, `{batchUid}`, `{uuid}`, `{uidOrKey}`.
fn path_has_uid_parameter(path: &str) -> bool {
    path.split('/')
        .filter_map(|segment| segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
        .any(|name| name.to_lowercase().contains("uid"))
}

/// Returns true if the schema value has a non-empty description, either directly or inside a oneOf/anyOf branch
/// (utoipa puts descriptions there for Option-like types like Setting<T>).
fn property_has_description(prop_obj: &serde_json::Map<String, Value>) -> bool {
    if let Some(desc) = prop_obj.get("description").and_then(|d| d.as_str()) {
        if !desc.trim().is_empty() {
            return true;
        }
    }
    // oneOf/anyOf: description can be on a branch (e.g. { "$ref": "...", "description": "..." })
    for key in ["oneOf", "anyOf"] {
        if let Some(arr) = prop_obj.get(key).and_then(|a| a.as_array()) {
            for branch in arr {
                if let Some(obj) = branch.as_object() {
                    if let Some(desc) = obj.get("description").and_then(|d| d.as_str()) {
                        if !desc.trim().is_empty() {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

/// Checks that all properties of a schema have a non-empty description.
fn check_schema_properties_have_description(
    openapi: &Value,
    schema: &Value,
    context: &str,
    errors: &mut Vec<String>,
) {
    let Some(properties) = get_schema_properties(openapi, schema) else {
        return;
    };
    for (prop_name, prop_value) in properties {
        let Some(prop_obj) = prop_value.as_object() else {
            continue;
        };
        if !property_has_description(prop_obj) {
            errors
                .push(format!("{}: property \"{}\" is missing a description", context, prop_name));
        }
        // One level of $ref for nested objects
        if let Some(nested_ref) = prop_obj.get("$ref").and_then(|r| r.as_str()) {
            if let Some(resolved) = resolve_ref(openapi, nested_ref) {
                if let Some(nested_props) = resolved.get("properties").and_then(|p| p.as_object()) {
                    for (nested_name, nested_value) in nested_props {
                        let Some(nested_obj) = nested_value.as_object() else {
                            continue;
                        };
                        if !property_has_description(nested_obj) {
                            errors.push(format!(
                                "{}: property \"{}.{}\" is missing a description",
                                context, prop_name, nested_name
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Checks documentation: parameters have descriptions, 2xx responses have examples, and schema properties have descriptions.
fn check_docs(openapi: &Value) -> Result<()> {
    let paths = openapi
        .get("paths")
        .and_then(|p| p.as_object())
        .context("OpenAPI spec missing 'paths' object")?;

    let mut errors: Vec<String> = Vec::new();

    // For each path and method: params documented, response example (and optionally schema properties)
    for (path, path_item) in paths.iter() {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };
        for method in HTTP_METHODS {
            let Some(operation) = path_item.get(*method) else {
                continue;
            };
            check_operation_docs(openapi, path, method, operation, &mut errors);
        }
    }

    if errors.is_empty() {
        println!("OpenAPI documentation check passed.");
        println!("  - Parameters have descriptions");
        println!("  - Request/response schema properties have descriptions");
        println!("  - 2xx responses have examples where applicable");
        println!("  - 401 (except GET /health), 404 (routes with *Uid param), and 400 responses have examples");
        Ok(())
    } else {
        errors.sort();
        eprintln!("OpenAPI documentation check failed:\n");
        for e in &errors {
            eprintln!("  - {}", e);
        }
        eprintln!("\nFix the above and re-run the check.");
        anyhow::bail!("{} documentation issue(s) found", errors.len());
    }
}

fn check_operation_docs(
    openapi: &Value,
    path: &str,
    method: &str,
    operation: &Value,
    errors: &mut Vec<String>,
) {
    let op_id_fallback = format!("{} {}", method, path);
    let op_id = operation.get("operationId").and_then(|o| o.as_str()).unwrap_or(&op_id_fallback);
    let prefix = format!("{} {} ({})", method.to_uppercase(), path, op_id);

    // DELETE routes must not have a request body
    if method == "delete" && operation.get("requestBody").is_some() {
        errors.push(format!("{}: DELETE route must not have a request body", prefix));
    }

    // Parameters (path, query, header) must have description
    let params =
        operation.get("parameters").and_then(|p| p.as_array()).map(|a| a.as_slice()).unwrap_or(&[]);
    for param in params {
        let name = param.get("name").and_then(|n| n.as_str()).unwrap_or("(unnamed)");
        let param_in = param.get("in").and_then(|i| i.as_str()).unwrap_or("unknown");
        let desc = param.get("description").and_then(|d| d.as_str());
        if desc.is_none_or(|s| s.trim().is_empty()) {
            errors.push(format!(
                "{}: parameter \"{}\" ({}) is missing a description",
                prefix, name, param_in
            ));
        }
    }

    // Request body schema properties must have description
    if let Some(req_body) = operation.get("requestBody") {
        if let Some(content) = req_body.get("content") {
            if let Some(app_json) = content.get("application/json") {
                if let Some(schema) = app_json.get("schema") {
                    check_schema_properties_have_description(
                        openapi,
                        schema,
                        &format!("{} request body", prefix),
                        errors,
                    );
                }
            }
        }
    }

    // At least one 2xx response must have an example when the response has a body
    let responses = operation.get("responses").and_then(|r| r.as_object());
    let success_codes: Vec<String> = responses
        .map(|r| r.keys().filter(|k| k.starts_with('2')).cloned().collect())
        .unwrap_or_default();
    let mut has_response_example = false;
    if let Some(resps) = responses {
        for code in &success_codes {
            let response = match resps.get(code) {
                Some(r) => r,
                None => continue,
            };
            let content = match response.get("content").and_then(|c| c.get("application/json")) {
                Some(c) => c,
                None => continue,
            };
            if content.get("example").is_some() {
                has_response_example = true;
                break;
            }
            if let Some(examples) = content.get("examples").and_then(|e| e.as_object()) {
                if !examples.is_empty() {
                    has_response_example = true;
                    break;
                }
            }
        }
    }
    let has_body = responses.is_some_and(|r| {
        success_codes.iter().any(|code| {
            r.get(code)
                .and_then(|res| res.get("content").and_then(|c| c.get("application/json")))
                .is_some()
        })
    });
    if has_body && !has_response_example {
        errors.push(format!(
            "{}: at least one 2xx response must have an example (response example required)",
            prefix
        ));
    }

    // 401 response must exist and have an example (missing authorization)
    // Exception: /health does not require authentication
    if path != "/health" {
        if let Some(resps) = responses {
            match resps.get("401") {
                Some(r401) => {
                    if !response_has_example(r401) {
                        errors.push(format!(
                            "{}: response 401 must have an example (e.g. missing_authorization_header)",
                            prefix
                        ));
                    }
                }
                None => {
                    errors.push(format!("{}: response 401 is required with an example", prefix));
                }
            }
        }
    }

    // 404 response required for routes with a *Uid (or Uid) path parameter (resource not found)
    if path_has_uid_parameter(path) {
        if let Some(resps) = responses {
            if let Some(r404) = resps.get("404") {
                if !response_has_example(r404) {
                    errors.push(format!(
                        "{}: response 404 must have an example (e.g. resource not found by uid)",
                        prefix
                    ));
                }
            } else {
                errors.push(format!(
                    "{}: response 404 is required for routes with a uid path parameter (e.g. resource not found)",
                    prefix
                ));
            }
        }
    }

    // 400 response must have an example when present (bad request / invalid payload)
    if let Some(resps) = responses {
        if let Some(r400) = resps.get("400") {
            if response_has_body(r400) && !response_has_example(r400) {
                errors.push(format!(
                    "{}: response 400 must have an example (e.g. error message and code)",
                    prefix
                ));
            }
        }
    }

    // Response body schema properties must have description
    if let Some(resps) = responses {
        for code in &success_codes {
            let response = match resps.get(code) {
                Some(r) => r,
                None => continue,
            };
            let content = match response.get("content").and_then(|c| c.get("application/json")) {
                Some(c) => c,
                None => continue,
            };
            if let Some(schema) = content.get("schema") {
                check_schema_properties_have_description(
                    openapi,
                    schema,
                    &format!("{} response {}", prefix, code),
                    errors,
                );
            }
        }
    }
}

/// Normalizes a path for duplicate detection.
///
/// - Removes leading and trailing slashes
/// - Collapses multiple consecutive slashes into one
fn normalize_path(path: &str) -> String {
    path.split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>().join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/indexes"), "indexes");
        assert_eq!(normalize_path("/indexes/"), "indexes");
        assert_eq!(normalize_path("indexes"), "indexes");
        assert_eq!(normalize_path("/indexes/{indexUid}"), "indexes/{indexUid}");
        assert_eq!(normalize_path("indexes//{indexUid}"), "indexes/{indexUid}");
        assert_eq!(normalize_path("/indexes//{indexUid}/compact"), "indexes/{indexUid}/compact");
        assert_eq!(normalize_path("//indexes///compact//"), "indexes/compact");
    }

    #[test]
    fn test_check_path_issues_valid() {
        let openapi = json!({
            "paths": {
                "/indexes": {},
                "/indexes/{indexUid}": {},
                "/indexes/{indexUid}/documents": {},
                "/indexes/{indexUid}/compact": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_path_issues_missing_leading_slash() {
        let openapi = json!({
            "paths": {
                "/indexes": {},
                "indexes/{indexUid}": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("path issue"));
    }

    #[test]
    fn test_check_path_issues_double_slash() {
        let openapi = json!({
            "paths": {
                "/indexes": {},
                "/indexes//{indexUid}/compact": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("path issue"));
    }

    #[test]
    fn test_check_path_issues_duplicate_routes() {
        let openapi = json!({
            "paths": {
                "/indexes/{indexUid}/compact": {},
                "indexes//{indexUid}/compact": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Should report at least the duplicate issue (and possibly the missing slash and double slash)
        assert!(err.contains("path issue"));
    }
}
