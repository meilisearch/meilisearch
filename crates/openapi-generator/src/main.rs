use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use serde_json::{Map, Value};
use utoipa::OpenApi;

/// HTTP methods supported in OpenAPI specifications.
const HTTP_METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

/// Alias for OpenAPI path item or operation object (map of string keys to JSON values).
type JsonObject = Map<String, Value>;

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

    /// Check that query and body parameters have explicit required = true/false in code (no utoipa inference)
    #[arg(long)]
    check_params: bool,
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

    // Check that query and body parameters have explicit required = true/false in code
    if cli.check_params {
        check_params()?;
    }

    // Determine output path
    let output_path = cli.output.unwrap_or_else(|| PathBuf::from("meilisearch-openapi.json"));

    // Serialize to JSON
    let json = if cli.pretty {
        serde_json::to_string_pretty(&openapi_value)?
    } else {
        serde_json::to_string(&openapi_value)?
    };

    std::fs::write(&output_path, &json)
        .with_context(|| format!("write OpenAPI spec to {}", output_path.display()))?;

    println!("OpenAPI specification written to: {}", output_path.display());

    Ok(())
}

/// Returns the top-level `paths` object from the OpenAPI spec.
fn get_paths_object(openapi: &Value) -> Result<&JsonObject> {
    openapi.get("paths").and_then(Value::as_object).context("OpenAPI spec missing 'paths' object")
}

/// Checks that all routes have a summary field.
///
/// Returns an error if any route is missing a summary.
fn check_all_routes_have_summaries(openapi: &Value) -> Result<()> {
    let paths = get_paths_object(openapi)?;

    let mut missing_summaries: Vec<String> = paths
        .iter()
        .flat_map(|(path, path_item)| {
            path_item.as_object().map(|path_item| {
                HTTP_METHODS.iter().filter_map(move |method| {
                    let op = path_item.get(*method)?;
                    let has_summary =
                        op.get("summary").and_then(Value::as_str).is_some_and(|s| !s.is_empty());
                    if has_summary {
                        None
                    } else {
                        Some(format!("{} {}", method.to_uppercase(), path))
                    }
                })
            })
        })
        .flatten()
        .collect();
    missing_summaries.sort_unstable();
    missing_summaries.dedup();

    if missing_summaries.is_empty() {
        println!("All routes have summaries.");
        return Ok(());
    }

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

/// Checks for path issues in the OpenAPI specification.
///
/// Validates that:
/// 1. All paths start with `/`
/// 2. No paths contain double slashes `//`
/// 3. No duplicate paths exist (after normalizing slashes)
fn check_path_issues(openapi: &Value) -> Result<()> {
    let paths = get_paths_object(openapi)?;

    let mut issues: Vec<String> = Vec::new();
    let mut normalized_paths: HashMap<String, String> = HashMap::new();

    for path in paths.keys() {
        if !path.starts_with('/') {
            issues.push(format!("Path does not start with '/': {}", path));
        }
        if path.contains("//") {
            issues.push(format!("Path contains double slashes '//': {}", path));
        }

        let normalized = normalize_path(path);
        match normalized_paths.entry(normalized) {
            Entry::Occupied(entry) if entry.get() != path => {
                issues.push(format!(
                    "Duplicate routes detected (same path after normalization):\n    - {}\n    - {}",
                    entry.get(),
                    path
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(path.clone());
            }
            _ => {}
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
fn get_schema_properties<'a>(openapi: &'a Value, schema: &'a Value) -> Option<&'a JsonObject> {
    let schema = schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|r#ref| resolve_ref(openapi, r#ref))
        .unwrap_or(schema);
    schema.get("properties").and_then(Value::as_object)
}

/// Returns true if the response object has an example (content.application/json.example or .examples).
fn response_has_example(response: &Value) -> bool {
    response
        .get("content")
        .and_then(|c| c.get("application/json"))
        .map(|content| {
            content.get("example").is_some()
                || content
                    .get("examples")
                    .and_then(Value::as_object)
                    .is_some_and(|ex| !ex.is_empty())
        })
        .unwrap_or(false)
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
fn property_has_description(prop_obj: &JsonObject) -> bool {
    prop_obj.get("description").and_then(Value::as_str).is_some_and(|d| !d.trim().is_empty())
        || ["oneOf", "anyOf"].iter().any(|&key| {
            prop_obj.get(key).and_then(Value::as_array).is_some_and(|arr| {
                arr.iter().filter_map(Value::as_object).any(|obj| {
                    obj.get("description")
                        .and_then(Value::as_str)
                        .is_some_and(|d| !d.trim().is_empty())
                })
            })
        })
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
                if let Some(nested_props) = resolved.get("properties").and_then(Value::as_object) {
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
    let paths = get_paths_object(openapi)?;
    let mut errors: Vec<String> = Vec::new();

    for (path, path_item) in paths.iter() {
        let Some(path_item) = path_item.as_object() else { continue };
        for method in HTTP_METHODS {
            let Some(operation) = path_item.get(*method) else { continue };
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

fn operation_prefix(operation: &Value, method: &str, path: &str) -> String {
    let op_id = operation
        .get("operationId")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("{} {}", method, path));
    format!("{} {} ({})", method.to_uppercase(), path, op_id)
}

fn check_delete_no_body(method: &str, operation: &Value, prefix: &str, errors: &mut Vec<String>) {
    if method == "delete" && operation.get("requestBody").is_some() {
        errors.push(format!("{}: DELETE route must not have a request body", prefix));
    }
}

fn check_parameters_descriptions(operation: &Value, prefix: &str, errors: &mut Vec<String>) {
    let params =
        operation.get("parameters").and_then(Value::as_array).map(|a| a.as_slice()).unwrap_or(&[]);
    for param in params {
        let name = param.get("name").and_then(Value::as_str).unwrap_or("(unnamed)");
        let param_in = param.get("in").and_then(Value::as_str).unwrap_or("unknown");
        if param.get("description").and_then(Value::as_str).is_none_or(|s| s.trim().is_empty()) {
            errors.push(format!(
                "{}: parameter \"{}\" ({}) is missing a description",
                prefix, name, param_in
            ));
        }
    }
}

fn check_request_body_schema(
    openapi: &Value,
    operation: &Value,
    prefix: &str,
    errors: &mut Vec<String>,
) {
    if let Some(schema) = operation
        .get("requestBody")
        .and_then(|b| b.get("content"))
        .and_then(|c| c.get("application/json"))
        .and_then(|j| j.get("schema"))
    {
        check_schema_properties_have_description(
            openapi,
            schema,
            &format!("{} request body", prefix),
            errors,
        );
    }
}

fn success_codes_from_responses(responses: Option<&JsonObject>) -> Vec<String> {
    responses
        .map(|r| r.keys().filter(|k| k.starts_with('2')).cloned().collect())
        .unwrap_or_default()
}

fn has_2xx_response_example(responses: Option<&JsonObject>, success_codes: &[String]) -> bool {
    let Some(resps) = responses else { return false };
    success_codes.iter().any(|code| {
        resps
            .get(code)
            .and_then(|r| r.get("content").and_then(|c| c.get("application/json")))
            .is_some_and(|content| {
                content.get("example").is_some()
                    || content
                        .get("examples")
                        .and_then(Value::as_object)
                        .is_some_and(|ex| !ex.is_empty())
            })
    })
}

fn success_responses_have_body(responses: Option<&JsonObject>, success_codes: &[String]) -> bool {
    responses.is_some_and(|r| {
        success_codes.iter().any(|code| {
            r.get(code)
                .and_then(|res| res.get("content").and_then(|c| c.get("application/json")))
                .is_some()
        })
    })
}

fn check_2xx_has_example(
    responses: Option<&JsonObject>,
    success_codes: &[String],
    prefix: &str,
    errors: &mut Vec<String>,
) {
    let has_body = success_responses_have_body(responses, success_codes);
    let has_example = has_2xx_response_example(responses, success_codes);
    if has_body && !has_example {
        errors.push(format!(
            "{}: at least one 2xx response must have an example (response example required)",
            prefix
        ));
    }
}

fn check_401_response(
    path: &str,
    responses: Option<&JsonObject>,
    prefix: &str,
    errors: &mut Vec<String>,
) {
    if path == "/health" {
        return;
    }
    let Some(resps) = responses else { return };
    match resps.get("401") {
        Some(r401) if !response_has_example(r401) => {
            errors.push(format!(
                "{}: response 401 must have an example (e.g. missing_authorization_header)",
                prefix
            ));
        }
        None => {
            errors.push(format!("{}: response 401 is required with an example", prefix));
        }
        _ => {}
    }
}

fn check_404_response(
    path: &str,
    responses: Option<&JsonObject>,
    prefix: &str,
    errors: &mut Vec<String>,
) {
    if !path_has_uid_parameter(path) {
        return;
    }
    let Some(resps) = responses else { return };
    match resps.get("404") {
        Some(r404) if !response_has_example(r404) => {
            errors.push(format!(
                "{}: response 404 must have an example (e.g. resource not found by uid)",
                prefix
            ));
        }
        None => {
            errors.push(format!(
                "{}: response 404 is required for routes with a uid path parameter (e.g. resource not found)",
                prefix
            ));
        }
        _ => {}
    }
}

fn check_400_response(responses: Option<&JsonObject>, prefix: &str, errors: &mut Vec<String>) {
    let Some(r400) = responses.and_then(|r| r.get("400")) else { return };
    if response_has_body(r400) && !response_has_example(r400) {
        errors.push(format!(
            "{}: response 400 must have an example (e.g. error message and code)",
            prefix
        ));
    }
}

fn check_response_schemas(
    openapi: &Value,
    responses: Option<&JsonObject>,
    success_codes: &[String],
    prefix: &str,
    errors: &mut Vec<String>,
) {
    let Some(resps) = responses else { return };
    for code in success_codes {
        let Some(schema) = resps
            .get(code)
            .and_then(|r| r.get("content"))
            .and_then(|c| c.get("application/json"))
            .and_then(|j| j.get("schema"))
        else {
            continue;
        };
        check_schema_properties_have_description(
            openapi,
            schema,
            &format!("{} response {}", prefix, code),
            errors,
        );
    }
}

fn check_operation_docs(
    openapi: &Value,
    path: &str,
    method: &str,
    operation: &Value,
    errors: &mut Vec<String>,
) {
    let prefix = operation_prefix(operation, method, path);
    let responses = operation.get("responses").and_then(Value::as_object);
    let success_codes = success_codes_from_responses(responses);

    check_delete_no_body(method, operation, &prefix, errors);
    check_parameters_descriptions(operation, &prefix, errors);
    check_request_body_schema(openapi, operation, &prefix, errors);
    check_2xx_has_example(responses, &success_codes, &prefix, errors);
    check_401_response(path, responses, &prefix, errors);
    check_404_response(path, responses, &prefix, errors);
    check_400_response(responses, &prefix, errors);
    check_response_schemas(openapi, responses, &success_codes, &prefix, errors);
}

/// Normalizes a path for duplicate detection.
///
/// - Removes leading and trailing slashes
/// - Collapses multiple consecutive slashes into one
fn normalize_path(path: &str) -> String {
    path.split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>().join("/")
}

/// Checks that query and body parameters in Rust source have explicit `required = true` or `required = false`.
///
/// Scans crates/meilisearch/src for:
/// - Query: structs with `#[into_params(..., parameter_in = Query, ...)]`: every `#[param(...)]` field must contain `required = true` or `required = false`.
/// - Body: structs used as `request_body` in path attributes: every field with `#[schema(...)]` must contain `required = true` or `required = false`.
fn check_params() -> Result<()> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR not set")?;
    let meilisearch_src = Path::new(&manifest_dir)
        .join("../meilisearch/src")
        .canonicalize()
        .context("resolve meilisearch/src path (run from workspace root)")?;

    let mut errors: Vec<String> = Vec::new();
    let mut request_body_types: HashSet<String> = HashSet::new();

    collect_request_body_types(&meilisearch_src, &mut request_body_types)?;

    for entry in walk_rs_files(&meilisearch_src)? {
        let path = entry.path();
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let rel = path.strip_prefix(&meilisearch_src).unwrap_or(&path);
        check_query_params_in_file(&content, rel, &mut errors);
        check_body_schema_in_file(&content, rel, &request_body_types, &mut errors);
    }

    if errors.is_empty() {
        println!("All query and body parameters have explicit required = true/false.");
        Ok(())
    } else {
        eprintln!("We do not want utoipa to infer whether a parameter is required or not, as that does not correctly cover our documentation needs. You must define it explicitly with required = true or required = false.\n");
        eprintln!(
            "The following parameters are missing explicit required = true or required = false:\n"
        );
        for e in &errors {
            eprintln!("  - {}", e);
        }
        eprintln!("\nFix the above by adding required = true or required = false in the #[param(...)] or #[schema(...)] attribute.");
        anyhow::bail!("{} parameter(s) missing explicit required", errors.len())
    }
}

fn walk_rs_files(dir: &Path) -> Result<Vec<std::fs::DirEntry>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_rs_files(&path)?);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(entry);
        }
    }
    Ok(out)
}

/// Extract the content of an attribute: from `#[attr(` to the matching `)`.
fn extract_attr_content(s: &str, open_pos: usize) -> Option<&str> {
    let rest = s.get(open_pos..)?;
    let start = rest.find('(')? + 1;
    let mut depth = 1u32;
    let mut i = start;
    let bytes = rest.as_bytes();
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return None;
    }
    rest.get(start..i - 1)
}

fn has_required_explicit(content: &str) -> bool {
    let content = content.trim();
    content.contains("required = true") || content.contains("required = false")
}

/// For a struct body, find every field (pub or private). For each, the "block above" is
/// the lines between the previous field and this one. Check that block contains required = true/false.
fn check_struct_fields_have_required(
    body: &str,
    struct_name: &str,
    kind: &str,
    rel_path: &Path,
    errors: &mut Vec<String>,
) {
    let lines: Vec<&str> = body.lines().collect();
    let mut block_above_lines: Vec<&str> = Vec::new();

    for line in &lines {
        let trimmed = line.trim();
        let field_name: Option<&str> = if let Some(after_pub) = trimmed.strip_prefix("pub ") {
            let after_pub = after_pub.trim_start();
            let end = after_pub
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(after_pub.len());
            let name = after_pub[..end].trim();
            if !name.is_empty()
                && after_pub.get(end..).is_none_or(|s| s.trim_start().starts_with(':'))
            {
                Some(name)
            } else {
                None
            }
        } else if let Some(colon_pos) = trimmed.find(':') {
            let before_colon = trimmed[..colon_pos].trim();
            if !before_colon.is_empty()
                && before_colon.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !before_colon.eq("pub")
            {
                Some(before_colon)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(name) = field_name {
            let block_above_str = block_above_lines.join("\n");
            let block_above = block_above_str.trim();
            if !has_required_explicit(block_above) {
                errors.push(format!(
                    "{}: {} struct `{}` has parameter `{}` without required = true/false in the attributes above it",
                    rel_path.display(),
                    kind,
                    struct_name,
                    name
                ));
            }
            block_above_lines.clear();
        } else {
            block_above_lines.push(line);
        }
    }
}

/// Collect type names used as request_body in path attributes (e.g. request_body = CreateApiKey).
fn collect_request_body_types(dir: &Path, out: &mut HashSet<String>) -> Result<()> {
    for entry in walk_rs_files(dir)? {
        let content = std::fs::read_to_string(entry.path())?;
        for line in content.lines() {
            let line = line.trim();
            if let Some(pos) = line.find("request_body") {
                let after = &line[pos + "request_body".len()..];
                let after = after.trim_start();
                let after = after.strip_prefix('=').map(|s| s.trim_start()).unwrap_or("");
                let name = after
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect::<String>();
                if !name.is_empty() && name != "serde_json" && name != "Vec" && name != "Value" {
                    out.insert(name);
                }
            }
        }
    }
    Ok(())
}

/// Check query param structs: #[into_params(..., parameter_in = Query, ...)] and then every #[param(...)] must have required.
fn check_query_params_in_file(content: &str, rel_path: &Path, errors: &mut Vec<String>) {
    let mut i = 0;
    while let Some(pos) = content[i..].find("#[into_params(") {
        let abs_pos = i + pos;
        let attr = match extract_attr_content(content, abs_pos + 2) {
            Some(a) => a,
            None => {
                i = abs_pos + 1;
                continue;
            }
        };
        if !attr.contains("parameter_in") || !attr.contains("Query") {
            i = abs_pos + 1;
            continue;
        }
        let into_params_prefix_len = "#[into_params(".len();
        let after_attr_offset = abs_pos + into_params_prefix_len + attr.len() + 2;
        let after_attr_slice = content.get(after_attr_offset..).unwrap_or("");
        let (struct_start, name_offset) = if let Some(p) = after_attr_slice.find("pub struct ") {
            (after_attr_offset + p, "pub struct ".len())
        } else if let Some(p) = after_attr_slice.find("struct ") {
            (after_attr_offset + p, "struct ".len())
        } else {
            i = abs_pos + 1;
            continue;
        };
        let name_start = struct_start + name_offset;
        let name_end = content[name_start..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|p| name_start + p)
            .unwrap_or(content.len());
        let struct_name = content[name_start..name_end].trim();
        let brace =
            content[struct_start..].find('{').map(|p| struct_start + p).unwrap_or(struct_start);
        let body = match extract_brace_content(content, brace) {
            Some(b) => b,
            None => {
                i = abs_pos + 1;
                continue;
            }
        };
        check_struct_fields_have_required(body, struct_name, "query", rel_path, errors);
        i = struct_start + 1;
    }
}

/// Extract content inside `{ ... }` starting at the opening brace.
fn extract_brace_content(s: &str, open_brace_pos: usize) -> Option<&str> {
    let rest = s.get(open_brace_pos..)?;
    if !rest.starts_with('{') {
        return None;
    }
    let mut depth = 1u32;
    let mut i = 1;
    let bytes = rest.as_bytes();
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => depth -= 1,
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return None;
    }
    rest.get(1..i - 1)
}

/// Check body structs: for structs in request_body_types, every #[schema(...)] field must have required.
fn check_body_schema_in_file(
    content: &str,
    rel_path: &Path,
    request_body_types: &HashSet<String>,
    errors: &mut Vec<String>,
) {
    let mut i = 0;
    while let Some(pos) = content[i..].find("pub struct ") {
        let struct_start = i + pos + "pub struct ".len();
        let name_end = content[struct_start..]
            .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '<')
            .map(|p| struct_start + p)
            .unwrap_or(content.len());
        let name = content[struct_start..name_end].trim();
        let base_name = name.split('<').next().unwrap_or(name).trim();
        if !request_body_types.contains(base_name) {
            i = struct_start + 1;
            continue;
        }
        let brace =
            content[struct_start..].find('{').map(|p| struct_start + p).unwrap_or(struct_start);
        let body = match extract_brace_content(content, brace) {
            Some(b) => b,
            None => {
                i = struct_start + 1;
                continue;
            }
        };
        check_struct_fields_have_required(body, base_name, "body", rel_path, errors);
        i = struct_start + 1;
    }
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

    #[test]
    fn test_check_struct_fields_missing_required() {
        let body = r#"
    /// Doc
    #[deserr(default)]
    pub q: Option<String>,
    #[param(value_type = usize)]
    pub limit: usize,
"#;
        let mut errors = Vec::new();
        check_struct_fields_have_required(
            body,
            "SearchQuery",
            "body",
            Path::new("search/mod.rs"),
            &mut errors,
        );
        assert!(
            !errors.is_empty(),
            "expected errors when no required = true/false in block above pub, got: {:?}",
            errors
        );
        assert!(errors[0].contains("parameter `q`"));
        assert!(errors[1].contains("parameter `limit`"));
    }

    #[test]
    fn test_check_struct_fields_with_required() {
        let body = r#"
    #[schema(required = false)]
    pub q: Option<String>,
    #[param(required = true, value_type = usize)]
    pub limit: usize,
"#;
        let mut errors = Vec::new();
        check_struct_fields_have_required(
            body,
            "SearchQuery",
            "body",
            Path::new("search/mod.rs"),
            &mut errors,
        );
        assert!(errors.is_empty(), "expected no errors when required is explicit: {:?}", errors);
    }
}
