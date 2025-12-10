use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use regex::Regex;
use serde_json::{json, Value};
use utoipa::OpenApi;

const HTTP_METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

/// Language used in the documentation repository (contains the key mapping)
const DOCS_LANG: &str = "cURL";

/// Mapping of repository URLs to language names.
/// The "cURL" entry is special: it contains the key mapping used to resolve sample IDs for all SDKs.
const CODE_SAMPLES: &[(&str, &str)] = &[
    ("https://raw.githubusercontent.com/meilisearch/documentation/refs/heads/main/.code-samples.meilisearch.yaml", "cURL"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-dotnet/refs/heads/main/.code-samples.meilisearch.yaml", "C#"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-dart/refs/heads/main/.code-samples.meilisearch.yaml", "Dart"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-go/refs/heads/main/.code-samples.meilisearch.yaml", "Go"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-java/refs/heads/main/.code-samples.meilisearch.yaml", "Java"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-js/refs/heads/main/.code-samples.meilisearch.yaml", "JS"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-php/refs/heads/main/.code-samples.meilisearch.yaml", "PHP"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-python/refs/heads/main/.code-samples.meilisearch.yaml", "Python"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-ruby/refs/heads/main/.code-samples.meilisearch.yaml", "Ruby"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-rust/refs/heads/main/.code-samples.meilisearch.yaml", "Rust"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-swift/refs/heads/main/.code-samples.meilisearch.yaml", "Swift"),
];

// Pre-compiled regex patterns
static COMMENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^#\s*([a-zA-Z0-9_]+)\s*$").unwrap());
static CODE_START_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([a-zA-Z0-9_]+):\s*\|-\s*$").unwrap());

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

    /// Skip fetching code samples (offline mode)
    #[arg(long)]
    no_code_samples: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate the OpenAPI specification
    let openapi = MeilisearchApi::openapi();

    // Convert to serde_json::Value for modification
    let mut openapi_value: Value = serde_json::to_value(&openapi)?;

    // Fetch and add code samples if not disabled
    if !cli.no_code_samples {
        let code_samples = fetch_all_code_samples()?;
        add_code_samples_to_openapi(&mut openapi_value, &code_samples)?;
    }

    // Clean up null descriptions in tags
    clean_null_descriptions(&mut openapi_value);

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

/// Code sample for a specific language
#[derive(Debug, Clone)]
struct CodeSample {
    lang: String,
    source: String,
}

/// Fetch and parse code samples from all repositories
/// Returns a map from key (e.g., "get_indexes") to a list of code samples for different languages
fn fetch_all_code_samples() -> Result<HashMap<String, Vec<CodeSample>>> {
    // First, fetch the documentation file (cURL) to get the key mapping
    let (docs_url, _) = CODE_SAMPLES
        .iter()
        .find(|(_, lang)| *lang == DOCS_LANG)
        .context("Documentation source not found in CODE_SAMPLES")?;

    let docs_content = reqwest::blocking::get(*docs_url)
        .context("Failed to fetch documentation code samples")?
        .text()
        .context("Failed to read documentation code samples response")?;

    let key_to_sample_ids = parse_documentation_mapping(&docs_content);

    // Fetch code samples from all sources
    let mut all_samples: HashMap<String, Vec<CodeSample>> = HashMap::new();

    for (url, lang) in CODE_SAMPLES {
        // For cURL, reuse already fetched content; for SDKs, fetch from URL
        let content: Cow<'_, str> = if *lang == DOCS_LANG {
            Cow::Borrowed(&docs_content)
        } else {
            match reqwest::blocking::get(*url).and_then(|r| r.text()) {
                Ok(text) => Cow::Owned(text),
                Err(e) => {
                    eprintln!("Warning: Failed to fetch code samples for {}: {}", lang, e);
                    continue;
                }
            }
        };

        let sample_id_to_code = parse_code_samples(&content);
        for (key, sample_ids) in &key_to_sample_ids {
            for sample_id in sample_ids {
                if let Some(source) = sample_id_to_code.get(sample_id) {
                    all_samples.entry(key.clone()).or_default().push(CodeSample {
                        lang: lang.to_string(),
                        source: source.clone(),
                    });
                }
            }
        }
    }

    Ok(all_samples)
}

/// Parse the documentation file to create a mapping from keys (comment IDs) to sample IDs
/// Returns: HashMap<key, Vec<sample_id>>
fn parse_documentation_mapping(content: &str) -> HashMap<String, Vec<String>> {
    let mut mapping: HashMap<String, Vec<String>> = HashMap::new();
    let mut current_key: Option<String> = None;

    for line in content.lines() {
        // Check if this is a comment line defining a new key
        if let Some(caps) = COMMENT_RE.captures(line) {
            current_key = Some(caps[1].to_string());
            continue;
        }

        // Check if this starts a new code block and extract the sample_id
        if let Some(caps) = CODE_START_RE.captures(line) {
            if let Some(ref key) = current_key {
                let sample_id = caps[1].to_string();
                mapping.entry(key.clone()).or_default().push(sample_id);
            }
        }
    }

    mapping
}

/// State machine for parsing YAML code blocks
struct YamlCodeBlockParser {
    current_value: Vec<String>,
    in_code_block: bool,
    base_indent: Option<usize>,
}

impl YamlCodeBlockParser {
    fn new() -> Self {
        Self { current_value: Vec::new(), in_code_block: false, base_indent: None }
    }

    fn start_new_block(&mut self) {
        self.current_value.clear();
        self.in_code_block = true;
        self.base_indent = None;
    }

    fn take_value(&mut self) -> Option<String> {
        if self.current_value.is_empty() {
            return None;
        }
        let value = self.current_value.join("\n").trim_end().to_string();
        self.current_value.clear();
        self.in_code_block = false;
        self.base_indent = None;
        Some(value)
    }

    fn process_line(&mut self, line: &str) {
        if !self.in_code_block {
            return;
        }

        // Empty line or line with only whitespace
        if line.trim().is_empty() {
            // Only add empty lines if we've already started collecting
            if !self.current_value.is_empty() {
                self.current_value.push(String::new());
            }
            return;
        }

        // Calculate indentation
        let indent = line.len() - line.trim_start().len();

        // Set base indent from first non-empty line
        let base = *self.base_indent.get_or_insert(indent);

        // If line has less indentation than base, we've exited the block
        if indent < base {
            self.in_code_block = false;
            return;
        }

        // Remove base indentation and add to value
        let dedented = line.get(base..).unwrap_or_else(|| line.trim_start());
        self.current_value.push(dedented.to_string());
    }
}

/// Parse a code samples YAML file
/// Returns: HashMap<sample_id, code>
fn parse_code_samples(content: &str) -> HashMap<String, String> {
    let mut samples: HashMap<String, String> = HashMap::new();
    let mut current_sample_id: Option<String> = None;
    let mut parser = YamlCodeBlockParser::new();

    for line in content.lines() {
        // Ignore comment lines
        if line.starts_with('#') {
            continue;
        }

        // Check if this starts a new code block
        if let Some(caps) = CODE_START_RE.captures(line) {
            // Save previous sample if exists
            if let Some(sample_id) = current_sample_id.take() {
                if let Some(value) = parser.take_value() {
                    samples.insert(sample_id, value);
                }
            }
            current_sample_id = Some(caps[1].to_string());
            parser.start_new_block();
            continue;
        }

        if current_sample_id.is_some() {
            parser.process_line(line);
        }
    }

    // Don't forget the last sample
    if let Some(sample_id) = current_sample_id {
        if let Some(value) = parser.take_value() {
            samples.insert(sample_id, value);
        }
    }

    samples
}

/// Convert an OpenAPI path to a code sample key
/// Path: /indexes/{index_uid}/documents/{document_id}
/// Method: GET
/// Key: get_indexes_indexUid_documents_documentId
fn path_to_key(path: &str, method: &str) -> String {
    let method_lower = method.to_lowercase();

    // Remove leading slash and convert path
    let path_part = path
        .trim_start_matches('/')
        .split('/')
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') {
                // Convert {param_name} to camelCase
                let param = &segment[1..segment.len() - 1];
                to_camel_case(param)
            } else {
                // Keep path segments as-is, but replace hyphens with underscores
                segment.replace('-', "_")
            }
        })
        .collect::<Vec<_>>()
        .join("_");

    if path_part.is_empty() {
        method_lower
    } else {
        format!("{}_{}", method_lower, path_part)
    }
}

/// Convert snake_case to camelCase
fn to_camel_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;

    for (i, c) in s.chars().enumerate() {
        match c {
            '_' => capitalize_next = true,
            _ if capitalize_next => {
                result.push(c.to_ascii_uppercase());
                capitalize_next = false;
            }
            _ if i == 0 => result.push(c.to_ascii_lowercase()),
            _ => result.push(c),
        }
    }

    result
}

/// Add code samples to the OpenAPI specification
fn add_code_samples_to_openapi(
    openapi: &mut Value,
    code_samples: &HashMap<String, Vec<CodeSample>>,
) -> Result<()> {
    let paths = openapi
        .get_mut("paths")
        .and_then(|p| p.as_object_mut())
        .context("OpenAPI spec missing 'paths' object")?;

    for (path, path_item) in paths.iter_mut() {
        let Some(path_item) = path_item.as_object_mut() else {
            continue;
        };

        for method in HTTP_METHODS {
            let Some(operation) = path_item.get_mut(*method) else {
                continue;
            };

            let key = path_to_key(path, method);

            if let Some(samples) = code_samples.get(&key) {
                // Create x-codeSamples array according to Redocly spec
                // Sort by language name for consistent output
                let mut sorted_samples = samples.clone();
                sorted_samples.sort_by(|a, b| a.lang.cmp(&b.lang));

                let code_sample_array: Vec<Value> = sorted_samples
                    .iter()
                    .map(|sample| {
                        json!({
                            "lang": sample.lang,
                            "source": sample.source
                        })
                    })
                    .collect();

                if let Some(op) = operation.as_object_mut() {
                    op.insert("x-codeSamples".to_string(), json!(code_sample_array));
                }
            }
        }
    }

    Ok(())
}

/// Clean up null descriptions in tags to make Mintlify work
/// Removes any "description" fields with null values (both JSON null and "null" string)
/// from the tags array and all nested objects
fn clean_null_descriptions(openapi: &mut Value) {
    if let Some(tags) = openapi.get_mut("tags").and_then(|t| t.as_array_mut()) {
        for tag in tags.iter_mut() {
            remove_null_descriptions_recursive(tag);
        }
    }
}

/// Recursively remove all "description" fields that are null or "null" string
fn remove_null_descriptions_recursive(value: &mut Value) {
    if let Some(obj) = value.as_object_mut() {
        // Check and remove description if it's null or "null" string
        if let Some(desc) = obj.get("description") {
            if desc.is_null() || (desc.is_string() && desc.as_str() == Some("null")) {
                obj.remove("description");
            }
        }

        // Recursively process all nested objects
        for (_, v) in obj.iter_mut() {
            remove_null_descriptions_recursive(v);
        }
    } else if let Some(arr) = value.as_array_mut() {
        // Recursively process arrays
        for item in arr.iter_mut() {
            remove_null_descriptions_recursive(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_key() {
        assert_eq!(path_to_key("/indexes", "GET"), "get_indexes");
        assert_eq!(path_to_key("/indexes/{index_uid}", "GET"), "get_indexes_indexUid");
        assert_eq!(
            path_to_key("/indexes/{index_uid}/documents", "POST"),
            "post_indexes_indexUid_documents"
        );
        assert_eq!(
            path_to_key("/indexes/{index_uid}/documents/{document_id}", "GET"),
            "get_indexes_indexUid_documents_documentId"
        );
        assert_eq!(
            path_to_key("/indexes/{index_uid}/settings/stop-words", "GET"),
            "get_indexes_indexUid_settings_stop_words"
        );
    }

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("index_uid"), "indexUid");
        assert_eq!(to_camel_case("document_id"), "documentId");
        assert_eq!(to_camel_case("task_uid"), "taskUid");
    }

    #[test]
    fn test_parse_documentation_mapping() {
        let yaml = r#"
# get_indexes
list_all_indexes_1: |-
  curl \
    -X GET 'MEILISEARCH_URL/indexes'
# post_indexes
create_an_index_1: |-
  curl \
    -X POST 'MEILISEARCH_URL/indexes'
another_sample_id: |-
  curl \
    -X POST 'MEILISEARCH_URL/indexes'
"#;
        let mapping = parse_documentation_mapping(yaml);

        assert_eq!(mapping.len(), 2);
        assert!(mapping.contains_key("get_indexes"));
        assert!(mapping.contains_key("post_indexes"));
        assert_eq!(mapping["get_indexes"], vec!["list_all_indexes_1"]);
        assert_eq!(mapping["post_indexes"], vec!["create_an_index_1", "another_sample_id"]);
    }

    #[test]
    fn test_parse_code_samples() {
        let yaml = r#"
# This is a comment that should be ignored
list_all_indexes_1: |-
  const client = new MeiliSearch({
    host: 'http://localhost:7700',
    apiKey: 'masterKey'
  });

  const response = await client.getIndexes();

# Another comment
create_an_index_1: |-
  const task = await client.createIndex('movies');
"#;
        let samples = parse_code_samples(yaml);

        assert_eq!(samples.len(), 2);
        assert!(samples.contains_key("list_all_indexes_1"));
        assert!(samples.contains_key("create_an_index_1"));
        assert!(samples["list_all_indexes_1"].contains("getIndexes"));
        assert!(samples["create_an_index_1"].contains("createIndex"));
    }

    #[test]
    fn test_clean_null_descriptions() {
        let mut openapi = json!({
            "tags": [
                {
                    "name": "Test1",
                    "description": "null"
                },
                {
                    "name": "Test2",
                    "description": null
                },
                {
                    "name": "Test3",
                    "description": "Valid description"
                },
                {
                    "name": "Test4",
                    "description": "null",
                    "externalDocs": {
                        "url": "https://example.com",
                        "description": null
                    }
                },
                {
                    "name": "Test5",
                    "externalDocs": {
                        "url": "https://example.com",
                        "description": "null"
                    }
                }
            ]
        });

        clean_null_descriptions(&mut openapi);

        let tags = openapi["tags"].as_array().unwrap();

        // Test1: description "null" should be removed
        assert!(!tags[0].as_object().unwrap().contains_key("description"));

        // Test2: description null should be removed
        assert!(!tags[1].as_object().unwrap().contains_key("description"));

        // Test3: valid description should remain
        assert_eq!(tags[2]["description"], "Valid description");

        // Test4: both tag description and externalDocs description should be removed
        assert!(!tags[3].as_object().unwrap().contains_key("description"));
        assert!(!tags[3]["externalDocs"]
            .as_object()
            .unwrap()
            .contains_key("description"));
        assert_eq!(tags[3]["externalDocs"]["url"], "https://example.com");

        // Test5: externalDocs description "null" should be removed
        assert!(!tags[4]["externalDocs"]
            .as_object()
            .unwrap()
            .contains_key("description"));
        assert_eq!(tags[4]["externalDocs"]["url"], "https://example.com");
    }
}
