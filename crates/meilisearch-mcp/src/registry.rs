use crate::protocol::Tool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use utoipa::openapi::{OpenApi, PathItem};
use utoipa::openapi::path::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    pub http_method: String,
    pub path_template: String,
}

pub struct McpToolRegistry {
    tools: HashMap<String, McpTool>,
}

impl McpToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }

    pub fn from_openapi(openapi: &OpenApi) -> Self {
        let mut registry = Self::new();

        // openapi.paths is of type Paths
        for (path, path_item) in openapi.paths.paths.iter() {
            registry.process_path_item(path, path_item);
        }

        registry
    }

    pub fn register_tool(&mut self, tool: McpTool) {
        self.tools.insert(tool.name.clone(), tool);
    }

    pub fn get_tool(&self, name: &str) -> Option<&McpTool> {
        self.tools.get(name)
    }

    pub fn list_tools(&self) -> Vec<Tool> {
        self.tools
            .values()
            .map(|mcp_tool| Tool {
                name: mcp_tool.name.clone(),
                description: mcp_tool.description.clone(),
                input_schema: mcp_tool.input_schema.clone(),
            })
            .collect()
    }

    fn process_path_item(&mut self, path: &str, path_item: &PathItem) {
        let methods = [
            ("GET", &path_item.get),
            ("POST", &path_item.post),
            ("PUT", &path_item.put),
            ("DELETE", &path_item.delete),
            ("PATCH", &path_item.patch),
        ];

        for (method_type, operation) in methods {
            if let Some(op) = operation {
                if let Some(tool) = McpTool::from_operation(path, method_type, op) {
                    self.register_tool(tool);
                }
            }
        }
    }
}

impl McpTool {
    pub fn from_openapi_path(
        path: &str,
        method: &str,
        _path_item: &PathItem,
    ) -> Self {
        // This is a simplified version for testing
        // In the real implementation, we would extract from the PathItem
        let name = Self::generate_tool_name(path, method);
        let description = format!("{} {}", method, path);
        
        let input_schema = json!({
            "type": "object",
            "properties": {},
            "required": []
        });

        Self {
            name,
            description,
            input_schema,
            http_method: method.to_string(),
            path_template: path.to_string(),
        }
    }

    fn from_operation(path: &str, method: &str, operation: &Operation) -> Option<Self> {
        let name = Self::generate_tool_name(path, method);
        let description = operation
            .summary
            .as_ref()
            .or(operation.description.as_ref())
            .cloned()
            .unwrap_or_else(|| format!("{} {}", method, path));

        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();

        // Extract path parameters
        if let Some(params) = &operation.parameters {
            for param in params {
                let camel_name = to_camel_case(&param.name);
                
                properties.insert(
                    camel_name.clone(),
                    json!({
                        "type": "string",
                        "description": param.description.as_deref().unwrap_or("")
                    }),
                );

                if matches!(param.required, utoipa::openapi::Required::True) {
                    required.push(camel_name);
                }
            }
        }

        // Extract request body schema
        if let Some(request_body) = &operation.request_body {
            if let Some(content) = request_body.content.get("application/json") {
                if let Some(schema) = &content.schema {
                    // Merge request body schema into properties
                    if let Some(body_props) = extract_schema_properties(schema) {
                        for (key, value) in body_props {
                            properties.insert(key, value);
                        }
                    }
                }
            }
        }

        let input_schema = json!({
            "type": "object",
            "properties": properties,
            "required": required,
        });

        Some(Self {
            name,
            description,
            input_schema,
            http_method: method.to_string(),
            path_template: path.to_string(),
        })
    }

    pub fn generate_tool_name(path: &str, method: &str) -> String {
        let parts: Vec<&str> = path
            .split('/')
            .filter(|s| !s.is_empty() && !s.starts_with('{'))
            .collect();

        let resource = parts.last().unwrap_or(&"resource");
        let is_collection = !path.contains('}') || path.ends_with('}');

        match method.to_uppercase().as_str() {
            "GET" => {
                if is_collection && !path.contains('{') {
                    // Don't pluralize if already plural
                    if resource.ends_with('s') {
                        format!("get{}", to_pascal_case(resource))
                    } else {
                        format!("get{}", to_pascal_case(&pluralize(resource)))
                    }
                } else {
                    format!("get{}", to_pascal_case(&singularize(resource)))
                }
            }
            "POST" => {
                if resource == &"search" {
                    "searchDocuments".to_string()
                } else if resource == &"multi-search" {
                    "multiSearch".to_string()
                } else if resource == &"swap-indexes" {
                    "swapIndexes".to_string()
                } else {
                    format!("create{}", to_pascal_case(&singularize(resource)))
                }
            }
            "PUT" => format!("update{}", to_pascal_case(&singularize(resource))),
            "DELETE" => format!("delete{}", to_pascal_case(&singularize(resource))),
            "PATCH" => format!("update{}", to_pascal_case(&singularize(resource))),
            _ => format!("{}{}", method.to_lowercase(), to_pascal_case(resource)),
        }
    }
}

fn to_camel_case(s: &str) -> String {
    let parts: Vec<&str> = s.split(&['_', '-'][..]).collect();
    if parts.is_empty() {
        return String::new();
    }
    
    let mut result = parts[0].to_lowercase();
    for part in &parts[1..] {
        result.push_str(&to_pascal_case(part));
    }
    result
}

fn to_pascal_case(s: &str) -> String {
    s.split(&['_', '-'][..])
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|c| c.to_uppercase().collect::<String>() + chars.as_str().to_lowercase().as_str())
                .unwrap_or_default()
        })
        .collect()
}

fn singularize(word: &str) -> String {
    if word.ends_with("ies") {
        word[..word.len() - 3].to_string() + "y"
    } else if word.ends_with("es") {
        word[..word.len() - 2].to_string()
    } else if word.ends_with('s') {
        word[..word.len() - 1].to_string()
    } else {
        word.to_string()
    }
}

fn pluralize(word: &str) -> String {
    if word.ends_with('y') {
        word[..word.len() - 1].to_string() + "ies"
    } else if word.ends_with('s') || word.ends_with('x') || word.ends_with("ch") {
        word.to_string() + "es"
    } else {
        word.to_string() + "s"
    }
}

fn extract_schema_properties(schema: &utoipa::openapi::RefOr<utoipa::openapi::Schema>) -> Option<serde_json::Map<String, Value>> {
    // This is a simplified extraction - in a real implementation, 
    // we would properly handle $ref resolution and nested schemas
    match schema {
        utoipa::openapi::RefOr::T(_schema) => {
            // Extract properties from the schema
            // This would need proper implementation based on the schema type
            Some(serde_json::Map::new())
        }
        utoipa::openapi::RefOr::Ref { .. } => {
            // Handle schema references
            Some(serde_json::Map::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_name_generation() {
        assert_eq!(McpTool::generate_tool_name("/indexes", "GET"), "getIndexes");
        assert_eq!(McpTool::generate_tool_name("/indexes/{index_uid}", "GET"), "getIndex");
        assert_eq!(McpTool::generate_tool_name("/indexes/{index_uid}/search", "POST"), "searchDocuments");
    }

    #[test]
    fn test_camel_case_conversion() {
        assert_eq!(to_camel_case("index_uid"), "indexUid");
        assert_eq!(to_camel_case("document-id"), "documentId");
        assert_eq!(to_camel_case("simple"), "simple");
    }

    #[test]
    fn test_pascal_case_conversion() {
        assert_eq!(to_pascal_case("index"), "Index");
        assert_eq!(to_pascal_case("multi-search"), "MultiSearch");
        assert_eq!(to_pascal_case("api_key"), "ApiKey");
    }
}