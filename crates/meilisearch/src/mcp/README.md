# MCP (Model Context Protocol) Server

Meilisearch includes an experimental MCP server that allows AI assistants (like Claude, ChatGPT, or custom LLM applications) to search your Meilisearch indexes using the standardized [Model Context Protocol](https://modelcontextprotocol.io/).

## Overview

The MCP server exposes Meilisearch capabilities as "tools" that AI assistants can discover and invoke. This enables natural language search queries to be translated into structured Meilisearch searches without requiring the AI to know the specific API details.

## Activation

MCP is an experimental feature that must be enabled at runtime via the experimental features API.

### Enable MCP

```bash
curl -X PATCH 'http://localhost:7700/experimental-features' \
  -H 'Content-Type: application/json' \
  -d '{"mcp": true}'
```

### Check MCP Status

```bash
curl 'http://localhost:7700/experimental-features'
```

Response:
```json
{
  "metrics": false,
  "logsRoute": false,
  "editDocumentsByFunction": false,
  "containsFilter": false,
  "network": false,
  "getTaskDocumentsRoute": false,
  "compositeEmbedders": false,
  "chatCompletions": false,
  "multimodal": false,
  "vectorStoreSetting": false,
  "mcp": true
}
```

### Disable MCP

```bash
curl -X PATCH 'http://localhost:7700/experimental-features' \
  -H 'Content-Type: application/json' \
  -d '{"mcp": false}'
```

## Protocol

The MCP server implements [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over HTTP at the `/mcp` endpoint.

### Endpoint

- **URL**: `POST /mcp`
- **Content-Type**: `application/json`

### Authentication

The MCP server uses the same API key authentication system as the rest of Meilisearch. This means you can use your existing API keys to control access to MCP tools.

#### How It Works

1. **No master key configured**: MCP is fully accessible without authentication (open access mode)
2. **Master key configured**: All MCP requests require a valid API key via the `Authorization: Bearer` header
3. **Per-request authentication**: Each HTTP request must include the Authorization header (the MCP session does not store credentials)

#### Creating API Keys for MCP

Use the standard Meilisearch keys API to create keys with specific permissions:

```bash
# Create a search-only API key for MCP clients
curl -X POST 'http://localhost:7700/keys' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <master-key>' \
  -d '{
    "name": "MCP Search Key",
    "description": "API key for MCP clients - search only",
    "actions": ["search"],
    "indexes": ["*"],
    "expiresAt": null
  }'

# Create a full-access MCP key (search + index info)
curl -X POST 'http://localhost:7700/keys' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <master-key>' \
  -d '{
    "name": "MCP Full Access Key",
    "description": "API key for MCP clients - full access",
    "actions": ["search", "indexes.get", "settings.get"],
    "indexes": ["*"],
    "expiresAt": null
  }'

# Create a key scoped to specific indexes
curl -X POST 'http://localhost:7700/keys' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <master-key>' \
  -d '{
    "name": "MCP Movies Key",
    "description": "API key for MCP clients - movies index only",
    "actions": ["search", "indexes.get", "settings.get"],
    "indexes": ["movies", "actors"],
    "expiresAt": null
  }'
```

#### Tool Permissions

Each MCP tool requires specific API key actions:

| Tool | Required Actions | Description |
|------|------------------|-------------|
| `meilisearch_list_indexes` | `indexes.get` | List available indexes |
| `meilisearch_get_index_info` | `indexes.get` + `settings.get` | Get index details and settings |
| `meilisearch_search` | `search` | Perform searches |

**Dynamic Tool Filtering**: When calling `tools/list`, only tools the API key has permission to use are returned. For example, a search-only key will only see `meilisearch_search` in the tools list.

#### Index Scoping

API keys can be scoped to specific indexes. When using such keys:
- `meilisearch_list_indexes` only returns indexes the key can access
- `meilisearch_get_index_info` and `meilisearch_search` return an error for unauthorized indexes

#### Making Authenticated Requests

Include the `Authorization` header in every MCP request:

```bash
# Initialize with authentication
curl -X POST 'http://localhost:7700/mcp' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <your-api-key>' \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "clientInfo": {"name": "my-app", "version": "1.0"},
      "capabilities": {}
    }
  }'

# Subsequent requests also need the Authorization header
curl -X POST 'http://localhost:7700/mcp' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <your-api-key>' \
  -H 'Mcp-Session-Id: <session-id>' \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```

#### Authentication Errors

**Missing Authorization Header** (when master key is configured):
```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32600,
    "message": "Missing Authorization header. Use 'Authorization: Bearer <api-key>'",
    "data": {
      "type": "authentication_required",
      "code": "missing_authorization_header",
      "context": {
        "fix": "Add 'Authorization: Bearer <api-key>' header to your request",
        "docs": "https://www.meilisearch.com/docs/reference/api/keys"
      }
    }
  }
}
```

**Invalid API Key**:
```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32600,
    "message": "Invalid API key",
    "data": {
      "type": "authentication_failed",
      "code": "invalid_api_key",
      "context": {
        "fix": "Check that your API key is valid and not expired",
        "docs": "https://www.meilisearch.com/docs/reference/api/keys"
      }
    }
  }
}
```

**Unauthorized Tool** (API key lacks required permission):
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "error": {
    "code": -32000,
    "message": "API key does not have 'indexes.get' permission required for tool 'meilisearch_list_indexes'",
    "data": {
      "type": "unauthorized",
      "code": "insufficient_permissions",
      "context": {
        "tool": "meilisearch_list_indexes",
        "required_action": "indexes.get",
        "fix": "Use an API key with 'indexes.get' permission",
        "docs": "https://www.meilisearch.com/docs/reference/api/keys#actions"
      }
    }
  }
}
```

**Unauthorized Index** (API key cannot access requested index):
```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "error": {
    "code": -32000,
    "message": "API key cannot access index 'secret_data'. Allowed indexes: [\"movies\", \"actors\"]",
    "data": {
      "type": "index_unauthorized",
      "code": "index_access_denied",
      "context": {
        "requested_index": "secret_data",
        "allowed_indexes": ["movies", "actors"],
        "fix": "Use an API key with access to 'secret_data' or search from allowed indexes"
      }
    }
  }
}
```

### Session Management

MCP uses session-based state management. After initialization, the server returns a session ID that must be included in subsequent requests.

**Session Header**: `Mcp-Session-Id: <session-id>`

Sessions automatically expire after 1 hour of inactivity.

## Usage Flow

### 1. Initialize Session

Every MCP interaction starts with an `initialize` request:

```bash
curl -X POST 'http://localhost:7700/mcp' \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "clientInfo": {
        "name": "my-ai-app",
        "version": "1.0.0"
      },
      "capabilities": {}
    }
  }'
```

Response includes the session ID in headers:
```
Mcp-Session-Id: abc123-session-id
```

Response body:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocolVersion": "2024-11-05",
    "serverInfo": {
      "name": "meilisearch",
      "version": "1.28.1"
    },
    "capabilities": {
      "tools": {
        "listChanged": false
      }
    }
  }
}
```

### 2. List Available Tools

Discover what tools the server provides:

```bash
curl -X POST 'http://localhost:7700/mcp' \
  -H 'Content-Type: application/json' \
  -H 'Mcp-Session-Id: <session-id>' \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```

### 3. Call Tools

Invoke tools to interact with Meilisearch:

```bash
curl -X POST 'http://localhost:7700/mcp' \
  -H 'Content-Type: application/json' \
  -H 'Mcp-Session-Id: <session-id>' \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "meilisearch_search",
      "arguments": {
        "indexUid": "movies",
        "q": "science fiction"
      }
    }
  }'
```

### 4. Batch Requests

Multiple requests can be batched in a single HTTP call:

```bash
curl -X POST 'http://localhost:7700/mcp' \
  -H 'Content-Type: application/json' \
  -d '[
    {
      "jsonrpc": "2.0",
      "id": 1,
      "method": "initialize",
      "params": {
        "protocolVersion": "2024-11-05",
        "clientInfo": {"name": "test", "version": "1.0"},
        "capabilities": {}
      }
    },
    {
      "jsonrpc": "2.0",
      "id": 2,
      "method": "tools/list",
      "params": {}
    }
  ]'
```

## Available Tools

### meilisearch_list_indexes

List all available Meilisearch indexes with basic information.

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | integer | No | 20 | Maximum indexes to return (1-100) |
| `offset` | integer | No | 0 | Number of indexes to skip |

**Example:**
```json
{
  "name": "meilisearch_list_indexes",
  "arguments": {
    "limit": 10,
    "offset": 0
  }
}
```

**Response:**
```json
{
  "results": [
    {
      "uid": "movies",
      "primaryKey": "id",
      "numberOfDocuments": 19546,
      "createdAt": "2024-01-15T10:30:00Z",
      "updatedAt": "2024-01-20T14:22:00Z"
    }
  ],
  "offset": 0,
  "limit": 10,
  "total": 1
}
```

### meilisearch_get_index_info

Get detailed information about a specific index including filterable/sortable attributes and embedder configuration.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `indexUid` | string | Yes | Index identifier |

**Example:**
```json
{
  "name": "meilisearch_get_index_info",
  "arguments": {
    "indexUid": "movies"
  }
}
```

**Response:**
```json
{
  "uid": "movies",
  "primaryKey": "id",
  "numberOfDocuments": 19546,
  "searchableAttributes": ["title", "overview", "genres"],
  "filterableAttributes": ["genres", "release_year", "rating"],
  "sortableAttributes": ["release_year", "rating", "title"],
  "embedders": {
    "default": {
      "source": "OpenAi",
      "quantized": false
    }
  }
}
```

### meilisearch_search

Perform full-text, semantic, or hybrid search on an index.

**Parameters:**
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `indexUid` | string | Yes | - | Target index identifier |
| `q` | string | No | - | Search query string |
| `vector` | array[number] | No | - | Query vector for semantic search |
| `hybrid` | object | No | - | Hybrid search configuration |
| `hybrid.embedder` | string | Yes* | - | Embedder name (*required if hybrid is set) |
| `hybrid.semanticRatio` | number | No | 0.5 | Balance: 0.0=keyword, 1.0=semantic |
| `filter` | string | No | - | Filter expression |
| `sort` | array[string] | No | - | Sort criteria |
| `limit` | integer | No | 20 | Max results (1-1000) |
| `offset` | integer | No | 0 | Results to skip |
| `attributesToRetrieve` | array[string] | No | all | Fields to return |
| `attributesToHighlight` | array[string] | No | - | Fields to highlight |
| `showRankingScore` | boolean | No | false | Include relevance scores |
| `rankingScoreThreshold` | number | No | - | Min score filter (0.0-1.0) |

**Example - Basic Search:**
```json
{
  "name": "meilisearch_search",
  "arguments": {
    "indexUid": "movies",
    "q": "science fiction adventure",
    "limit": 5
  }
}
```

**Example - Filtered Search:**
```json
{
  "name": "meilisearch_search",
  "arguments": {
    "indexUid": "movies",
    "q": "adventure",
    "filter": "genres = 'Action' AND release_year > 2000",
    "sort": ["rating:desc"],
    "attributesToRetrieve": ["title", "overview", "rating"]
  }
}
```

**Example - Hybrid Search:**
```json
{
  "name": "meilisearch_search",
  "arguments": {
    "indexUid": "movies",
    "q": "movies about space exploration",
    "hybrid": {
      "embedder": "default",
      "semanticRatio": 0.7
    },
    "showRankingScore": true
  }
}
```

**Response:**
```json
{
  "hits": [
    {
      "id": 11,
      "title": "Interstellar",
      "overview": "A team of explorers travel through a wormhole...",
      "_rankingScore": 0.92
    }
  ],
  "query": "movies about space exploration",
  "processingTimeMs": 12,
  "limit": 20,
  "offset": 0,
  "estimatedTotalHits": 45
}
```

## Error Handling

Errors follow JSON-RPC 2.0 conventions:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32600,
    "message": "using MCP requires enabling the `mcp` experimental feature"
  }
}
```

### Error Codes

| Code | Meaning |
|------|---------|
| -32700 | Parse error - Invalid JSON |
| -32600 | Invalid request - Missing required fields |
| -32601 | Method not found - Unknown method |
| -32602 | Invalid params - Bad parameter values |
| -32603 | Internal error - Server error |

### Feature Not Enabled Error

If MCP is not enabled, requests return:
```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32600,
    "message": "using MCP requires enabling the `mcp` experimental feature. See https://github.com/orgs/meilisearch/discussions/868"
  }
}
```

## Integration Examples

### Claude Desktop

Add to Claude Desktop's MCP configuration:

```json
{
  "mcpServers": {
    "meilisearch": {
      "url": "http://localhost:7700/mcp",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

**Note**:
- Requires enabling MCP on the Meilisearch instance first
- If no master key is configured on Meilisearch, you can omit the `headers` section
- Use an API key with appropriate permissions (see [Authentication](#authentication))

### Custom Integration (Python)

```python
import requests

class MeilisearchMCP:
    def __init__(self, host="http://localhost:7700", api_key=None):
        self.host = host
        self.api_key = api_key
        self.session_id = None

    def _get_headers(self):
        """Build headers including auth and session ID."""
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        return headers

    def initialize(self):
        """Initialize MCP session."""
        response = requests.post(
            f"{self.host}/mcp",
            headers=self._get_headers(),
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "clientInfo": {"name": "python-client", "version": "1.0"},
                    "capabilities": {}
                }
            }
        )
        response.raise_for_status()
        self.session_id = response.headers.get("Mcp-Session-Id")
        return response.json()

    def list_tools(self):
        """List available tools (filtered by API key permissions)."""
        return requests.post(
            f"{self.host}/mcp",
            headers=self._get_headers(),
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            }
        ).json()

    def list_indexes(self, limit=20, offset=0):
        """List indexes accessible to this API key."""
        return requests.post(
            f"{self.host}/mcp",
            headers=self._get_headers(),
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "meilisearch_list_indexes",
                    "arguments": {"limit": limit, "offset": offset}
                }
            }
        ).json()

    def search(self, index_uid, query, **kwargs):
        """Search an index."""
        return requests.post(
            f"{self.host}/mcp",
            headers=self._get_headers(),
            json={
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "meilisearch_search",
                    "arguments": {"indexUid": index_uid, "q": query, **kwargs}
                }
            }
        ).json()


# Usage without authentication (no master key configured)
mcp = MeilisearchMCP()
mcp.initialize()
results = mcp.search("movies", "science fiction", limit=5)

# Usage with authentication (master key configured)
mcp = MeilisearchMCP(api_key="your-api-key-here")
mcp.initialize()

# Check which tools are available with this key
tools = mcp.list_tools()
print(f"Available tools: {[t['name'] for t in tools['result']['tools']]}")

# Search
results = mcp.search("movies", "science fiction", limit=5)
```

## Limitations

- MCP is an experimental feature and may change in future versions
- Session state does not persist across Meilisearch restarts
- Maximum of 1000 concurrent sessions

## Related Documentation

- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [Meilisearch Search API](https://www.meilisearch.com/docs/reference/api/search)
- [Meilisearch API Keys](https://www.meilisearch.com/docs/reference/api/keys)
- [Experimental Features](https://www.meilisearch.com/docs/reference/api/experimental_features)
