# Meilisearch MCP Server

This crate implements a Model Context Protocol (MCP) server for Meilisearch, enabling AI assistants and LLM applications to interact with Meilisearch through a standardized protocol.

## Overview

The MCP server automatically exposes all Meilisearch HTTP API endpoints as MCP tools, allowing AI assistants to:
- Search documents
- Manage indexes
- Add, update, or delete documents
- Configure settings
- Monitor tasks
- And more...

## Architecture

### Dynamic Tool Generation

The server dynamically generates MCP tools from Meilisearch's OpenAPI specification. This ensures:
- Complete API coverage
- Automatic updates when new endpoints are added
- Consistent parameter validation
- Type-safe operations

### Components

1. **Protocol Module** (`protocol.rs`): Defines MCP protocol types and messages
2. **Registry Module** (`registry.rs`): Converts OpenAPI specs to MCP tools
3. **Server Module** (`server.rs`): Handles MCP requests and SSE communication
4. **Integration Module** (`integration.rs`): Connects with the main Meilisearch server

## Usage

### Enabling the MCP Server

The MCP server is an optional feature. To enable it:

```bash
cargo build --release --features mcp
```

### Accessing the MCP Server

Once enabled, the MCP server is available at:
- SSE endpoint: `GET /mcp`
- HTTP endpoint: `POST /mcp`

### Authentication

The MCP server integrates with Meilisearch's existing authentication:

```json
{
  "method": "tools/call",
  "params": {
    "name": "searchDocuments",
    "arguments": {
      "_auth": {
        "apiKey": "your-api-key"
      },
      "indexUid": "movies",
      "q": "search query"
    }
  }
}
```

## Protocol Flow

1. **Initialize**: Client establishes connection and negotiates protocol version
2. **List Tools**: Client discovers available Meilisearch operations
3. **Call Tools**: Client executes Meilisearch operations through MCP tools
4. **Stream Results**: Server streams responses, especially for long-running operations

## Example Interactions

### Initialize Connection

```json
{
  "method": "initialize",
  "params": {
    "protocol_version": "2024-11-05",
    "capabilities": {},
    "client_info": {
      "name": "my-ai-assistant",
      "version": "1.0.0"
    }
  }
}
```

### List Available Tools

```json
{
  "method": "tools/list"
}
```

Response includes tools like:
- `searchDocuments` - Search within an index
- `createIndex` - Create a new index
- `addDocuments` - Add documents to an index
- `getTask` - Check task status
- And many more...

### Search Documents

```json
{
  "method": "tools/call",
  "params": {
    "name": "searchDocuments",
    "arguments": {
      "indexUid": "products",
      "q": "laptop",
      "filter": "price < 1000",
      "limit": 20,
      "attributesToRetrieve": ["name", "price", "description"]
    }
  }
}
```

## Testing

The crate includes comprehensive tests:

```bash
# Run all tests
cargo test -p meilisearch-mcp

# Run specific test categories
cargo test -p meilisearch-mcp conversion_tests
cargo test -p meilisearch-mcp integration_tests
cargo test -p meilisearch-mcp e2e_tests
```

## Development

### Adding New Features

Since tools are generated dynamically from the OpenAPI specification, new Meilisearch endpoints are automatically available through MCP without code changes.

### Customizing Tool Names

Tool names are generated automatically from endpoint paths and HTTP methods. The naming convention:
- `GET /indexes` → `getIndexes`
- `POST /indexes/{index_uid}/search` → `searchDocuments`
- `DELETE /indexes/{index_uid}` → `deleteIndex`

## Future Enhancements

- WebSocket support for bidirectional communication
- Tool result caching
- Batch operations
- Custom tool aliases
- Rate limiting per MCP client