# PRD: URL Fetching for Embeddings

## Overview

Enable Meilisearch to automatically fetch content from URLs during the embedding extraction process. This allows users to store URLs in their documents (e.g., `imageUrl`) and have Meilisearch download the content, convert it to base64, and pass it to the embedder—without persisting the fetched content in the database.

## Problem Statement

Currently, to use image or document embeddings in Meilisearch, users must:
1. Download the media content themselves
2. Convert it to base64
3. Include the base64 data in the document

This creates friction for users who already have media hosted on CDNs or external services. They want to simply provide URLs and let Meilisearch handle the fetching.

## Goals

- Allow users to specify a document field containing a URL to fetch
- Fetch content during embedding extraction (not stored in DB)
- Provide security controls (allowed domains, size limits, timeouts)
- Work with any media type (images, PDFs, etc.)
- Simple, minimal configuration for common use cases

## Non-Goals

- Text extraction from PDFs (embedder's responsibility)
- Caching fetched content across indexing sessions
- Storing fetched content in the document database
- Fetching multiple URLs per embedder (keep it simple)

## API Design

### Embedder Configuration

**Minimal configuration (works out of the box):**
```json
{
  "embedders": {
    "multimodal": {
      "source": "rest",
      "url": "https://embedding-service/embed",
      "fetchUrl": {
        "input": "imageUrl",
        "output": "_image"
      },
      "request": {
        "image": "{{doc._image}}"
      }
    }
  }
}
```

**Full configuration with all options:**
```json
{
  "embedders": {
    "multimodal": {
      "source": "rest",
      "url": "https://embedding-service/embed",
      "fetchUrl": {
        "input": "imageUrl",
        "output": "_image",
        "allowedDomains": ["cdn.example.com", "*.s3.amazonaws.com"],
        "timeout": 15000,
        "maxSize": "20MB",
        "retries": 3,
        "outputFormat": "dataUri"
      },
      "request": {
        "image": "{{doc._image}}"
      }
    }
  }
}
```

### Configuration Schema

#### `fetchUrl` (Object)

Specifies a document field containing a URL and the virtual field name to expose the fetched content.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `input` | `string` | Yes | - | Document field containing the URL (supports nested paths like `media.imageUrl`) |
| `output` | `string` | Yes | - | Virtual field name for use in templates |
| `allowedDomains` | `string[]` | No | `["*"]` | Allowed domains. `["*"]` allows any. Supports wildcards: `["*.s3.amazonaws.com"]` |
| `timeout` | `number` | No | `10000` | Request timeout in milliseconds |
| `maxSize` | `string` | No | `"10MB"` | Maximum content size to download |
| `retries` | `number` | No | `2` | Number of retry attempts on failure |
| `outputFormat` | `string` | No | `"dataUri"` | Output format: `"dataUri"` (with MIME prefix) or `"base64"` (raw) |

### Behavior

1. **During document indexing**: When extracting vectors, Meilisearch checks if the embedder has `fetchUrl` configured
2. **URL resolution**: Read the URL from the `input` field (supports nested paths)
3. **Domain validation**: Check URL domain against `allowedDomains`
4. **Content fetching**: Download content with configured timeout/retries
5. **Size validation**: Check content size against `maxSize`
6. **Encoding**: Convert fetched content to base64 or data URI based on `outputFormat`
7. **Template injection**: Make content available as `output` field in template context
8. **No persistence**: Fetched content is discarded after embedding extraction

### Virtual Fields

Virtual fields (e.g., `_image`) are:
- Only available during template rendering for that embedder
- Not stored in the document database
- Not returned in search results
- Prefixed with `_` by convention (not enforced)

### Document Example

**Stored document:**
```json
{
  "id": 1,
  "title": "Product Photo",
  "imageUrl": "https://cdn.example.com/product.jpg"
}
```

**What embedder template sees:**
```json
{
  "id": 1,
  "title": "Product Photo",
  "imageUrl": "https://cdn.example.com/product.jpg",
  "_image": "data:image/jpeg;base64,/9j/4AAQSkZJRg..."
}
```

**What is persisted:** Only original document (URL as string)

## Security Considerations

### SSRF Protection

- `allowedDomains` defaults to `["*"]` for ease of use
- For production, users should restrict to specific domains
- Wildcard support for subdomains: `["*.example.com"]`
- Private IP ranges should be blocked by default (localhost, 10.x.x.x, 192.168.x.x, etc.)

### Resource Limits

- `maxSize` prevents downloading excessively large files
- `timeout` prevents hanging on slow/unresponsive servers
- Consider global rate limiting across all URL fetches

### Content Validation

- Validate Content-Type header matches expected media types (optional future enhancement)
- Reject responses with suspicious content

## Error Handling

| Scenario | Behavior |
|----------|----------|
| URL field is null/missing | Skip fetch, `output` field is not set |
| URL field is empty string | Skip fetch, `output` field is not set |
| Domain not in allowedDomains | Error logged, document embedding fails |
| Timeout exceeded | Retry up to `retries` times, then fail |
| Content exceeds maxSize | Error logged, document embedding fails |
| HTTP error (4xx, 5xx) | Retry on 5xx, fail on 4xx |
| Network error | Retry up to `retries` times, then fail |

When a fetch fails, the document's embedding for that embedder fails, but other embedders and other documents continue processing.

## Implementation Plan

### Phase 1: Configuration Types

1. Add `FetchUrlMapping` struct to `milli/src/vector/settings.rs`
2. Include all options inline (input, output, allowedDomains, timeout, maxSize, retries, outputFormat)
3. Update `EmbeddingSettings` to include `fetch_url: Option<FetchUrlMapping>`
4. Add serialization/deserialization with proper validation and defaults

### Phase 2: URL Fetching Service

1. Create `milli/src/vector/url_fetcher.rs` module
2. Implement `UrlFetcher` struct with:
   - HTTP client (reuse existing ureq setup)
   - Domain validation with wildcard support
   - Size limit enforcement
   - Retry logic with exponential backoff
3. Add private IP blocking for SSRF protection

### Phase 3: Integration with Vector Extraction

1. Modify `EmbeddingExtractor` in `milli/src/update/new/extract/vectors/mod.rs`
2. Before template rendering, check for `fetchUrl` config
3. Fetch URL and inject virtual field into document context
4. Pass enriched context to template renderer

### Phase 4: Testing

1. Unit tests for URL fetcher (mocked HTTP)
2. Integration tests for full pipeline
3. Security tests (SSRF, size limits)
4. Error handling tests

## Future Enhancements

- **Content-Type validation**: Restrict to specific MIME types
- **Caching**: Cache fetched content during a single indexing batch
- **Async fetching**: Parallel URL fetches for better performance
- **Multiple URLs**: Support array of fetchUrl mappings if needed
- **Text extraction**: Built-in PDF/DOCX text extraction option

## Open Questions

1. Should we support authentication headers per-URL (e.g., signed URLs)?
2. Should private IP blocking be configurable or always enforced?
3. Should we add metrics/logging for fetch operations?
