# Declarative upgrade tests

Declarative upgrade tests ensure that Meilisearch features remain stable across versions.

While we already have unit tests, those are run against **temporary databases** that are created fresh each time and therefore never risk corruption.

Upgrade tests instead **simulate the lifetime of a database**: they chain together commands and version upgrades, verifying that database state and API responses remain consistent.

## Basic example

```json
{
  "type": "test",
  "name": "api-keys",
  "initialVersion": "1.19.0", // the first command will run on a brand new database of this version
  "commands": []
}
```

This example defines a no-op test (it does nothing).

If the file is saved at `workloads/tests/example.json`, you can run it with:

```bash
cargo xtask test workloads/tests/example.json
```

## Commands

Commands represent API requests sent to Meilisearch endpoints during a test.

They are executed sequentially, and their responses can be validated to ensure consistent behavior across upgrades.

```json

{
  "route": "keys",
  "method": "POST",
  "body": {
    "inline": {
      "actions": [
        "search",
        "documents.add"
      ],
      "description": "Test API Key",
      "expiresAt": null,
      "indexes": [ "movies" ]
    }
  }
}
```

This command issues a `POST /keys` request, creating an API key with permissions to search and add documents in the `movies` index.

### Using assets in commands

To keep tests concise and reusable, you can define **assets** at the root of the workload file.

Assets are external data sources (such as datasets) that are cached between runs, making tests faster and easier to read.

```json
{
  "type": "test",
  "name": "movies",
  "initialVersion": "1.12.0",
  "assets": {
    "movies.json": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/movies.json",
      "sha256": "5b6e4cb660bc20327776e8a33ea197b43d9ec84856710ead1cc87ab24df77de1"
    }
  },
  "commands": [
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "movies.json"
      }
    }
  ]
}
```

In this example:
- The `movies.json` dataset is defined as an asset, pointing to a remote URL.
- The SHA-256 checksum ensures integrity.
- The `POST /indexes/movies/documents` command uses this asset as the request body.

This makes the test much cleaner than inlining a large dataset directly into the command.

### Asserting responses

Commands can specify both the **expected status code** and the **expected response body**.

```json
{
  "route": "indexes/movies/documents",
  "method": "POST",
  "body": {
    "asset": "movies.json"
  },
  "expectedStatus": 202,
  "expectedResponse": {
    "enqueuedAt": "[timestamp]", // Set to a bracketed string to ignore the value
    "indexUid": "movies",
    "status": "enqueued",
    "taskUid": 1,
    "type": "documentAdditionOrUpdate"
  },
  "synchronous": "WaitForTask"
}
```

Manually writing `expectedResponse` fields can be tedious.

Instead, you can let the test runner populate them automatically:

```bash
# Run the workload to populate expected fields. Only adds the missing ones, doesn't change existing data
cargo xtask test workloads/tests/example.json --add-missing-responses

# OR

# Run the workload to populate expected fields. Updates all fields including existing ones
cargo xtask test workloads/tests/example.json --update-responses
```

This workflow is recommended:

1. Write the test without expected fields.
2. Run it with `--add-missing-responses` to capture the actual responses.
3. Review and commit the generated expectations.

## Upgrade commands

Upgrade commands allow you to switch the Meilisearch instance from one version to another during a test.

When executed, an upgrade command will:
1. Stop the current Meilisearch server.
2. Upgrade the database to the specified version.
3. Restart the server with the new specified version.

### Typical Usage

In most cases, you will:

- **Set up** some data using commands on an older version.
- **Upgrade** to the latest version.
- **Assert** that the data and API behavior remain correct after the upgrade.

```json
{
  "type": "test",
  "name": "movies",
  "initialVersion": "1.12.0", // An older version to start with
  "commands": [
    // Commands to populate the database
    {
      "upgrade": "latest" // Will build meilisearch locally and run it
    },
    // Commands to check the state of the database
  ]
}
```

This ensures backward compatibility: databases created with older Meilisearch versions should remain functional and consistent after an upgrade.

### Advanced usage

As time goes on, tests may grow more complex as they evolve alongside new features and schema changes.
A single test can chain together multiple upgrades, interleaving data population, API checks, and version transitions.

For example:

```json
{
  "type": "test",
  "name": "movies",
  "initialVersion": "1.12.0",
  "commands": [
    // Commands to populate the database
    {
      "upgrade": "1.17.0"
    },
    // Commands on endpoints that were removed after 1.17
    {
      "upgrade": "latest"
    },
    // Check the state
  ]
}
```

## Variables

Sometimes a command needs to use a value returned by a **previous response**.
These values can be captured and reused using the register field.

```json
{
  "route": "keys",
  "method": "POST",
  "body": {
    "inline": {
        "actions": [
        "search",
        "documents.add"
        ],
        "description": "Test API Key",
        "expiresAt": null,
        "indexes": [ "movies" ]
    }
  },
  "expectedResponse": {
      "key": "c6f64630bad2996b1f675007c8800168e14adf5d6a7bb1a400a6d2b158050eaf",
      // ...
  },
  "register": {
    "key": "/key"
  },
  "synchronous": "WaitForResponse"
}
```

The `register` field captures the value at the JSON path `/key` from the response.
Paths follow the **JavaScript Object Notation Pointer (RFC 6901)** format.
Registered variables are available for all subsequent commands.

Registered variables can be referenced by wrapping their name in double curly braces:

In the route/path:

```json
{
  "route": "tasks/{{ task_id }}",
  "method": "GET"
}
```

In the request body:

```json
{
  "route": "indexes/movies/documents",
  "method": "PATCH",
  "body": {
    "inline": {
      "id": "{{ document_id }}",
      "overview": "Shazam turns evil and the world is in danger.",
    }
  }
}
```

As an API-key:

```json
{
  "route": "indexes/movies/documents",
  "method": "POST",
  "body": { /* ... */ },
  "apiKeyVariable": "key" // The content of the key variable will be used as an API key
}
```
