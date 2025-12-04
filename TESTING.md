# Declarative tests

Declarative tests ensure that Meilisearch features remain stable across versions.

While we already have unit tests, those are run against **temporary databases** that are created fresh each time and therefore never risk corruption.

Declarative tests instead **simulate the lifetime of a database**: they chain together commands and requests to change the binary, verifying that database state and API responses remain consistent.

## Basic example

```jsonc
{
  "type": "test",
  "name": "api-keys",
  "binary": { // the first command will run on the binary following this specification.
    "source": "release", // get the binary as a release from GitHub
    "version": "1.19.0", // version to fetch
    "edition": "community" // edition to fetch
  },
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

```jsonc

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

```jsonc
{
  "type": "test",
  "name": "movies",
  "binary": {
    "source": "release",
    "version": "1.19.0",
    "edition": "community"
  },
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

For asset handling, please refer to the [declarative benchmarks documentation](/BENCHMARKS.md#adding-new-assets).

### Asserting responses

Commands can specify both the **expected status code** and the **expected response body**.

```jsonc
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

## Changing binary

It is possible to insert an instruction to change the current Meilisearch instance from one binary specification to another during a test.

When executed, such an instruction will:
1. Stop the current Meilisearch instance.
2. Fetch the binary specified by the instruction.
3. Restart the server with the specified binary on the same database.

```jsonc
{
  "type": "test",
  "name": "movies",
  "binary": {
    "source": "release",
    "version": "1.19.0", // start with version v1.19.0
    "edition": "community"
  },
  "assets": {
    "movies.json": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/movies.json",
      "sha256": "5b6e4cb660bc20327776e8a33ea197b43d9ec84856710ead1cc87ab24df77de1"
    }
  },
  "commands": [
    // setup some data
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "movies.json"
      }
    },
    // switch binary to v1.24.0
    {
      "binary": {
        "source": "release",
        "version": "1.24.0",
        "edition": "community"
      }
    }
  ]
}
```

### Typical Usage

In most cases, the change binary instruction will be used to update a database.

- **Set up** some data using commands on an older version.
- **Upgrade** to the latest version.
- **Assert** that the data and API behavior remain correct after the upgrade.

To properly test the dumpless upgrade, one should typically:

1. Open the database without processing the update task: Use a `binary` instruction to switch to the desired version, passing `--experimental-dumpless-upgrade` and `--experimental-max-number-of-batched-tasks=0` as extra CLI arguments
2. Check that the search, stats and task queue still work.
3. Open the database and process the update task: Use a `binary` instruction to switch to the desired version, passing `--experimental-dumpless-upgrade` as the extra CLI argument. Use a `health` command to wait for the upgrade task to finish.
4. Check that the indexing, search, stats, and task queue still work.

```jsonc
{
  "type": "test",
  "name": "movies",
  "binary": {
    "source": "release",
    "version": "1.12.0",
    "edition": "community"
  },
  "commands": [
    // 0. Run commands to populate the database
    {
      // ..
    },
    // 1. Open the database with new MS without processing the update task
    {
      "binary": {
        "source": "build", // build the binary from the sources in the current git repository
        "edition": "community",
        "extraCliArgs": [
          "--experimental-dumpless-upgrade", // allows to open with a newer MS
          "--experimental-max-number-of-batched-tasks=0" // prevent processing of the update task
        ]
      }
    },
    // 2. Check the search etc.
    {
      // ..
    },
    // 3. Open the database with new MS and processing the update task
    {
      "binary": {
        "source": "build", // build the binary from the sources in the current git repository
        "edition": "community",
        "extraCliArgs": [
          "--experimental-dumpless-upgrade" // allows to open with a newer MS
          // no `--experimental-max-number-of-batched-tasks=0`
        ]
      }
    },
    // 4. Check the indexing, search, etc.
    {
      // ..
    }
  ]
}
```

This ensures backward compatibility: databases created with older Meilisearch versions should remain functional and consistent after an upgrade.

## Variables

Sometimes a command needs to use a value returned by a **previous response**.
These values can be captured and reused using the register field.

```jsonc
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

```jsonc
{
  "route": "tasks/{{ task_id }}",
  "method": "GET"
}
```

In the request body:

```jsonc
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

Or they can be referenced by their name (**without curly braces**) as an API key:

```jsonc
{
  "route": "indexes/movies/documents",
  "method": "POST",
  "body": { /* ... */ },
  "apiKeyVariable": "key" // The **content** of the key variable will be used as an API key
}
```
