# Benchmarks

Currently this repository hosts two kinds of benchmarks:

1. The older "milli benchmarks", that use [criterion](https://github.com/bheisler/criterion.rs) and live in the "benchmarks" directory.
2. The newer "bench" that are workload-based and so split between the [`workloads`](./workloads/) directory and the [`xtask::bench`](./xtask/src/bench/) module.

This document describes the newer "bench" benchmarks. For more details on the "milli benchmarks", see [benchmarks/README.md](./benchmarks/README.md).

## Design philosophy for the benchmarks

The newer "bench" benchmarks are **integration** benchmarks, in the sense that they spawn an actual Meilisearch server and measure its performance end-to-end, including HTTP request overhead.

Since this is prone to fluctuating, the benchmarks regain a bit of precision by measuring the runtime of the individual spans using the [logging machinery](./CONTRIBUTING.md#logging) of Meilisearch.

A span roughly translates to a function call. The benchmark runner collects all the spans by name using the [logs route](https://github.com/orgs/meilisearch/discussions/721) and sums their runtime. The processed results are then sent to the [benchmark dashboard](https://bench.meilisearch.dev), which is in charge of storing and presenting the data.

## Running the benchmarks

Benchmarks can run locally or in CI.

### Locally

#### With a local benchmark dashboard

The benchmarks dashboard lives in its [own repository](https://github.com/meilisearch/benchboard). We provide binaries for Ubuntu/Debian, but you can build from source for other platforms (MacOS should work as it was developed under that platform).

Run the `benchboard` binary to create a fresh database of results. By default it will serve the results and the API to gather results on `http://localhost:9001`.

From the Meilisearch repository, you can then run benchmarks with:

```sh
cargo xtask bench -- workloads/my_workload_1.json ..
```

This command will build and run Meilisearch locally on port 7700, so make sure that this port is available.
To run benchmarks on a different commit, just use the usual git command to get back to the desired commit.

#### Without a local benchmark dashboard

To work with the raw results, you can also skip using a local benchmark dashboard.

Run:

```sh
cargo xtask bench --no-dashboard -- workloads/my_workload_1.json workloads/my_workload_2.json ..
```

For processing the results, look at [Looking at benchmark results/Without dashboard](#without-dashboard).

#### Sending a workload by hand

Sometimes you want to visualize the metrics of a worlkoad that comes from a custom report.
It is not quite easy to trick the benchboard in thinking that your report is legitimate but here are the commands you can run to upload your firefox report on a running benchboard.

```bash
# Name this hostname whatever you want
echo '{ "hostname": "the-best-place" }' | xh PUT 'http://127.0.0.1:9001/api/v1/machine'

# You'll receive an UUID from this command that we will call $invocation_uuid
echo '{ "commit": { "sha1": "1234567", "commit_date": "2024-09-05 12:00:12.0 +00:00:00", "message": "A cool message" }, "machine_hostname": "the-best-place", "max_workloads": 1 }' | xh PUT 'http://127.0.0.1:9001/api/v1/invocation'

# Just use UUID from the previous command
# and you'll receive another UUID that we will call $workload_uuid
echo '{ "invocation_uuid": "$invocation_uuid", "name": "toto", "max_runs": 1 }' | xh PUT 'http://127.0.0.1:9001/api/v1/workload'

# And now use your $workload_uuid and the content of your firefox report
# but don't forget to convert your firefox report from JSONLines into an object
echo '{ "workload_uuid": "$workload_uuid", "data": $REPORT_JSON_DATA }' | xh PUT 'http://127.0.0.1:9001/api/v1/run'
```

### In CI

We have dedicated runners to run workloads on CI. Currently, there are three ways of running the CI:

1. Automatically, on every push to `main`.
2. Manually, by clicking the [`Run workflow`](https://github.com/meilisearch/meilisearch/actions/workflows/bench-manual.yml) button and specifying the target reference (tag, commit or branch) as well as one or multiple workloads to run. The workloads must exist in the Meilisearch repository (conventionally, in the [`workloads`](./workloads/) directory) on the target reference. Globbing (e.g., `workloads/*.json`) works.
3. Manually on a PR, by posting a comment containing a `/bench` command, followed by one or multiple workloads to run. Globbing works. The workloads must exist in the Meilisearch repository in the branch of the PR.
  ```
  /bench workloads/movies*.json /hackernews_1M.json
  ```

## Looking at benchmark results

### On the dashboard

Results are available on the global dashboard used by CI at <https://bench.meilisearch.dev> or on your [local dashboard](#with-a-local-benchmark-dashboard).

The dashboard homepage presents three sections:

1. The latest invocations (a call to `cargo xtask bench`, either local or by CI) with their reason (generally set to some helpful link in CI) and their status.
2. The latest workloads ran on `main`.
3. The latest workloads ran on other references.

By default, the workload shows the total runtime delta with the latest applicable commit on `main`. The latest applicable commit is the latest commit for workload invocations that do not originate on `main`, and the latest previous commit for workload invocations that originate on `main`.

You can explicitly request a detailed comparison by span with the `main` branch, the branch or origin, or any previous commit, by clicking the links at the bottom of the workload invocation.

In the detailed comparison view, the spans are sorted by improvements, regressions, stable (no statistically significant change) and unstable (the span runtime is comparable to its standard deviation).

You can click on the name of any span to get a box plot comparing the target commit with multiple commits of the selected branch.

### Without dashboard

After the workloads are done running, the reports will live in the Meilisearch repository, in the `bench/reports` directory (by default).

You can then convert these reports into other formats.

- To [Firefox profiler](https://profiler.firefox.com) format. Run:
  ```sh
  cd bench/reports
  cargo run --release --bin trace-to-firefox -- my_workload_1-0-trace.json
  ```
  You can then upload the resulting `firefox-my_workload_1-0-trace.json` file to the online profiler.


## Designing benchmark workloads

Benchmark workloads conventionally live in the `workloads` directory of the Meilisearch repository.

They are JSON files with the following structure (comments are not actually supported, to make your own, remove them or copy some existing workload file):

```jsonc
{
  // Name of the workload. Must be unique to the workload, as it will be used to group results on the dashboard.
  "name": "hackernews.ndjson_1M,no-threads",
  // Number of consecutive runs of the commands that should be performed.
  // Each run uses a fresh instance of Meilisearch and a fresh database.
  // Each run produces its own report file.
  "run_count": 3,
  // List of arguments to add to the Meilisearch command line.
  "extra_cli_args": ["--max-indexing-threads=1"],
  // An expression that can be parsed as a comma-separated list of targets and levels
  // as described in [tracing_subscriber's documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/targets/struct.Targets.html#examples).
  // The expression is used to filter the spans that are measured for profiling purposes.
  // Optional, defaults to "indexing::=trace" (for indexing workloads), common other values is
  // "search::=trace"
  "target": "indexing::=trace",
  // List of named assets that can be used in the commands.
  "assets": {
    // name of the asset.
    // Must be unique at the workload level.
    // For better results, the same asset (same sha256) should have the same name accross workloads.
    // Having multiple assets with the same name and distinct hashes is supported accross workloads,
    // but will lead to superfluous downloads.
    //
    // Assets are stored in the `bench/assets/` directory by default.
    "hackernews-100_000.ndjson": {
      // If the assets exists in the local filesystem (Meilisearch repository or for your local workloads)
      // Its file path can be specified here.
      // `null` if the asset should be downloaded from a remote location.
      "local_location": null,
      // URL of the remote location where the asset can be downloaded.
      // Use the `--assets-key` of the runner to pass an API key in the `Authorization: Bearer` header of the download requests.
      // `null` if the asset should be imported from a local location.
      // if both local and remote locations are specified, then the local one is tried first, then the remote one
      // if the file is locally missing or its hash differs.
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-100_000.ndjson",
      // SHA256 of the asset.
      // Optional, the `sha256` of the asset will be displayed during a run of the workload if it is missing.
      // If present, the hash of the asset in the `bench/assets/` directory will be compared against this hash before
      // running the workload. If the hashes differ, the asset will be downloaded anew.
      "sha256": "60ecd23485d560edbd90d9ca31f0e6dba1455422f2a44e402600fbb5f7f1b213",
      // Optional, one of "Auto", "Json", "NdJson" or "Raw".
      // If missing, assumed to be "Auto".
      // If "Auto", the format will be determined from the extension in the asset name.
      "format": "NdJson"
    },
    "hackernews-200_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-200_000.ndjson",
      "sha256": "785b0271fdb47cba574fab617d5d332276b835c05dd86e4a95251cf7892a1685"
    },
    "hackernews-300_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-300_000.ndjson",
      "sha256": "de73c7154652eddfaf69cdc3b2f824d5c452f095f40a20a1c97bb1b5c4d80ab2"
    },
    "hackernews-400_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-400_000.ndjson",
      "sha256": "c1b00a24689110f366447e434c201c086d6f456d54ed1c4995894102794d8fe7"
    },
    "hackernews-500_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-500_000.ndjson",
      "sha256": "ae98f9dbef8193d750e3e2dbb6a91648941a1edca5f6e82c143e7996f4840083"
    },
    "hackernews-600_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-600_000.ndjson",
      "sha256": "b495fdc72c4a944801f786400f22076ab99186bee9699f67cbab2f21f5b74dbe"
    },
    "hackernews-700_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-700_000.ndjson",
      "sha256": "4b2c63974f3dabaa4954e3d4598b48324d03c522321ac05b0d583f36cb78a28b"
    },
    "hackernews-800_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-800_000.ndjson",
      "sha256": "cb7b6afe0e6caa1be111be256821bc63b0771b2a0e1fad95af7aaeeffd7ba546"
    },
    "hackernews-900_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-900_000.ndjson",
      "sha256": "e1154ddcd398f1c867758a93db5bcb21a07b9e55530c188a2917fdef332d3ba9"
    },
    "hackernews-1_000_000.ndjson": {
      "local_location": null,
      "remote_location": "https://milli-benchmarks.fra1.digitaloceanspaces.com/bench/datasets/hackernews/hackernews-1_000_000.ndjson",
      "sha256": "27e25efd0b68b159b8b21350d9af76938710cb29ce0393fa71b41c4f3c630ffe"
    }
  },
  // Core of the workload.
  // A list of commands to run sequentially.
  // Optional: A precommand is a request to the Meilisearch instance that is executed before the profiling runs.
  "precommands": [
    {
      // Meilisearch route to call. `http://localhost:7700/` will be prepended.
      "route": "indexes/movies/settings",
      // HTTP method to call.
      "method": "PATCH",
      // If applicable, body of the request.
      // Optional, if missing, the body will be empty.
      "body": {
        // One of "empty", "inline" or "asset".
        // If using "empty", you can skip the entire "body" key.
        "inline": {
          // when "inline" is used, the body is the JSON object that is the value of the `"inline"` key.
          "displayedAttributes": [
            "title",
            "by",
            "score",
            "time"
          ],
          "searchableAttributes": [
            "title"
          ],
          "filterableAttributes": [
            "by"
          ],
          "sortableAttributes": [
            "score",
            "time"
          ]
        }
      },
      // Whether to wait before running the next request.
      // One of:
      // - DontWait: run the next command without waiting the response to this one.
      // - WaitForResponse: run the next command as soon as the response from the server is received.
      // - WaitForTask: run the next command once **all** the Meilisearch tasks created up to now have finished processing.
      "synchronous": "WaitForTask"
    }
  ],
  // A command is a request to the Meilisearch instance that is executed while the profiling runs.
  "commands": [
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        // When using "asset", use the name of an asset as value to use the content of that asset as body.
        // the content type is derived of the format of the asset:
        // "NdJson" => "application/x-ndjson"
        // "Json" => "application/json"
        // "Raw" => "application/octet-stream"
        // See [AssetFormat::to_content_type](https://github.com/meilisearch/meilisearch/blob/7b670a4afadb132ac4a01b6403108700501a391d/xtask/src/bench/assets.rs#L30)
        // for details and up-to-date list.
        "asset": "hackernews-100_000.ndjson"
      },
      "synchronous": "WaitForTask"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-200_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-300_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-400_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-500_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-600_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-700_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-800_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-900_000.ndjson"
      },
      "synchronous": "WaitForResponse"
    },
    {
      "route": "indexes/movies/documents",
      "method": "POST",
      "body": {
        "asset": "hackernews-1_000_000.ndjson"
      },
      "synchronous": "WaitForTask"
    }
  ]
}
```

### Adding new assets

Assets reside in our DigitalOcean S3 space. Assuming you have team access to the DigitalOcean S3 space:

1. go to <https://cloud.digitalocean.com/spaces/milli-benchmarks?i=d1c552&path=bench%2Fdatasets%2F>
2. upload your dataset:
   1. if your dataset is a single file, upload that single file using the "upload" button,
   2. otherwise, create a folder using the "create folder" button, then inside that folder upload your individual files.

## Upgrading `https://bench.meilisearch.dev`

The URL of the server is in our password manager (look for "benchboard").

1. Make the needed modifications on the [benchboard repository](https://github.com/meilisearch/benchboard) and merge them to main.
2. Publish a new release to produce the Ubuntu/Debian binary.
3. Download the binary locally, send it to the server:
  ```
  scp -6 ~/Downloads/benchboard root@\[<ipv6-address>\]:/bench/new-benchboard
  ```
  Note that the ipv6 must be between escaped square brackets for SCP.
4. SSH to the server:
  ```
  ssh root@<ipv6-address>
  ```
  Note the ipv6 must **NOT** be between escaped square brackets for SSH 🥲
5. On the server, set the correct permissions for the new binary:
   ```
   chown bench:bench /bench/new-benchboard
   chmod 700 /bench/new-benchboard
   ```
6. On the server, move the new binary to the location of the running binary (if unsure, start by making a backup of the running binary):
  ```
  mv /bench/{new-,}benchboard
  ```
7. Restart the benchboard service.
  ```
  systemctl restart benchboard
  ```
8. Check that the service runs correctly.
  ```
  systemctl status benchboard
  ```
9. Check the availability of the service by going to <https://bench.meilisearch.dev> on your browser.
