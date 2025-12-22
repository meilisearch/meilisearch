# Contributing

First, thank you for contributing to Meilisearch! The goal of this document is to provide everything you need to start contributing to Meilisearch.

Remember that there are many ways to contribute other than writing code: writing [tutorials or blog posts](https://github.com/meilisearch/awesome-meilisearch), improving [the documentation](https://github.com/meilisearch/documentation), submitting [bug reports](https://github.com/meilisearch/meilisearch/issues/new?assignees=&labels=&template=bug_report.md&title=) and [feature requests](https://github.com/meilisearch/product/discussions/categories/feedback-feature-proposal)...

Meilisearch can manage multiple indexes, handle the update store, and expose an HTTP API. Search and indexation are the domain of our core engine, [`milli`](https://github.com/meilisearch/meilisearch/tree/main/milli), while tokenization is handled by [our `charabia` library](https://github.com/meilisearch/charabia/).

If Meilisearch does not offer optimized support for your language, please consider contributing to `charabia` by following the [CONTRIBUTING.md file](https://github.com/meilisearch/charabia/blob/main/CONTRIBUTING.md) and integrating your intended normalizer/segmenter.

## Table of Contents

- [Assumptions](#assumptions)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Git Guidelines](#git-guidelines)
- [Release Process (for internal team only)](#release-process-for-internal-team-only)

## Assumptions

1. **You're familiar with [GitHub](https://github.com) and the [Pull Requests (PR)](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) workflow.**
2. **You've read the Meilisearch [documentation](https://www.meilisearch.com/docs).**
3. **You know about the [Meilisearch community on Discord](https://discord.meilisearch.com).
   Please use this for help.**

## How to Contribute

1. Ensure your change has an issue! Find an
   [existing issue](https://github.com/meilisearch/meilisearch/issues/) or [open a new issue](https://github.com/meilisearch/meilisearch/issues/new).
   * This is where you can get a feel if the change will be accepted or not.
2. Once approved, [fork the Meilisearch repository](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) in your own GitHub account.
3. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository)
4. Review the [Development Workflow](#development-workflow) section that describes the steps to maintain the repository.
5. Make your changes on your branch.
6. [Submit the branch as a Pull Request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) pointing to the `main` branch of the Meilisearch repository. A maintainer should comment and/or review your Pull Request within a few days. Although depending on the circumstances, it may take longer.

## Development Workflow

### Setup and run Meilisearch

```bash
cargo run --release
```

We recommend using the `--release` flag to test the full performance of Meilisearch.

### Test

```bash
cargo test
```

This command will be triggered to each PR as a requirement for merging it.

#### Faster build

You can set the `LINDERA_CACHE` environment variable to speed up your successive builds by up to 2 minutes.
It'll store some built artifacts in the directory of your choice.

We recommend using the `$HOME/.cache/meili/lindera` directory:
```sh
export LINDERA_CACHE=$HOME/.cache/meili/lindera
```

You can set the `MILLI_BENCH_DATASETS_PATH` environment variable to further speed up your builds.
It'll store some big files used for the benchmarks in the directory of your choice.

We recommend using the `$HOME/.cache/meili/benches` directory:
```sh
export MILLI_BENCH_DATASETS_PATH=$HOME/.cache/meili/benches
```

Furthermore, you can improve incremental compilation by setting the `MEILI_NO_VERGEN` environment variable.
Setting this variable will prevent the Meilisearch binary from being rebuilt each time the directory that hosts the Meilisearch repository changes.
Do not enable this environment variable for production builds (as it will break the `version` route, among other things).

#### Snapshot-based tests

We are using [insta](https://insta.rs) to perform snapshot-based testing.
We recommend using the insta tooling (such as `cargo-insta`) to update the snapshots if they change following a PR.

New tests should use insta where possible rather than manual `assert` statements.

Furthermore, we provide some macros on top of insta, notably a way to use snapshot hashes instead of inline snapshots, saving a lot of space in the repository.

To effectively debug snapshot-based hashes, we recommend you export the `MEILI_TEST_FULL_SNAPS` environment variable so that snapshot are fully created locally:

```sh
export MEILI_TEST_FULL_SNAPS=true # add this to your .bashrc, .zshrc, ...
```

#### Test troubleshooting

If you get a "Too many open files" error you might want to increase the open file limit using this command:

```bash
ulimit -Sn 3000
```

#### Build tools

Meilisearch follows the [cargo xtask](https://github.com/matklad/cargo-xtask) workflow to provide some build tools.

Run `cargo xtask --help` from the root of the repository to find out what is available.

#### Update the openAPI file if the API changed

To update the openAPI file in the code, see [sprint_issue.md](https://github.com/meilisearch/meilisearch/blob/main/.github/ISSUE_TEMPLATE/sprint_issue.md#reminders-when-modifying-the-api).

If you want to generate OpenAPI file manually:

With swagger:
- Starts Meilisearch with the `swagger` feature flag: `cargo run --features swagger`
- On a browser, open the following URL: http://localhost:7700/scalar
- Click the « Download openAPI file »

With the internal crate:
```bash
cd crates/openapi-generator
cargo run --release -- --pretty
```

### Logging

Meilisearch uses [`tracing`](https://lib.rs/crates/tracing) for logging purposes. Tracing logs are structured and can be displayed as JSON to the end user, so prefer passing arguments as fields rather than interpolating them in the message.

Refer to the [documentation](https://docs.rs/tracing/0.1.40/tracing/index.html#using-the-macros) for the syntax of the spans and events.

Logging spans are used for 3 distinct purposes:

1. Regular logging
2. Profiling
3. Benchmarking

As a result, the spans should follow some rules:

- They should not be put on functions that are called too often. That is because opening and closing a span causes some overhead. For regular logging, avoid putting spans on functions that are taking less than a few hundred nanoseconds. For profiling or benchmarking, avoid putting spans on functions that are taking less than a few microseconds.
- For profiling and benchmarking, use the `TRACE` level.
- For profiling and benchmarking, use the following `target` prefixes:
  - `indexing::` for spans meant when profiling the indexing operations.
  - `search::` for spans meant when profiling the search operations.

### Benchmarking

See [BENCHMARKS.md](./BENCHMARKS.md)

## Git Guidelines

### Git Branches

All changes must be made in a branch and submitted as PR.

We do not enforce any branch naming style, but please use something descriptive of your changes.

### Git Commits

As minimal requirements, your commit message should:
- be capitalized
- not finish by a dot or any other punctuation character (!,?)
- start with a verb so that we can read your commit message this way: "This commit will ...", where "..." is the commit message.
  e.g.: "Fix the home page button" or "Add more tests for create_index method"

We don't follow any other convention, but if you want to use one, we recommend [the Chris Beams one](https://chris.beams.io/posts/git-commit/).

### GitHub Pull Requests

Some notes on GitHub PRs:

- All PRs must be reviewed and approved by at least one maintainer.
- The PR title should be accurate and descriptive of the changes.
- [Convert your PR as a draft](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/changing-the-stage-of-a-pull-request) if your changes are a work in progress: no one will review it until you pass your PR as ready for review.<br>
  The draft PRs are recommended when you want to show that you are working on something and make your work visible.
- The branch related to the PR must be **up-to-date with `main`** before merging. Fortunately, this project uses [GitHub Merge Queues](https://github.blog/news-insights/product-news/github-merge-queue-is-generally-available/) to automatically enforce this requirement without the PR author having to rebase manually.

## Merging PRs

This project uses GitHub Merge Queues that helps us manage pull requests merging.

Before merging a PR, the maintainer should ensure the following requirements are met
- Automated tests have been added.
- If some tests cannot be automated, manual rigorous tests should be applied.
- ⚠️ If there is an change in the DB: it's mandatory to manually test the `--experimental-dumpless-upgrade` on a DB of the previous Meilisearch minor version (e.g. v1.13 for the v1.14 release).
- If necessary, the feature have been tested in the Cloud production environment (with [prototypes](./documentation/prototypes.md)) and the Cloud UI is ready.
- If necessary, the [documentation](https://github.com/meilisearch/documentation) related to the implemented feature in the PR is ready.
- If necessary, the [integrations](https://github.com/meilisearch/integration-guides) related to the implemented feature in the PR are ready.

## Publish Process (for internal team only)

Meilisearch tools follow the [Semantic Versioning Convention](https://semver.org/).

### How to publish a new release

The full Meilisearch release process is described in [this guide](./documentation/release.md).

### How to publish a prototype

Depending on the developed feature, you might need to provide a prototyped version of Meilisearch to make it easier to test by the users.

This happens in two steps:
- [Release the prototype](./documentation/prototypes.md#how-to-publish-a-prototype)
- [Communicate about it](./documentation/prototypes.md#communication)

### How to implement and publish an experimental feature

Here is our [guidelines and process](./documentation/experimental-features.md) to implement and publish an experimental feature.

### Release assets

For each release, the following assets are created:
- Binaries for different platforms (Linux, MacOS, Windows and ARM architectures) are attached to the GitHub release
- Binaries are pushed to HomeBrew and APT (not published for RC)
- Docker tags are created/updated:
  - `vX.Y.Z`
  - `vX.Y` (not published for RC)
  - `latest` (not published for RC)

<hr>

Thank you again for reading this through, we can not wait to begin to work with you if you made your way through this contributing guide ❤️
