# Contributing

First, thank you for contributing to Meilisearch! The goal of this document is to provide everything you need to start contributing to Meilisearch.

Remember that there are many ways to contribute other than writing code: writing [tutorials or blog posts](https://github.com/meilisearch/awesome-meilisearch), improving [the documentation](https://github.com/meilisearch/documentation), submitting [bug reports](https://github.com/meilisearch/meilisearch/issues/new?assignees=&labels=&template=bug_report.md&title=) and [feature requests](https://github.com/meilisearch/product/discussions/categories/feedback-feature-proposal)...

The code in this repository is only concerned with managing multiple indexes, handling the update store, and exposing an HTTP API. Search and indexation are the domain of our core engine, [`milli`](https://github.com/meilisearch/milli), while tokenization is handled by [our `charabia` library](https://github.com/meilisearch/charabia/).

If Meilisearch does not offer optimized support for your language, please consider contributing to `charabia` by following the [CONTRIBUTING.md file](https://github.com/meilisearch/charabia/blob/main/CONTRIBUTING.md) and integrating your intended normalizer/segmenter.

## Table of Contents

- [Assumptions](#assumptions)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Git Guidelines](#git-guidelines)
- [Release Process (for internal team only)](#release-process-for-internal-team-only)

## Assumptions

1. **You're familiar with [GitHub](https://github.com) and the [Pull Requests](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests)(PR) workflow.**
2. **You've read the Meilisearch [documentation](https://docs.meilisearch.com).**
3. **You know about the [Meilisearch community](https://docs.meilisearch.com/learn/what_is_meilisearch/contact.html).
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

If you get a "Too many open files" error you might want to increase the open file limit using this command:

```bash
ulimit -Sn 3000
```

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
- The branch related to the PR must be **up-to-date with `main`** before merging. Fortunately, this project uses [Bors](https://github.com/bors-ng/bors-ng) to automatically enforce this requirement without the PR author having to rebase manually.

## Release Process (for internal team only)

Meilisearch tools follow the [Semantic Versioning Convention](https://semver.org/).

### Automation to rebase and Merge the PRs

This project integrates a bot that helps us manage pull requests merging.<br>
_[Read more about this](https://github.com/meilisearch/integration-guides/blob/main/resources/bors.md)._

### How to Publish a new Release

The full Meilisearch release process is described in [this guide](https://github.com/meilisearch/core-team/blob/main/resources/meilisearch-release.md). Please follow it carefully before doing any release.

### Release assets

For each release, the following assets are created:
- Binaries for differents platforms (Linux, MacOS, Windows and ARM architectures) are attached to the GitHub release
- Binaries are pushed to HomeBrew and APT (not published for RC)
- Docker tags are created/updated:
  - `vX.Y.Z`
  - `vX.Y` (not published for RC)
  - `latest` (not published for RC)

<hr>

Thank you again for reading this through, we can not wait to begin to work with you if you made your way through this contributing guide ❤️
