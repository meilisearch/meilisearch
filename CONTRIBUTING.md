# Contributing

First, thank you for contributing to Meilisearch! The goal of this document is to provide everything you need to start contributing to Milli, the search engine of Meilisearch.

Remember that there are many ways to contribute other than writing code: writing [tutorials or blog posts](https://github.com/meilisearch/awesome-meilisearch), improving [the documentation](https://github.com/meilisearch/documentation), submitting [bug reports](https://github.com/meilisearch/milli/issues/new) and [feature requests](https://github.com/meilisearch/product/discussions/categories/feedback-feature-proposal)...

## Table of Contents
- [Hacktoberfest](#hacktoberfest-2022)
- [Assumptions](#assumptions)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Git Guidelines](#git-guidelines)
- [Release Process (for internal team only)](#release-process-for-internal-team-only)

## Hacktoberfest 2022

It's [Hacktoberfest month](https://hacktoberfest.com)! 🥳

Thanks so much for participating with Meilisearch this year!

1. We will follow the quality standards set by the organizers of Hacktoberfest (see detail on their [website](https://hacktoberfest.com/participation/#spam)). Our reviewers will not consider any PR that doesn’t match that standard.
2. PRs reviews will take place from Monday to Thursday, during usual working hours, CEST time. If you submit outside of these hours, there’s no need to panic; we will get around to your contribution.
3. There will be no issue assignment as we don’t want people to ask to be assigned specific issues and never return, discouraging the volunteer contributors from opening a PR to fix this issue. We take the liberty to choose the PR that best fixes the issue, so we encourage you to get to it as soon as possible and do your best!

You can check out the longer, more complete guideline documentation [here](https://github.com/meilisearch/.github/blob/main/Hacktoberfest_2022_contributors_guidelines.md).

## Assumptions

1. **You're familiar with [GitHub](https://github.com) and the [Pull Requests](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests)(PR) workflow.**
2. **You've read the Meilisearch [documentation](https://docs.meilisearch.com).**
3. **You know about the [Meilisearch community](https://docs.meilisearch.com/learn/what_is_meilisearch/contact.html).
   Please use this for help.**

## How to Contribute

1. Ensure your change has an issue! Find an
   [existing issue](https://github.com/meilisearch/milli/issues/) or [open a new issue](https://github.com/meilisearch/milli/issues/new).
   * This is where you can get a feel if the change will be accepted or not.
2. Once approved, [fork the Milli repository](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) in your own GitHub account.
3. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository)
4. Review the [Development Workflow](#development-workflow) section that describes the steps to maintain the repository.
5. Make your changes on your branch.
6. [Submit the branch as a Pull Request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) pointing to the `main` branch of the Meilisearch repository. A maintainer should comment and/or review your Pull Request within a few days. Although depending on the circumstances, it may take longer.

## Development Workflow

### Test

```bash
cargo test
```

### Index your documents

It can index a massive amount of documents in not much time, I already achieved to index:
 - 115m songs (song and artist name) in \~48min and take 81GiB on disk.
 - 12m cities (name, timezone and country ID) in \~4min and take 6GiB on disk.

These metrics are done on a MacBook Pro with the M1 processor.

You can feed the engine with your CSV (comma-separated, yes) data like this:

```bash
printf "id,name,age\n1,hello,32\n2,kiki,24\n" | http POST 127.0.0.1:9700/documents content-type:text/csv
```

Don't forget to specify the `id` of the documents. Also, note that it supports JSON and JSON
streaming: you can send them to the engine by using the `content-type:application/json` and
`content-type:application/x-ndjson` headers respectively.

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
- The PR title should be accurate and descriptive of the changes. The title of the PR will be indeed automatically added to the next [release changelogs](https://github.com/meilisearch/milli/releases/).
- [Convert your PR as a draft](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/changing-the-stage-of-a-pull-request) if your changes are a work in progress: no one will review it until you pass your PR as ready for review.<br>
  The draft PRs are recommended when you want to show that you are working on something and make your work visible.
- The branch related to the PR must be **up-to-date with `main`** before merging. Fortunately, this project uses [Bors](https://github.com/bors-ng/bors-ng) to automatically enforce this requirement without the PR author having to rebase manually.

## Release Process (for internal team only)

Meilisearch tools follow the [Semantic Versioning Convention](https://semver.org/).

### Automation to rebase and Merge the PRs <!-- omit in toc -->

This project integrates a bot that helps us manage pull requests merging.<br>
_[Read more about this](https://github.com/meilisearch/integration-guides/blob/main/resources/bors.md)._

### Automated changelogs <!-- omit in toc -->

This project integrates a tool to create automated changelogs: the [release-drafter](https://github.com/release-drafter/release-drafter/).

### How to Publish the Release <!-- omit in toc -->

Make a PR modifying all the `Cargo.toml` files with the right version.

Once the changes are merged on `main`, you can publish the current draft release via the [GitHub interface](https://github.com/meilisearch/milli/releases): on this page, click on `Edit` (related to the draft release) > update the description if needed > when you are ready, click on `Publish release`.

<hr>

Thank you again for reading this through, we can not wait to begin to work with you if you made your way through this contributing guide ❤️
