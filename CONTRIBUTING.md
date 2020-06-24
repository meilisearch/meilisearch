# Contributing

First, thank you for contributing to MeiliSearch! The goal of this document is to
provide everything you need to start contributing to MeiliSearch. The
following TOC is sorted progressively, starting with the basics and
expanding into more specifics.

<!-- MarkdownTOC autolink="true" style="ordered" indent="   " -->

1. [Assumptions](#assumptions)
1. [Your First Contribution](#your-first-contribution)
1. [Change Control](#change-control)
   1. [Git Branches](#git-branches)
   1. [Git Commits](#git-commits)
      1. [Style](#style)
   1. [Github Pull Requests](#github-pull-requests)
      1. [Reviews & Approvals](#reviews--approvals)
      1. [Merge Style](#merge-style)
   1. [CI](#ci)
1. [Development](#development)
   1. [Setup](#setup)
   1. [Testing](#testing)
   1. [Benchmarking](#benchmarking--profiling)
1. [Humans](#humans)
   1. [Documentation](#documentation)
   1. [Changelog](#changelog)

<!-- /MarkdownTOC -->

## Assumptions

1. **You're familiar with [Github](https://github.com) and the [pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests)
   workflow.**
2. **You've read the MeiliSearch [docs](https://docs.meilisearch.com).**
3. **You know about the [MeiliSearch community](https://docs.meilisearch.com/resources/contact.html).
   Please use this for help.**

## Your First Contribution

1. Ensure your change has an issue! Find an
   [existing issue](https://github.com/meilisearch/meilisearch/issues/) or [open a new issue](https://github.com/meilisearch/meilisearch/issues/new).
   * This is where you can get a feel if the change will be accepted or not.
2. Once approved, [fork the MeiliSearch repository](https://help.github.com/en/github/getting-started-with-github/fork-a-repo) in your own
   Github account.
3. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository)
4. Review the MeiliSearch [workflow](#workflow) and [development](#development).
5. Make your changes.
6. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the main MeiliSearch
   repo. A MeiliSearch team member should comment and/or review your pull request
   with a few days. Although, depending on the circumstances, it may take
   longer.

## Change Control

### Git Branches

_All_ changes must be made in a branch and submitted as [pull requests](#pull-requests).
MeiliSearch does not adopt any type of branch naming style, but please use something
descriptive of your changes.

### Git Commits

#### Style

Please ensure your commits are small and focused; they should tell a story of
your change. This helps reviewers to follow your changes, especially for more
complex changes.

Familiarise yourself with [How to Write a Git Commit Message](https://chris.beams.io/posts/git-commit/).

### Github Pull Requests

Once your changes are ready you must submit your branch as a pull request.

#### Reviews & Approvals

All pull requests must be reviewed and approved by at least one MeiliSearch team
member.

#### Merge Style

All pull requests are squashed and merged. We generally discourage large pull
requests that are over 300-500 lines of diff. If you would like to propose
a change that is larger we suggest coming onto our chat channel and
discuss it with one of our engineers. This way we can talk through the
solution and discuss if a change that large is even needed! This overall
will produce a quicker response to the change and likely produce code that
aligns better with our process.

## Development

### Setup

See the [MeiliSearch Docs](https://docs.meilisearch.com/guides/advanced_guides/installation.html) for how to set up a development environment.

### Benchmarking & Profiling

We do not yet do any benchmarking, nor have we formalised our profiling. If you'd like to work on this please get in touch!

## Humans

After making your change, you'll want to prepare it for MeiliSearch users (mostly humans). This usually entails updating documentation and announcing your feature.

### Documentation

Documentation is very important to MeiliSearch. All contributions that
alter user-facing behavior MUST include documentation changes. Please see
[GitHub.com/meilisearch/documentation](https://github.com/meilisearch/documentation) for more info.

### Changelog

Until we have guidelines in place, updating the [`Changelog`](/CHANGELOG.md) is solely the responsibility of MeiliSearch team members.
