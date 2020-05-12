# GitHub Actions Workflow for MeiliSearch

> **Note:**

> - We do not use [cache](https://github.com/actions/cache) yet but we could use it to speed up CI

## Workflow

- On each pull request, we trigger `cargo test`.
- On each tag, we build:
    - the tagged Docker image and publish it to Docker Hub
    - the binaries for MacOS, Ubuntu, and Windows
    - the Debian package
- On each stable release (`v*.*.*` tag):
    - we build the `latest` Docker image and publish it to Docker Hub
    - we publish the binary to Hombrew and Gemfury

## Problems

- We do not test on Windows because we are unable to make it work, there is a disk space problem.
