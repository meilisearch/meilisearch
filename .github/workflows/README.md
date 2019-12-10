# GitHub actions workflow for MeiliDB

> **Note:**

> - We do not use [cache](https://github.com/actions/cache) yet but we could use it to speed up CI

## Workflow

- On each pull request, we are triggering `cargo test`.
- On each commit on master, we are building the latest docker image.
- On each tag, we are building:
    - the tagged docker image
    - the binaries for MacOS, Ubuntu, and Windows
    - the debian package

## Problems

- We do not test on Windows because we are unable to make it work, there is a disk space problem.
