# GitHub actions workflow for MeiliDB

> **Note:**

> - We do not use [cache](https://github.com/actions/cache) yet but we could use it to speed up CI

## Workflow

- On each pull request, we are triggering `cargo test`.
- On each commit on master, we are building the latest docker image.

## TODO
- On each tag, we should build the tagged docker image and the binaries for Debian & CentOS
