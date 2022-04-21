<p align="center">
  <img alt="the milli logo" src="http-ui/public/logo-black.svg">
</p>

<p align="center">a concurrent indexer combined with fast and relevant search algorithms</p>

## Introduction

This repository contains the core engine used in [Meilisearch].

It contains a library that can manage one and only one index. Meilisearch
manages the multi-index itself. Milli is unable to store updates in a store:
it is the job of something else above and this is why it is only able
to process one update at a time.

This repository contains crates to quickly debug the engine:
 - There are benchmarks located in the `benchmarks` crate.
 - The `http-ui` crate is a simple HTTP dashboard to tests the features like for real!
 - The `infos` crate is used to dump the internal data-structure and ensure correctness.
 - The `search` crate is a simple command-line that helps run [flamegraph] on top of it.
 - The `helpers` crate is only used to modify the database inplace, sometimes.

## How to use it?

_Section in WIP_

## Contributing

We're glad you're thinking about contributing to this repository! Feel free to pick an issue, and to ask any question you need. Some points might not be clear and we are available to help you!

Also, we recommend following the [CONTRIBUTING.md](/CONTRIBUTING.md) to create your PR.

[Meilisearch]: https://github.com/meilisearch/meilisearch
[flamegraph]: https://github.com/flamegraph-rs/flamegraph
