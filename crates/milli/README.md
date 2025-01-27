<p align="center">
  <img alt="the milli logo" src="../../assets/milli-logo.svg">
</p>

<p align="center">a concurrent indexer combined with fast and relevant search algorithms</p>

## Introduction

This crate contains the internal engine used by [Meilisearch].

It contains a library that can manage one and only one index. Meilisearch
manages the multi-index itself. Milli is unable to store updates in a store:
it is the job of something else above and this is why it is only able
to process one update at a time.

[Meilisearch]: https://github.com/meilisearch/meilisearch
