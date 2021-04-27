# Transplant

Transplant makes communication between the users and [Milli](https://github.com/meilisearch/milli) using HTTP. The final purpose of Transplant is to be merged into the current [MeiliSearch repository](https://github.com/meilisearch/MeiliSearch) so that users will enjoy the new search engine performance provided by Milli.

## Run the alpha releases

Currently only alpha versions are available.

You can:

- Run it with Docker, for instance:

```bash
docker run -p 7700:7700 getmeili/meilisearch:v0.21.0-alpha.4 ./meilisearch
```

- With the available [release assets](https://github.com/meilisearch/transplant/releases).

- Compile from the source code:

```bash
cargo run --release
```

## Run the tests

```
cargo test
```

If you encounter any `Too many open files` error when running the tests, please upgrade the maximum number of open file descriptors with this command:

```
ulimit -Sn 3000
```
