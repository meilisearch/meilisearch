# Compile
FROM    alpine:3.14 AS compiler

RUN     apk update --quiet
RUN     apk add curl
RUN     apk add build-base

RUN     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

WORKDIR /meilisearch

COPY    Cargo.lock .
COPY    Cargo.toml .

COPY    meilisearch-error/Cargo.toml meilisearch-error/
COPY    meilisearch-http/Cargo.toml meilisearch-http/

ENV     RUSTFLAGS="-C target-feature=-crt-static"

# Create dummy main.rs files for each workspace member to be able to compile all the dependencies
RUN     find . -type d -name "meilisearch-*" | xargs -I{} sh -c 'mkdir {}/src; echo "fn main() { }" > {}/src/main.rs;'
# Use `cargo build` instead of `cargo vendor` because we need to not only download but compile dependencies too
RUN     $HOME/.cargo/bin/cargo build --release
# Cleanup dummy main.rs files
RUN     find . -path "*/src/main.rs" -delete

ARG     COMMIT_SHA
ARG     COMMIT_DATE
ENV     COMMIT_SHA=${COMMIT_SHA} COMMIT_DATE=${COMMIT_DATE}

COPY    . .
RUN     $HOME/.cargo/bin/cargo build --release

# Run
FROM    alpine:3.14

RUN     apk add -q --no-cache libgcc tini

COPY    --from=compiler /meilisearch/target/release/meilisearch .

ENV     MEILI_HTTP_ADDR 0.0.0.0:7700
EXPOSE  7700/tcp

ENTRYPOINT ["tini", "--"]
CMD     ./meilisearch
