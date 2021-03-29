# Compile
FROM    alpine:3.10 AS compiler

RUN     apk update --quiet
RUN     apk add curl
RUN     apk add build-base

RUN     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

WORKDIR /meilisearch

COPY    Cargo.lock .
COPY    Cargo.toml .

COPY    meilisearch-core/Cargo.toml meilisearch-core/
COPY    meilisearch-error/Cargo.toml meilisearch-error/
COPY    meilisearch-http/Cargo.toml meilisearch-http/
COPY    meilisearch-schema/Cargo.toml meilisearch-schema/
COPY    meilisearch-tokenizer/Cargo.toml meilisearch-tokenizer/
COPY    meilisearch-types/Cargo.toml meilisearch-types/

ENV     RUSTFLAGS="-C target-feature=-crt-static"

RUN     find . -type d | xargs -I{} sh -c 'mkdir {}/src; echo "fn main() { }" > {}/src/main.rs;'
RUN     $HOME/.cargo/bin/cargo build --release
RUN     find . -path "*/src/main.rs" -delete

COPY    . .
RUN     $HOME/.cargo/bin/cargo build --release

# Run
FROM    alpine:3.10

RUN     apk add -q --no-cache libgcc tini

COPY    --from=compiler /meilisearch/target/release/meilisearch .

ENV     MEILI_HTTP_ADDR 0.0.0.0:7700
EXPOSE  7700/tcp

ENTRYPOINT ["tini", "--"]
CMD     ./meilisearch
