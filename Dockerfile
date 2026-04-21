# Compile
FROM    rust:1.89-alpine3.22 AS compiler

RUN     apk add -q --no-cache build-base openssl-dev

WORKDIR /

ARG     COMMIT_SHA
ARG     COMMIT_DATE
ARG     GIT_TAG
ARG     EXTRA_ARGS
ENV     VERGEN_GIT_SHA=${COMMIT_SHA} VERGEN_GIT_COMMIT_TIMESTAMP=${COMMIT_DATE} VERGEN_GIT_DESCRIBE=${GIT_TAG}
ENV     RUSTFLAGS="-C target-feature=-crt-static"

COPY    . .
RUN     set -eux; \
        apkArch="$(apk --print-arch)"; \
        cargo build --release -p meilisearch -p meilitool ${EXTRA_ARGS}

# Run
FROM    alpine:3.22
LABEL   org.opencontainers.image.source="https://github.com/meilisearch/meilisearch"

ENV     MEILI_HTTP_ADDR 0.0.0.0:7700
ENV     MEILI_SERVER_PROVIDER docker

RUN     apk add -q --no-cache libgcc tini curl

# add meilisearch and meilitool to the `/bin` so you can run it from anywhere
# and it's easy to find.
COPY    --from=compiler /target/release/meilisearch /bin/meilisearch
COPY    --from=compiler /target/release/meilitool /bin/meilitool
# To stay compatible with the older version of the container (pre v0.27.0) we're
# going to symlink the meilisearch binary in the path to `/meilisearch`
RUN     ln -s /bin/meilisearch /meilisearch

# This directory should hold all the data related to meilisearch so we're going
# to move our PWD in there.
# We don't want to put the meilisearch binary
WORKDIR /meili_data


EXPOSE  7700/tcp

ENTRYPOINT ["tini", "--"]
CMD     /bin/meilisearch
