# Run
FROM    uffizzi/ttyd:alpine

ENV     MEILI_HTTP_ADDR 0.0.0.0:7700
ENV     MEILI_SERVER_PROVIDER docker
ENV     MEILI_NO_ANALYTICS true

RUN     apk update --quiet \
        && apk add -q --no-cache libgcc tini curl

COPY    target/x86_64-unknown-linux-musl/release/meilisearch /bin/meilisearch
RUN     ln -s /bin/meilisearch /meilisearch

WORKDIR /meili_data

EXPOSE  7700/tcp

ENTRYPOINT ["tini", "--"]
CMD     ["ttyd", "/bin/zsh"]
