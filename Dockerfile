# Compile
FROM    alpine:3.14 AS compiler

RUN     apk update --quiet \
        && apk add -q --no-cache libgcc tini curl

RUN adduser -D testuser

RUN mkdir /mnt/test_volume && \
    chown testuser /mnt/test_volume

USER testuser

ENTRYPOINT ["tini", "--"]
CMD     ["${HOME}/meilisearch"]
