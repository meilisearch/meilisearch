# Compile
FROM    alpine:3.14 AS compiler

RUN adduser -D testuser

RUN mkdir -p /mnt/test_volume && \
    chown testuser /mnt/test_volume

USER testuser
