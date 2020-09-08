# start from base
FROM ubuntu:latest

RUN apt -yqq update
RUN apt -yqq install libc-bin libssl-dev
# install system-wide deps for python and node
# copy our application code
ADD target/debug/meilisearch /opt/
ADD raft-config.toml /
RUN mkdir /usr/logs
WORKDIR /opt

# expose port
EXPOSE 7700
EXPOSE 8000
