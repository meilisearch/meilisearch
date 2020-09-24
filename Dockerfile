# start from base
FROM archlinux:latest

RUN pacman -Syy gcc --noconfirm
# install system-wide deps for python and node
# copy our application code
ADD target/debug/meilisearch /opt/
ADD raft-config.toml /
RUN mkdir /usr/logs
WORKDIR /opt

# expose port
EXPOSE 7700
EXPOSE 8000
