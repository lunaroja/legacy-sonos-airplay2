# This digest is the reviewed ARM64 Debian 13 base, not a floating tag.
FROM debian@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS base
RUN rm /etc/apt/sources.list.d/debian.sources && \
    printf '%s\n' \
      'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20261117T000000Z trixie main' \
      'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20261117T000000Z trixie-updates main' \
      'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20261117T000000Z trixie-security main' \
      > /etc/apt/sources.list && \
    printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d && chmod 755 /usr/sbin/policy-rc.d

FROM base AS toolchain
COPY build-packages.txt /build-packages.txt
RUN apt-get -o Acquire::Retries=1 -o Acquire::http::Timeout=30 update -qq && \
    DEBIAN_FRONTEND=noninteractive xargs -r apt-get install -y --no-install-recommends < /build-packages.txt && \
    mkdir /recipe-metadata && \
    dpkg-query -W -f='${binary:Package}\t${Version}\n' > /recipe-metadata/installed-packages.tsv && \
    cp /var/lib/apt/lists/*InRelease /recipe-metadata/ && \
    apt-cache show libconfig11 > /recipe-metadata/libconfig11-package.txt && \
    dpkg-query -L libconfig11 > /recipe-metadata/libconfig11-files.txt && \
    rm -rf /var/lib/apt/lists/*

FROM base AS runtime
COPY runtime-packages.txt /runtime-packages.txt
RUN apt-get -o Acquire::Retries=1 -o Acquire::http::Timeout=30 update -qq && \
    DEBIAN_FRONTEND=noninteractive xargs -r apt-get install -y --no-install-recommends < /runtime-packages.txt && \
    rm -rf /var/lib/apt/lists/*
