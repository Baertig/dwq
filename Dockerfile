FROM ubuntu:jammy

RUN \
    echo 'Update the package index files to latest available versions' >&2 && \
    apt-get update && \
    apt-get -y --no-install-recommends install \
    git \
    python3 \
    python3-dev \
    python3-pip \
    && \
    echo 'Clean up installation files' >&2 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install rust if not available.
RUN if test -z "$(command -v cargo)"; then apt-get update && apt-get -y install build-essential curl ; fi
RUN if test -z "$(command -v cargo)"; then curl https://sh.rustup.rs -sSf | sh -s -- -y ; fi

# rust:buster ships 1.79, bump that
RUN if test "${DEBIAN_FROM}" = "rust:buster"; then rustup default 1.82 ; fi

# this works on both debian and ubuntu
ENV PATH="/root/.cargo/bin:${PATH}"

# build git-cache
RUN cargo install --git https://github.com/kaspar030/git-cache-rs
# get git-cache-rs binary
# COPY --from=ghcr.io/kaspar030/git-cache:0.1.5-jammy /git-cache /usr/bin/git-cache
# ENV GIT_CACHE_RS /usr/bin/git-cache

RUN mkdir ./dwq
COPY ./dwq ./dwq/dwq
COPY setup.py ./dwq/setup.py
COPY README.md ./dwq/README.md

# uninstall old version
# RUN pip uninstall -y dwq

# install my custom version
RUN cd dwq && python3 ./setup.py install

CMD ["dwqw", "--jobs" ,"1"]
