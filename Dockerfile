# syntax=docker/dockerfile:1.6

FROM rust:1.85.0-slim-bullseye AS build
WORKDIR /app

ARG PROFILE=debug
ENV PROFILE=${PROFILE}

COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src
COPY ./worker_configs ./worker_configs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo run --bin create_sample_data -- --connection-name="default" --config-file="worker_configs/fs_worker_config.json" --path-prefix="./"

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    if [ "$PROFILE" = "release" ]; then \
        cargo build --bin main --release; \
    else \
        cargo build --bin main; \
    fi

RUN --mount=type=cache,target=/app/target \
    cp ./target/${PROFILE}/main /bin/server-node

FROM debian:bullseye-slim AS final
WORKDIR /app

COPY --from=build /app/sample_data ./sample_data
COPY --from=build /bin/server-node ./server-node
COPY ./worker_configs ./worker_configs
RUN chmod +x ./server-node

EXPOSE 7000

CMD ["./server-node", "--config-file=./worker_configs/fs_worker_config.json"]
