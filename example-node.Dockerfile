FROM rust:1.85.0-slim-bullseye AS build
WORKDIR /app

COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src

RUN cargo run --bin create_sample_data
RUN cargo build --bin main --profile docker-release
RUN cp ./target/docker-release/main /bin/server-node

FROM debian:bullseye-slim AS final
WORKDIR /app

COPY --from=build /app/sample_data ./sample_data
COPY --from=build /app/target/docker-release/main ./server-node
RUN chmod +x ./server-node

EXPOSE 7000

CMD ["./server-node", "--log-level=debug"]
# CMD ["ls", "app"]
