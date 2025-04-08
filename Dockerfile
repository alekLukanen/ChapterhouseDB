
FROM rust:1.85.0-slim-bullseye AS build
WORKDIR /app

COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src

RUN cargo run --bin create_sample_data
RUN cargo build --bin main --profile docker-release
RUN cp ./target/docker-release/main /bin/server-node

FROM debian:bullseye-slim AS final

RUN mkdir /app
COPY --from=build /app/sample_data /app/sample_data
COPY --from=build /app/target/docker-release/main /app/server-node

EXPOSE 7000

CMD ["/app/server-node", "--log-level=info"]
