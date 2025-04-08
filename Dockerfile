
FROM rust:1.85.0-slim-bullseye AS build
WORKDIR /app

COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./src ./src

RUN cargo build --bin main --profile docker-release
RUN cp ./target/docker-release/main /bin/server-node

FROM debian:bullseye-slim AS final

COPY --from=build /app/target/docker-release/main /bin/server-node

EXPOSE 7000

CMD ["/bin/server-node", "--log-level=info"]
