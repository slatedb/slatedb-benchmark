FROM rust:1.89-bookworm AS builder
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY runner/Cargo.toml runner/build.rs ./runner/
RUN mkdir -p runner/src && printf 'fn main() {}\n' > runner/src/main.rs && printf '\n' > runner/src/lib.rs
RUN cargo build --release --locked --manifest-path runner/Cargo.toml
COPY runner/src ./runner/src
COPY config ./config
COPY schema ./schema
RUN touch runner/src/main.rs runner/src/lib.rs && cargo build --release --locked --manifest-path runner/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /workspace/target/release/slatedb-benchmark /usr/local/bin/slatedb-benchmark
COPY config ./config
COPY schema ./schema
ENTRYPOINT ["slatedb-benchmark"]
