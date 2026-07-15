FROM rust:1.89-bookworm AS builder
WORKDIR /workspace
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir -p src && printf 'fn main() {}\n' > src/main.rs && printf '\n' > src/lib.rs
RUN cargo build --release --locked
COPY src ./src
COPY config ./config
COPY schema ./schema
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /workspace/target/release/slatedb-benchmark /usr/local/bin/slatedb-benchmark
COPY config ./config
COPY schema ./schema
ENTRYPOINT ["slatedb-benchmark"]
