FROM rust:1.95-slim-bookworm AS builder
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake make g++ \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY crates ./crates
RUN cargo build --release --locked -p mediaserverd

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/target/release/mediaserverd /usr/local/bin/mediaserverd
USER nonroot
ENTRYPOINT ["/usr/local/bin/mediaserverd"]
