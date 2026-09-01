FROM lukemathwalker/cargo-chef:latest-rust-1.95-bookworm AS chef
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake make g++ \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY crates ./crates
RUN cargo build --release --locked -p mediaserverd

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/target/release/mediaserverd /usr/local/bin/mediaserverd
USER nonroot
ENTRYPOINT ["/usr/local/bin/mediaserverd"]
