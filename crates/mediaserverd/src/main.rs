//! mediaserverd — the Media Streaming Service daemon.
//!
//! Two-world architecture (docs/architecture.md §7):
//!
//! - **Control world (Tokio):** gRPC/REST session API, RTPEngine NG
//!   client, Redis session registry, Kafka events, consumer WebSocket and
//!   gRPC I/O. Latency-tolerant; async is the right tool.
//! - **Media world (dedicated OS threads):** RTP receive, jitter buffer,
//!   decode/resample, fan-out, paced playout. Wall-clock-anchored loops,
//!   no async, no blocking syscalls except the sockets they own, no heap
//!   allocation per packet.
//!
//! The worlds talk over bounded lock-free queues. The media world never
//! waits on the control world.

mod media_rt;
#[allow(dead_code)] // wired into the worker loop in milestone M3
mod supervisor;

use tracing::info;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    info!(version = env!("CARGO_PKG_VERSION"), "mediaserverd starting");

    // Media world: real-time workers, one per core by default, spawned
    // before the async runtime so their threads exist independently of it.
    let media = media_rt::MediaWorld::spawn(media_rt::MediaConfig::default());

    // Control world: Tokio runtime for everything latency-tolerant.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("mss-control")
        .build()
        .expect("failed to build control-plane runtime");

    runtime.block_on(async {
        info!("control plane up (session API / NG client wiring lands in milestone 2)");
        // TODO(milestone 2): gRPC MediaControl service (tonic), NG client
        // task, Redis session registry, Kafka producer, health endpoint.
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for shutdown signal");
        info!("shutdown signal received");
    });

    media.shutdown();
    info!("mediaserverd stopped");
}
