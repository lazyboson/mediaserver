#![forbid(unsafe_code)]

mod media_rt;
#[allow(dead_code)]
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

    let media = media_rt::MediaWorld::spawn(media_rt::MediaConfig::default());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("mss-control")
        .build()
        .expect("failed to build control-plane runtime");

    runtime.block_on(async {
        info!("control plane up (session API / NG client wiring lands in milestone 2)");
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for shutdown signal");
        info!("shutdown signal received");
    });

    media.shutdown();
    info!("mediaserverd stopped");
}
