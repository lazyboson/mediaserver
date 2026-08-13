#![forbid(unsafe_code)]

mod media_rt;
mod ng_transport;
#[allow(dead_code)]
mod supervisor;
mod tap_session;
mod tap_spike;

use ng_transport::{NgTransport, NgTransportConfig};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info};

const RTPENGINE_NODE_ENV: &str = "MSS_RTPENGINE_NODE";

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
        match tap_session::request_from_env(cookie_prefix()) {
            Ok(Some(request)) => {
                info!(
                    call_id = %request.call_id,
                    output = %request.output.display(),
                    seconds = request.duration.as_secs(),
                    "running the phase-0 tap spike instead of the daemon loop"
                );
                if let Err(error) = tap_session::run(request, cookie_prefix()).await {
                    error!(%error, "tap spike failed");
                }
                return;
            }
            Ok(None) => {}
            Err(error) => {
                error!(%error, "tap spike configuration rejected");
                return;
            }
        }

        probe_configured_rtpengine_node().await;
        info!("control plane up (session API and tap orchestration land in milestone 4)");
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for shutdown signal");
        info!("shutdown signal received");
    });

    media.shutdown();
    info!("mediaserverd stopped");
}

async fn probe_configured_rtpengine_node() {
    let Ok(configured) = std::env::var(RTPENGINE_NODE_ENV) else {
        info!(
            env = RTPENGINE_NODE_ENV,
            "no rtpengine node configured; skipping NG reachability probe"
        );
        return;
    };
    let node: SocketAddr = match configured.parse() {
        Ok(node) => node,
        Err(error) => {
            error!(
                configured,
                %error,
                env = RTPENGINE_NODE_ENV,
                "rtpengine node must be an ip:port address"
            );
            return;
        }
    };

    let any_local = SocketAddr::from(([0, 0, 0, 0], 0));
    let transport = match NgTransport::bind(
        any_local,
        node,
        NgTransportConfig::default(),
        cookie_prefix(),
    )
    .await
    {
        Ok(transport) => transport,
        Err(error) => {
            error!(%node, %error, "could not bind the NG control socket");
            return;
        }
    };

    let outcome = transport.ping().await;
    let health = transport.health();
    match outcome {
        Ok(_) => info!(
            %node,
            local = ?transport.local_addr().ok(),
            healthy = health.healthy,
            replies = health.replies,
            "rtpengine NG node answered ping"
        ),
        Err(error) => error!(
            %node,
            %error,
            healthy = health.healthy,
            timeouts = health.timeouts,
            consecutive_timeouts = health.consecutive_timeouts,
            "rtpengine NG node did not answer ping"
        ),
    }
}

fn cookie_prefix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since_epoch| since_epoch.as_nanos() as u64)
        .unwrap_or_default()
}
