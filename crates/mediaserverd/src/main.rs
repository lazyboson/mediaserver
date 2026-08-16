#![forbid(unsafe_code)]

mod consumer_ws;
mod hub;
mod media_rt;
mod ng_transport;
#[allow(dead_code)]
mod supervisor;
mod tap_plane;
mod tap_session;
mod tap_spike;

use control_api::SessionController;
use media_core::AudioFormat;
use ng_transport::{NgTransport, NgTransportConfig};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tap_plane::{TapPlane, TapPlaneConfig};
use tracing::{error, info, warn};

const RTPENGINE_NODE_ENV: &str = "MSS_RTPENGINE_NODE";
const CONTROL_LISTEN_ENV: &str = "MSS_CONTROL_LISTEN";
const POD_NAME_ENV: &str = "MSS_POD_NAME";
const LOCAL_MEDIA_IP_ENV: &str = "MSS_TAP_LOCAL_IP";
const DEFAULT_POD_NAME: &str = "mediaserverd";

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

        match control_listen_address() {
            Some(Ok(listen)) => serve_control_plane(listen).await,
            Some(Err(configured)) => {
                error!(
                    configured,
                    env = CONTROL_LISTEN_ENV,
                    "the control plane listen address must be ip:port"
                );
            }
            None => {
                info!(
                    env = CONTROL_LISTEN_ENV,
                    "no control plane listen address configured; idling"
                );
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to listen for shutdown signal");
                info!("shutdown signal received");
            }
        }
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

fn control_listen_address() -> Option<Result<SocketAddr, String>> {
    let configured = std::env::var(CONTROL_LISTEN_ENV).ok()?;
    Some(configured.parse().map_err(|_| configured))
}

fn local_media_address() -> IpAddr {
    std::env::var(LOCAL_MEDIA_IP_ENV)
        .ok()
        .and_then(|configured| configured.parse().ok())
        .unwrap_or(IpAddr::from([0, 0, 0, 0]))
}

async fn serve_control_plane(listen: SocketAddr) {
    let plane = Arc::new(TapPlane::new(TapPlaneConfig {
        default_node: std::env::var(RTPENGINE_NODE_ENV)
            .ok()
            .and_then(|configured| configured.parse().ok()),
        local_media_address: local_media_address(),
        format: AudioFormat::pcmu_8k_20ms(),
        cookie_prefix: cookie_prefix(),
        sdp_session_id: cookie_prefix(),
    }));
    let owner = std::env::var(POD_NAME_ENV).unwrap_or_else(|_| DEFAULT_POD_NAME.to_string());
    let draining = Arc::clone(&plane);
    let controller = SessionController::new(owner.clone()).with_media_plane(plane);

    let listener = match tokio::net::TcpListener::bind(listen).await {
        Ok(listener) => listener,
        Err(error) => {
            error!(%listen, %error, "could not bind the control plane listener");
            return;
        }
    };
    info!(%listen, owner = %owner, "MediaControl is serving");

    let served = control_api::serve_on_until(controller, listener, async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for shutdown signal");
        info!(
            live_taps = draining.live_sessions(),
            "shutdown signal received; draining the control plane"
        );
    })
    .await;
    if let Err(error) = served {
        warn!(%error, "the control plane stopped with an error");
    }
}
