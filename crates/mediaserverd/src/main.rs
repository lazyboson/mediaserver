#![forbid(unsafe_code)]

mod conference;
mod consumer_ws;
mod drain;
mod event_pump;
mod hub;
mod inline_leg;
mod media_ports;
mod media_rt;
mod metrics;
mod ng_transport;
mod recorder;
mod recording_spill;
mod registry_keeper;
mod rtpengine_capability;
mod session_store;
mod supervisor;
mod tap_plane;
mod tap_session;
mod tap_spike;

use control_api::proto;
use control_api::proto::media_control_server::MediaControl;
use control_api::tonic::Request;
use control_api::{AuthPolicy, ObservationSink, SessionController};
use drain::{DrainSteps, SessionsClosed};
use media_core::AudioFormat;
use ng_transport::{NgTransport, NgTransportConfig};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tap_plane::{TapPlane, TapPlaneConfig};
use tracing::{error, info, warn};

const RTPENGINE_NODE_ENV: &str = "MSS_RTPENGINE_NODE";
const CONTROL_LISTEN_ENV: &str = "MSS_CONTROL_LISTEN";
const KAFKA_BROKERS_ENV: &str = "MSS_KAFKA_BROKERS";
const EVENTS_TOPIC_ENV: &str = "MSS_EVENTS_TOPIC";
const EVENTS_PARTITIONS_ENV: &str = "MSS_EVENTS_PARTITIONS";
const REDIS_URL_ENV: &str = "MSS_REDIS_URL";
const POD_NAME_ENV: &str = "MSS_POD_NAME";
const LOCAL_MEDIA_IP_ENV: &str = "MSS_TAP_LOCAL_IP";
const METRICS_LISTEN_ENV: &str = "MSS_METRICS_LISTEN";
const TAP_TRANSCODE_ENV: &str = "MSS_TAP_TRANSCODE";
const OPUS_DECODE_RATE_ENV: &str = "MSS_OPUS_DECODE_RATE_HZ";
const TAP_FORMAT_ENV: &str = "MSS_TAP_FORMAT";
const OPUS_DECODE_RATE_DEFAULT_HZ: u32 = 16000;
const AUTH_TOKEN_ENV: &str = "MSS_AUTH_TOKEN";
const RECORDING_BUCKET_ENV: &str = "MSS_RECORDING_BUCKET";
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

        let capabilities = Arc::new(rtpengine_capability::NodeCapabilityLog::new(
            transcode_at_tap(),
        ));
        probe_configured_rtpengine_node(&capabilities).await;

        match control_listen_address() {
            Some(Ok(listen)) => serve_control_plane(listen, capabilities).await,
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
                let signal = drain::next_shutdown_signal().await;
                info!(signal = signal.name(), "shutdown signal received");
            }
        }
    });

    media.shutdown();
    info!("mediaserverd stopped");
}

async fn probe_configured_rtpengine_node(capabilities: &rtpengine_capability::NodeCapabilityLog) {
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
        Ok(_) => {
            info!(
                %node,
                local = ?transport.local_addr().ok(),
                healthy = health.healthy,
                replies = health.replies,
                "rtpengine NG node answered ping"
            );
            capabilities.report_first_contact(node, &transport).await;
        }
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

fn tap_format(opus_decode_rate_hz: u32) -> AudioFormat {
    let configured = std::env::var(TAP_FORMAT_ENV).unwrap_or_else(|_| "pcmu".to_string());
    let format = match configured.trim().to_ascii_lowercase().as_str() {
        "pcma" | "alaw" => Some(AudioFormat {
            encoding: media_core::Encoding::Pcma,
            ..AudioFormat::pcmu_8k_20ms()
        }),
        "opus" => Some(AudioFormat {
            encoding: media_core::Encoding::Opus,
            sample_rate_hz: opus_decode_rate_hz,
            channels: 1,
            ptime_ms: 20,
        }),
        "pcmu" | "ulaw" => Some(AudioFormat::pcmu_8k_20ms()),
        _ => None,
    };
    match format {
        Some(format) => {
            info!(
                env = TAP_FORMAT_ENV,
                encoding = ?format.encoding,
                sample_rate_hz = format.sample_rate_hz,
                "the tap decodes this format"
            );
            format
        }
        None => {
            warn!(
                env = TAP_FORMAT_ENV,
                configured = %configured,
                "a tap decodes pcmu, pcma or opus; falling back to pcmu"
            );
            AudioFormat::pcmu_8k_20ms()
        }
    }
}

fn opus_decode_rate_hz() -> u32 {
    let Ok(configured) = std::env::var(OPUS_DECODE_RATE_ENV) else {
        return OPUS_DECODE_RATE_DEFAULT_HZ;
    };
    match configured.trim().parse::<u32>() {
        Ok(rate) if media_core::opus::is_decodable_rate(rate) => {
            info!(
                env = OPUS_DECODE_RATE_ENV,
                rate_hz = rate,
                "decoding opus taps at this rate"
            );
            rate
        }
        _ => {
            warn!(
                env = OPUS_DECODE_RATE_ENV,
                configured = %configured,
                fallback_hz = OPUS_DECODE_RATE_DEFAULT_HZ,
                "libopus decodes 8000, 12000, 16000, 24000 or 48000 only"
            );
            OPUS_DECODE_RATE_DEFAULT_HZ
        }
    }
}

fn transcode_at_tap() -> bool {
    let configured = std::env::var(TAP_TRANSCODE_ENV).unwrap_or_else(|_| "on".to_string());
    let transcoding = !matches!(
        configured.trim().to_ascii_lowercase().as_str(),
        "off" | "false" | "0" | "no"
    );
    if transcoding {
        info!(
            env = TAP_TRANSCODE_ENV,
            "asking rtpengine to transcode the tap to our format"
        );
    } else {
        info!(
            env = TAP_TRANSCODE_ENV,
            "accepting the call's own codec on the tap; rtpengine transcodes nothing, \
             so a call whose codec this pipeline cannot decode will be refused"
        );
    }
    transcoding
}

fn metrics_listen_address() -> Option<Result<SocketAddr, String>> {
    let configured = std::env::var(METRICS_LISTEN_ENV).ok()?;
    Some(configured.parse().map_err(|_| configured))
}

fn auth_policy_from_env() -> AuthPolicy {
    match std::env::var(AUTH_TOKEN_ENV) {
        Ok(token) if !token.is_empty() => {
            info!("control plane authentication is on");
            AuthPolicy::shared_secret(token)
        }
        _ => {
            warn!(
                env = AUTH_TOKEN_ENV,
                "no auth token configured; the control plane accepts unauthenticated callers"
            );
            AuthPolicy::open()
        }
    }
}

fn local_media_address() -> IpAddr {
    let address = std::env::var(LOCAL_MEDIA_IP_ENV)
        .ok()
        .and_then(|configured| configured.parse().ok())
        .unwrap_or(IpAddr::from([0, 0, 0, 0]));
    info!(
        env = LOCAL_MEDIA_IP_ENV,
        address = %address,
        "media and ng sockets bind to this address"
    );
    address
}

async fn serve_control_plane(
    listen: SocketAddr,
    capabilities: Arc<rtpengine_capability::NodeCapabilityLog>,
) {
    let owner = std::env::var(POD_NAME_ENV).unwrap_or_else(|_| DEFAULT_POD_NAME.to_string());
    let drain_state = drain::DrainState::shared();
    let drain_budget = drain::drain_timeout();
    let recording = match recorder::RecordingSupport::from_env(&owner) {
        Ok(recording) => recording,
        Err(error) => {
            error!(%error, "the configured recording storage is unusable; refusing to start");
            return;
        }
    };
    if recording.sink.is_none() {
        info!(
            env = RECORDING_BUCKET_ENV,
            "no recording storage configured; file-s3 attachments will be refused"
        );
    }
    let salvaged = recording_spill::salvage(&recording).await;
    if salvaged != recording_spill::SalvageSummary::default() {
        info!(
            uploaded = salvaged.uploaded,
            already_present = salvaged.already_present,
            failed = salvaged.failed,
            "recordings spilled by an earlier life of this pod were reconciled with storage"
        );
    }
    let opus_rate = opus_decode_rate_hz();
    let local_media = local_media_address();
    let media_ports = media_ports::MediaPortAllocator::from_env();
    let plane = Arc::new(TapPlane::new(TapPlaneConfig {
        default_node: std::env::var(RTPENGINE_NODE_ENV)
            .ok()
            .and_then(|configured| configured.parse().ok()),
        local_media_address: local_media,
        advertised_media_address: media_ports::advertise_address(local_media),
        media_ports: Arc::clone(&media_ports),
        format: tap_format(opus_rate),
        transcode_at_tap: capabilities.transcode_at_tap(),
        opus_decode_rate_hz: opus_rate,
        cookie_prefix: cookie_prefix(),
        sdp_session_id: cookie_prefix(),
        recording,
        capabilities,
    }));
    let draining = Arc::clone(&plane);
    let observing = Arc::clone(&plane);
    let tap_metrics = plane.metrics();
    let mut controller = SessionController::new(owner.clone()).with_media_plane(plane);

    let mut pump_worker = None;
    match event_sink_from_env().await {
        Ok(Some((sink, worker, counters))) => {
            controller = controller.with_event_sink(sink);
            pump_worker = Some((worker, counters));
        }
        Ok(None) => {
            info!(
                env = KAFKA_BROKERS_ENV,
                "no event bus configured; events stay in-process"
            );
        }
        Err(error) => {
            error!(%error, "the configured event bus is unreachable; refusing to start");
            return;
        }
    }

    let listener = match tokio::net::TcpListener::bind(listen).await {
        Ok(listener) => listener,
        Err(error) => {
            error!(%listen, %error, "could not bind the control plane listener");
            return;
        }
    };
    info!(%listen, owner = %owner, "MediaControl is serving");

    let controller = Arc::new(controller);
    observing.observe_through(Arc::downgrade(&controller) as std::sync::Weak<dyn ObservationSink>);
    let mut keeper_counters = None;
    let mut keeper_handles = None;
    match session_store_from_env().await {
        Ok(Some(store)) => {
            let keeper = Arc::new(
                registry_keeper::RegistryKeeper::new(Arc::clone(&controller), store, owner.clone())
                    .with_subscriptions(
                        Arc::clone(&observing) as Arc<dyn registry_keeper::TapSubscriptions>
                    ),
            );
            keeper_counters = Some(keeper.counters());
            let renewing = tokio::spawn(Arc::clone(&keeper).run());
            keeper_handles = Some(KeeperHandles { keeper, renewing });
        }
        Ok(None) => info!(
            env = REDIS_URL_ENV,
            "no session registry configured; sessions live and die with this pod"
        ),
        Err(error) => {
            error!(%error, "the configured session registry is unreachable; refusing to start");
            return;
        }
    }

    match metrics_listen_address() {
        Some(Ok(metrics_listen)) => match tokio::net::TcpListener::bind(metrics_listen).await {
            Ok(metrics_listener) => {
                let sources = metrics::MetricsSources {
                    tap: tap_metrics,
                    controller: Arc::clone(&controller),
                    pump: pump_worker
                        .as_ref()
                        .map(|(_, counters)| Arc::clone(counters)),
                    keeper: keeper_counters.clone(),
                    drain: Arc::clone(&drain_state),
                    ports: Arc::clone(&media_ports),
                };
                tokio::spawn(metrics::serve(metrics_listener, sources));
            }
            Err(error) => {
                error!(%metrics_listen, %error, "could not bind the metrics listener; refusing to start");
                return;
            }
        },
        Some(Err(configured)) => {
            error!(
                configured,
                env = METRICS_LISTEN_ENV,
                "the metrics listen address must be ip:port; refusing to start"
            );
            return;
        }
        None => info!(
            env = METRICS_LISTEN_ENV,
            "no metrics listen address configured; counters stay in logs"
        ),
    }

    let auth = auth_policy_from_env();
    let mut until_draining = controller.drain_watch();
    let serving = tokio::spawn(control_api::serve_authenticated_until(
        Arc::clone(&controller),
        auth,
        listener,
        async move {
            let _ = until_draining.wait_for(|draining| *draining).await;
        },
    ));

    let signal = drain::next_shutdown_signal().await;
    info!(
        signal = signal.name(),
        live_taps = draining.live_sessions(),
        drain_budget_secs = drain_budget.as_secs(),
        "shutdown signal received; draining the control plane"
    );
    drain::exit_on_second_signal();

    let steps = ControlPlaneDrain {
        drain: Arc::clone(&drain_state),
        controller: Arc::clone(&controller),
        plane: Arc::clone(&draining),
        keeper: keeper_handles,
        serving: Mutex::new(Some(serving)),
        events: pump_worker
            .as_ref()
            .map(|(_, counters)| Arc::clone(counters)),
    };
    drain::run_drain(&steps, drain_budget).await;

    if let Some(counters) = keeper_counters {
        info!(
            persisted = counters
                .persisted
                .load(std::sync::atomic::Ordering::Relaxed),
            adopted = counters.adopted.load(std::sync::atomic::Ordering::Relaxed),
            released = counters.released.load(std::sync::atomic::Ordering::Relaxed),
            handed_off = counters
                .handed_off
                .load(std::sync::atomic::Ordering::Relaxed),
            lost = counters.lost.load(std::sync::atomic::Ordering::Relaxed),
            failed = counters.failed.load(std::sync::atomic::Ordering::Relaxed),
            "session registry totals at shutdown"
        );
    }
    if let Some((worker, counters)) = pump_worker {
        let unsent = counters
            .retry_depth
            .load(std::sync::atomic::Ordering::Relaxed);
        if unsent > 0 {
            warn!(
                unsent,
                "the event backlog did not drain within the shutdown window"
            );
        }
        worker.abort();
        info!(
            published = counters
                .published
                .load(std::sync::atomic::Ordering::Relaxed),
            failed = counters.failed.load(std::sync::atomic::Ordering::Relaxed),
            retried = counters.retried.load(std::sync::atomic::Ordering::Relaxed),
            dropped = counters.dropped.load(std::sync::atomic::Ordering::Relaxed),
            dropped_oldest = counters
                .dropped_oldest
                .load(std::sync::atomic::Ordering::Relaxed),
            unsent,
            "event bus totals at shutdown"
        );
    }
}

struct KeeperHandles {
    keeper: Arc<registry_keeper::RegistryKeeper>,
    renewing: tokio::task::JoinHandle<()>,
}

type ServingControlPlane =
    tokio::task::JoinHandle<Result<(), control_api::tonic::transport::Error>>;

struct ControlPlaneDrain {
    drain: Arc<drain::DrainState>,
    controller: Arc<SessionController>,
    plane: Arc<TapPlane>,
    keeper: Option<KeeperHandles>,
    serving: Mutex<Option<ServingControlPlane>>,
    events: Option<Arc<event_pump::PumpCounters>>,
}

#[control_api::async_trait]
impl DrainSteps for ControlPlaneDrain {
    fn stop_accepting(&self) {
        self.drain.begin();
        self.controller.begin_drain();
        info!(
            live_taps = self.plane.live_sessions(),
            "readiness is off and no new session or attachment will be taken here"
        );
    }

    async fn hand_off_leases(&self) -> usize {
        let Some(handles) = self.keeper.as_ref() else {
            return 0;
        };
        handles.renewing.abort();
        handles.keeper.hand_off_leases().await
    }

    async fn close_sessions(&self) -> SessionsClosed {
        let mut closed = SessionsClosed::default();
        for (session, _) in self.controller.snapshot() {
            let external_id = session.external_id.clone();
            let ended = self
                .controller
                .destroy_session(Request::new(proto::SessionRef {
                    id: Some(proto::session_ref::Id::ExternalId(external_id.clone())),
                }))
                .await;
            match ended {
                Ok(_) => {
                    closed.closed += 1;
                    info!(
                        %external_id,
                        "session closed for shutdown: consumers stopped, recording finished, tap unsubscribed"
                    );
                }
                Err(error) => {
                    closed.failed += 1;
                    warn!(
                        %external_id,
                        %error,
                        "this session could not be closed cleanly; rtpengine keeps its subscription"
                    );
                }
            }
        }
        closed
    }

    async fn await_control_plane_idle(&self) {
        let serving = self.serving.lock().ok().and_then(|mut held| held.take());
        let Some(serving) = serving else {
            return;
        };
        match serving.await {
            Ok(Ok(())) => info!("the control plane listener closed"),
            Ok(Err(error)) => warn!(%error, "the control plane stopped with an error"),
            Err(error) => warn!(%error, "the control plane task did not end cleanly"),
        }
    }

    async fn flush_events(&self) -> u64 {
        let Some(counters) = self.events.as_ref() else {
            return 0;
        };
        event_pump::await_empty_backlog(counters, drain::EVENT_FLUSH_WINDOW).await
    }
}

async fn event_sink_from_env() -> Result<
    Option<(
        Arc<event_pump::KafkaEventPump>,
        tokio::task::JoinHandle<()>,
        Arc<event_pump::PumpCounters>,
    )>,
    String,
> {
    let Ok(configured) = std::env::var(KAFKA_BROKERS_ENV) else {
        return Ok(None);
    };
    let brokers: Vec<String> = configured
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(str::to_string)
        .collect();
    if brokers.is_empty() {
        return Err(format!("{KAFKA_BROKERS_ENV} is set but names no brokers"));
    }
    let topic =
        std::env::var(EVENTS_TOPIC_ENV).unwrap_or_else(|_| event_pump::DEFAULT_TOPIC.to_string());
    let partitions = std::env::var(EVENTS_PARTITIONS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(event_pump::DEFAULT_PARTITIONS);

    let transport = event_pump::RskafkaTransport::connect(brokers, &topic, partitions).await?;
    let (pump, worker) = event_pump::KafkaEventPump::start(Arc::new(transport));
    let counters = pump.counters();
    Ok(Some((Arc::new(pump), worker, counters)))
}

async fn session_store_from_env(
) -> Result<Option<Arc<dyn session_store::SessionStore>>, session_store::StoreError> {
    let Ok(url) = std::env::var(REDIS_URL_ENV) else {
        return Ok(None);
    };
    let store = session_store::RedisSessionStore::connect(&url).await?;
    info!(url, "session registry connected");
    Ok(Some(Arc::new(store)))
}
