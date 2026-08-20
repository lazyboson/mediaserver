use crate::event_pump::PumpCounters;
use crate::registry_keeper::KeeperCounters;
use crate::tap_plane::TapPlaneMetrics;
use control_api::SessionController;
use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

const REQUEST_READ_LIMIT: usize = 8 * 1024;
const REQUEST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
pub struct MetricsSources {
    pub tap: TapPlaneMetrics,
    pub controller: Arc<SessionController>,
    pub pump: Option<Arc<PumpCounters>>,
    pub keeper: Option<Arc<KeeperCounters>>,
}

pub fn render(sources: &MetricsSources) -> String {
    let mut out = String::with_capacity(4096);
    let snapshot = sources.tap.snapshot();
    let (registry_sessions, registry_attachments) = sources.controller.counts();

    let mut counter = |name: &str, help: &str, value: u64| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {value}");
    };

    counter(
        "mss_ingest_datagrams_total",
        "Datagrams received on tap legs",
        snapshot.totals.datagrams,
    );
    counter(
        "mss_ingest_recv_errors_total",
        "Socket errors while draining tap legs",
        snapshot.totals.recv_errors,
    );
    counter(
        "mss_ingest_underruns_total",
        "Frames released as silence because no audio was buffered",
        snapshot.totals.underruns,
    );
    counter(
        "mss_ingest_frames_played_total",
        "Frames released from real audio",
        snapshot.totals.frames_played,
    );
    counter(
        "mss_ingest_frames_concealed_total",
        "Frames released as concealment for lost packets",
        snapshot.totals.frames_concealed,
    );
    counter(
        "mss_ingest_frames_suppressed_total",
        "Frames suppressed for telephone events",
        snapshot.totals.frames_suppressed,
    );
    counter(
        "mss_ingest_companded_total",
        "Packets converted from the companion g711 variant",
        snapshot.totals.companded,
    );
    counter(
        "mss_ingest_unknown_payload_type_total",
        "Packets dropped for an unexpected payload type",
        snapshot.totals.unknown_payload_type,
    );
    counter(
        "mss_ingest_unparsable_total",
        "Datagrams that were not parseable RTP",
        snapshot.totals.unparsable,
    );
    counter(
        "mss_ingest_telephone_events_total",
        "RFC 4733 telephone-event packets seen",
        snapshot.totals.telephone_events,
    );
    counter(
        "mss_ingest_dtmf_digits_total",
        "DTMF digits reported",
        snapshot.totals.dtmf_digits,
    );
    counter(
        "mss_jitter_lost_total",
        "Packets the jitter buffer declared lost",
        snapshot.totals.jitter_lost,
    );
    counter(
        "mss_jitter_duplicates_total",
        "Duplicate packets discarded by the jitter buffer",
        snapshot.totals.jitter_duplicates,
    );
    counter(
        "mss_jitter_late_drops_total",
        "Packets that arrived too late to play",
        snapshot.totals.jitter_late_drops,
    );
    counter(
        "mss_jitter_resets_total",
        "Jitter buffer resets from sequence discontinuities",
        snapshot.totals.jitter_resets,
    );
    counter(
        "mss_ingest_stalls_total",
        "Times a tap leg stopped receiving datagrams for the watchdog window",
        snapshot.totals.stalls,
    );
    counter(
        "mss_consumer_dropped_oldest_total",
        "Frames dropped from slow consumer queues, oldest first",
        snapshot.consumer_dropped_oldest,
    );
    counter(
        "mss_consumer_delivered_total",
        "Frames queued to consumers",
        snapshot.consumer_delivered,
    );

    let mut gauge = |name: &str, help: &str, value: u64| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {value}");
    };

    gauge(
        "mss_sessions_live",
        "Sessions with a live tap in this pod",
        snapshot.sessions_live,
    );
    gauge(
        "mss_legs_live",
        "Tap legs currently capturing",
        snapshot.legs_live,
    );
    gauge(
        "mss_legs_unknown_ssrc",
        "Live legs whose speaker could not be named from rtpengine ssrcs",
        snapshot.legs_unknown_ssrc,
    );
    gauge(
        "mss_legs_stalled",
        "Live legs the audio-flow watchdog reports as stalled",
        snapshot.legs_stalled,
    );
    gauge(
        "mss_consumers_live",
        "Consumers currently attached to hubs",
        snapshot.consumers_live,
    );
    gauge(
        "mss_consumer_queue_depth_frames",
        "Frames waiting in consumer queues, summed",
        snapshot.consumer_queue_depth,
    );
    gauge(
        "mss_consumer_queue_depth_frames_max",
        "Deepest single consumer queue",
        snapshot.consumer_queue_depth_max,
    );
    gauge(
        "mss_registry_sessions",
        "Sessions the control-plane registry holds",
        registry_sessions as u64,
    );
    gauge(
        "mss_registry_attachments",
        "Attachments the control-plane registry holds",
        registry_attachments as u64,
    );

    let mut counter = |name: &str, help: &str, value: u64| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {value}");
    };

    counter(
        "mss_events_outbox_dropped_total",
        "Events the registry outbox dropped before any sink saw them",
        sources.controller.events_dropped(),
    );
    if let Some(pump) = &sources.pump {
        counter(
            "mss_events_accepted_total",
            "Events handed to the bus pump",
            pump.accepted.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_published_total",
            "Events that reached the bus",
            pump.published.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_failed_total",
            "Events the broker refused",
            pump.failed.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_dropped_total",
            "Events dropped because the pump queue was full",
            pump.dropped.load(Ordering::Relaxed),
        );
    }
    if let Some(keeper) = &sources.keeper {
        counter(
            "mss_registry_persisted_total",
            "Sessions written to the shared registry",
            keeper.persisted.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_renewed_total",
            "Ownership lease renewals",
            keeper.renewed.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_lost_total",
            "Leases another pod now holds; rising means split brain",
            keeper.lost.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_adopted_total",
            "Orphaned sessions this pod adopted",
            keeper.adopted.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_unrebuildable_total",
            "Orphans released because they carried no call identity",
            keeper.unrebuildable.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_released_total",
            "Ended sessions forgotten from the shared registry",
            keeper.released.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_failed_total",
            "Registry operations that failed",
            keeper.failed.load(Ordering::Relaxed),
        );
    }

    let _ = writeln!(
        out,
        "# HELP mss_build_info Version of this mediaserverd binary"
    );
    let _ = writeln!(out, "# TYPE mss_build_info gauge");
    let _ = writeln!(
        out,
        "mss_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );
    out
}

pub async fn serve(listener: TcpListener, sources: MetricsSources) {
    let local = listener.local_addr().ok();
    info!(listen = ?local, "metrics endpoint is serving");
    loop {
        let (mut socket, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                warn!(%error, "metrics listener could not accept a connection");
                continue;
            }
        };
        let sources = sources.clone();
        tokio::spawn(async move {
            let mut request = vec![0u8; REQUEST_READ_LIMIT];
            let mut read = 0usize;
            let complete = tokio::time::timeout(REQUEST_READ_TIMEOUT, async {
                loop {
                    match socket.read(&mut request[read..]).await {
                        Ok(0) => return false,
                        Ok(n) => {
                            read += n;
                            if request[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                                return true;
                            }
                            if read == request.len() {
                                return false;
                            }
                        }
                        Err(_) => return false,
                    }
                }
            })
            .await
            .unwrap_or(false);
            if !complete {
                return;
            }
            let body = render(&sources);
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                body.len(),
                body
            );
            if let Err(error) = socket.write_all(response.as_bytes()).await {
                warn!(%peer, %error, "could not write a metrics response");
            }
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tap_plane::{TapPlane, TapPlaneConfig};
    use media_core::AudioFormat;
    use std::net::IpAddr;

    fn sources() -> MetricsSources {
        let plane = TapPlane::new(TapPlaneConfig {
            default_node: None,
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            format: AudioFormat::pcmu_8k_20ms(),
            cookie_prefix: 1,
            sdp_session_id: 1,
        });
        MetricsSources {
            tap: plane.metrics(),
            controller: Arc::new(SessionController::new("test-pod")),
            pump: Some(Arc::new(PumpCounters::default())),
            keeper: Some(Arc::new(KeeperCounters::default())),
        }
    }

    #[test]
    fn the_exposition_carries_every_drop_counter_the_alerts_need() {
        let sources = sources();
        sources
            .pump
            .as_ref()
            .unwrap()
            .dropped
            .store(3, Ordering::Relaxed);
        let text = render(&sources);
        for name in [
            "mss_consumer_dropped_oldest_total",
            "mss_events_dropped_total 3",
            "mss_events_failed_total",
            "mss_events_outbox_dropped_total",
            "mss_registry_lost_total",
            "mss_legs_stalled",
            "mss_ingest_stalls_total",
            "mss_jitter_lost_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
        assert!(text.contains("mss_build_info{version="));
    }

    #[test]
    fn every_series_declares_help_and_type_before_its_value() {
        let text = render(&sources());
        let mut declared = std::collections::HashSet::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                declared.insert(rest.split(' ').next().unwrap().to_string());
            } else if !line.starts_with('#') && !line.is_empty() {
                let name = line.split([' ', '{']).next().unwrap();
                assert!(declared.contains(name), "{name} has no TYPE declaration");
            }
        }
    }

    #[test]
    fn absent_optional_sources_leave_their_series_out_rather_than_lying_zero() {
        let mut sources = sources();
        sources.pump = None;
        sources.keeper = None;
        let text = render(&sources);
        assert!(!text.contains("mss_events_published_total"));
        assert!(!text.contains("mss_registry_persisted_total"));
        assert!(text.contains("mss_sessions_live"));
    }

    #[tokio::test]
    async fn the_endpoint_answers_a_plain_http_get() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, sources()));

        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        socket.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("mss_sessions_live 0"));
        assert!(response.contains("text/plain"));
    }
}
