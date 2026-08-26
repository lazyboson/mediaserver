use crate::drain::DrainState;
use crate::event_pump::PumpCounters;
use crate::health::{Readiness, HEALTHZ_PATH, READYZ_PATH};
use crate::media_ports::MediaPortAllocator;
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
const METRICS_PATH: &str = "/metrics";
const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const PLAIN_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
const REQUEST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
pub struct MetricsSources {
    pub tap: TapPlaneMetrics,
    pub controller: Arc<SessionController>,
    pub pump: Option<Arc<PumpCounters>>,
    pub keeper: Option<Arc<KeeperCounters>>,
    pub drain: Arc<DrainState>,
    pub ports: Arc<MediaPortAllocator>,
    pub readiness: Arc<Readiness>,
}

pub struct HttpResponse {
    pub status: &'static str,
    pub content_type: &'static str,
    pub body: String,
}

pub fn route(request: &[u8], sources: &MetricsSources) -> HttpResponse {
    let Some(line) = std::str::from_utf8(request)
        .ok()
        .and_then(|text| text.lines().next())
    else {
        return HttpResponse {
            status: "400 Bad Request",
            content_type: PLAIN_CONTENT_TYPE,
            body: "the request line is not readable\n".to_string(),
        };
    };
    let mut words = line.split_whitespace();
    let method = words.next().unwrap_or_default();
    let target = words.next().unwrap_or_default();
    let path = target.split(['?', '#']).next().unwrap_or(target);
    if !matches!(method, "GET" | "HEAD") {
        return HttpResponse {
            status: "405 Method Not Allowed",
            content_type: PLAIN_CONTENT_TYPE,
            body: format!("{METRICS_PATH}, {HEALTHZ_PATH} and {READYZ_PATH} answer GET\n"),
        };
    }
    match path {
        METRICS_PATH => HttpResponse {
            status: "200 OK",
            content_type: EXPOSITION_CONTENT_TYPE,
            body: render(sources),
        },
        HEALTHZ_PATH => HttpResponse {
            status: "200 OK",
            content_type: PLAIN_CONTENT_TYPE,
            body: "alive\n".to_string(),
        },
        READYZ_PATH => {
            let verdict = sources.readiness.verdict();
            HttpResponse {
                status: if verdict.ready {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                },
                content_type: PLAIN_CONTENT_TYPE,
                body: verdict.body,
            }
        }
        _ => HttpResponse {
            status: "404 Not Found",
            content_type: PLAIN_CONTENT_TYPE,
            body: format!("{METRICS_PATH}\n{HEALTHZ_PATH}\n{READYZ_PATH}\n"),
        },
    }
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
        "mss_jitter_silence_gaps_total",
        "Sequence numbers absorbed as sender silence instead of counted as loss",
        snapshot.totals.jitter_silence_gaps,
    );
    counter(
        "mss_legs_ssrc_changes_total",
        "Times a tap leg started carrying a different sender ssrc mid-call",
        snapshot.totals.ssrc_changes,
    );
    counter(
        "mss_legs_ssrc_reresolved_total",
        "Times a refreshed rtpengine speaker map renamed a tap leg",
        snapshot.totals.reresolutions,
    );
    counter(
        "mss_inline_egress_packets_total",
        "RTP packets an inline leg paced out to its peer",
        snapshot.inline.datagrams_sent,
    );
    counter(
        "mss_inline_egress_send_errors_total",
        "Datagrams the inline egress socket refused",
        snapshot.inline.send_errors,
    );
    counter(
        "mss_inline_egress_silence_frames_total",
        "Paced frames the inline egress filled with silence because its queue was empty",
        snapshot.inline.silence_frames,
    );
    counter(
        "mss_inline_egress_late_ticks_total",
        "Inline egress pacing deadlines missed by more than one frame",
        snapshot.inline.late_ticks,
    );
    counter(
        "mss_inline_egress_chunks_refused_total",
        "Audio chunks the control world could not queue because the egress queue was full",
        snapshot.inline.chunks_refused,
    );
    counter(
        "mss_inline_egress_dropped_samples_total",
        "Samples dropped from the inline egress ring as oldest-first",
        snapshot.inline.dropped_samples,
    );
    counter(
        "mss_inline_egress_clears_total",
        "Times the inline egress queue was flushed for a barge-in",
        snapshot.inline.clears,
    );
    counter(
        "mss_inline_egress_cleared_samples_total",
        "Samples discarded by inline egress flushes",
        snapshot.inline.cleared_samples,
    );
    counter(
        "mss_inline_egress_pushed_samples_total",
        "PCM samples the control world queued for an inline leg to speak",
        snapshot.inline.pushed_samples,
    );
    counter(
        "mss_inline_egress_drained_samples_total",
        "Queued samples that have left the inline egress, whether paced out, dropped or flushed",
        snapshot.inline.drained_samples,
    );
    counter(
        "mss_conference_joins_total",
        "Inline legs seated in a conference mix",
        snapshot.conference.joins,
    );
    counter(
        "mss_conference_leaves_total",
        "Inline legs that left a conference mix",
        snapshot.conference.leaves,
    );
    counter(
        "mss_conference_mixed_frames_total",
        "Frames a conference mixed and paced to its members",
        snapshot.conference.mixed_frames,
    );
    counter(
        "mss_conference_clipped_samples_total",
        "Mixed samples saturated at the i16 rail; a conference that wants AGC",
        snapshot.conference.clipped_samples,
    );
    counter(
        "mss_conference_absent_frames_total",
        "Conference ticks a member contributed no frame to and was mixed as silence",
        snapshot.conference.absent_frames,
    );
    counter(
        "mss_conference_reanchors_total",
        "Times a conference clock fell a whole frame behind and re-anchored",
        snapshot.conference.reanchors,
    );
    counter(
        "mss_conference_route_changes_total",
        "Times an injecting attachment was routed to a whisper target, to everyone, or back to private",
        snapshot.conference.route_changes,
    );
    counter(
        "mss_conference_member_controls_total",
        "Times a member was muted, deafened, put on hold or released",
        snapshot.conference.member_controls,
    );
    counter(
        "mss_conference_prompt_frames_total",
        "Frames a prompt played into the whole room contributed to the mix",
        snapshot.conference.prompt_frames,
    );
    counter(
        "mss_conference_frames_refused_total",
        "Frames the mix matrix refused, by frame size or a stale membership handle",
        snapshot.conference.frames_refused,
    );
    counter(
        "mss_inline_egress_encode_errors_total",
        "Inline egress frames that could not be encoded or serialized",
        snapshot.inline.encode_errors,
    );
    counter(
        "mss_ssrc_requeries_total",
        "rtpengine queries made to re-resolve a leg whose ssrc changed",
        snapshot.ssrc_requeries,
    );
    counter(
        "mss_dtmf_events_dropped_total",
        "DTMF presses dropped before reaching the event bus because its queue was full",
        snapshot.dtmf_events_dropped,
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
    counter(
        "mss_consumer_suppressed_while_paused_total",
        "Frames not delivered to a consumer because its attachment was paused",
        snapshot.consumer_suppressed_while_paused,
    );
    counter(
        "mss_recordings_started_total",
        "Recordings opened on this pod",
        snapshot.recordings_started,
    );
    counter(
        "mss_recordings_stopped_total",
        "Recordings closed, whether or not the upload then succeeded",
        snapshot.recordings_stopped,
    );
    counter(
        "mss_recording_pauses_total",
        "Recording pauses that cut a segment",
        snapshot.recording_pauses,
    );
    counter(
        "mss_recording_uploads_total",
        "Recordings that reached object storage",
        snapshot.recording_uploads,
    );
    counter(
        "mss_recording_upload_failures_total",
        "Recordings that did not reach object storage",
        snapshot.recording_upload_failures,
    );
    counter(
        "mss_recording_spills_total",
        "Recordings written to local disk because their upload failed",
        snapshot.recording_spills,
    );
    counter(
        "mss_recording_spill_segments_total",
        "Closed recording segments written to local disk while the call was still up",
        snapshot.recording_segments_spilled,
    );
    counter(
        "mss_recording_spill_failures_total",
        "Closed recording segments that could not be written to local disk",
        snapshot.recording_segment_spill_failures,
    );
    counter(
        "mss_recording_salvaged_total",
        "Recordings uploaded from segments an earlier life of this pod left on disk",
        snapshot.recording_salvaged,
    );
    counter(
        "mss_recording_salvage_skipped_total",
        "Spilled recordings left alone because the object was already in storage",
        snapshot.recording_salvage_skipped,
    );
    counter(
        "mss_recording_salvage_failures_total",
        "Spilled recordings that could not be uploaded on this pod's start",
        snapshot.recording_salvage_failures,
    );
    counter(
        "mss_recording_frames_lost_on_adopt_total",
        "Recorded frames a dead pod held that the adopting pod could not read back",
        snapshot.recording_frames_lost_on_adopt,
    );
    counter(
        "mss_recordings_truncated_total",
        "Recordings that hit the length cap and lost their tail",
        snapshot.recordings_truncated,
    );
    counter(
        "mss_recording_bytes_uploaded_total",
        "Bytes of recorded audio uploaded",
        snapshot.recording_bytes_uploaded,
    );
    counter(
        "mss_recording_seconds_total",
        "Seconds of audio recorded, pauses excluded",
        snapshot.recording_seconds,
    );
    counter(
        "mss_recording_group_joins_refused_total",
        "Recording group joins refused for a reused label or a second recording id",
        snapshot.recording_group_joins_refused,
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
        "mss_inline_legs_live",
        "Inline rtp endpoints this pod is pacing audio out of",
        snapshot.inline_legs_live,
    );
    gauge(
        "mss_conferences_live",
        "Conference mixes this pod is running",
        snapshot.conferences_live,
    );
    gauge(
        "mss_conference_members_live",
        "Inline legs seated in a conference on this pod",
        snapshot.conference_members_live,
    );
    gauge(
        "mss_conference_whispers_live",
        "Conference legs whose injected audio is routed somewhere other than their own ear",
        snapshot.conference_whispers_live,
    );
    gauge(
        "mss_conference_muted_members",
        "Conference members whose own audio reaches nobody, hold included",
        snapshot.conference_members_muted,
    );
    gauge(
        "mss_conference_deaf_members",
        "Conference members the room is silent to, hold included",
        snapshot.conference_members_deaf,
    );
    gauge(
        "mss_conference_held_members",
        "Conference members on hold: neither heard nor hearing the room",
        snapshot.conference_members_held,
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
        "mss_recordings_live",
        "Recordings currently accumulating audio",
        snapshot.recordings_live,
    );
    gauge(
        "mss_recording_groups_live",
        "Recording groups open in this pod",
        snapshot.recording_groups_live,
    );
    gauge(
        "mss_recording_group_members_live",
        "Attachments recording as a member of a group",
        snapshot.recording_group_members_live,
    );
    if let Some(pump) = &sources.pump {
        gauge(
            "mss_events_retry_depth",
            "Events waiting in the pump backlog for a retry",
            pump.retry_depth.load(Ordering::Relaxed),
        );
    }
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
            "Publish attempts the broker refused; each one is retried",
            pump.failed.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_retried_total",
            "Republish attempts made after a refusal",
            pump.retried.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_dropped_total",
            "Events dropped because the pump handoff queue was full",
            pump.dropped.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_dropped_oldest_total",
            "Unsent events evicted because the retry backlog hit its cap",
            pump.dropped_oldest.load(Ordering::Relaxed),
        );
        counter(
            "mss_events_abandoned_total",
            "Unsent events discarded when the pump stopped with the bus still refusing",
            pump.abandoned.load(Ordering::Relaxed),
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
        counter(
            "mss_registry_orphans_unsubscribed_total",
            "Dead pods' rtpengine subscriptions cancelled before re-subscribing",
            keeper.orphans_unsubscribed.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_orphans_still_subscribed_total",
            "Adoptions that left the previous owner's tap in place; rtpengine copies              that call twice until it ends",
            keeper.orphans_still_subscribed.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_inline_not_adopted_total",
            "Orphaned inline legs released without adoption; an rtp endpoint cannot move pods",
            keeper.inline_not_adopted.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_grouped_not_adopted_total",
            "Recording-group members not restored on the adopting pod",
            keeper.grouped_not_adopted.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_surrendered_total",
            "Sessions this pod gave up because another pod holds their lease",
            keeper.surrendered.load(Ordering::Relaxed),
        );
        counter(
            "mss_registry_handed_off_total",
            "Leases released at shutdown so an adopter does not wait for the ttl",
            keeper.handed_off.load(Ordering::Relaxed),
        );
    }

    let ports = sources.ports.counters();
    for (name, help, kind, value) in [
        (
            "mss_media_ports_exhausted_total",
            "Media socket binds refused because the configured port range had nothing free",
            "counter",
            ports.exhausted,
        ),
        (
            "mss_media_ports_bind_conflicts_total",
            "Ports in the configured media range that another process already held",
            "counter",
            ports.bind_conflicts,
        ),
        (
            "mss_media_ports_in_use",
            "Media sockets this pod holds open",
            "gauge",
            ports.in_use,
        ),
        (
            "mss_media_ports_free",
            "Ports left in the configured media range; 0 when no range is configured",
            "gauge",
            ports.free,
        ),
        (
            "mss_media_ports_capacity",
            "Rtp sockets the configured media range can serve; 0 when no range is configured",
            "gauge",
            ports.capacity,
        ),
    ] {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        let _ = writeln!(out, "{name} {value}");
    }

    let _ = writeln!(
        out,
        "# HELP mss_draining Whether this pod is draining and refusing new sessions"
    );
    let _ = writeln!(out, "# TYPE mss_draining gauge");
    let _ = writeln!(
        out,
        "mss_draining {}",
        u8::from(sources.drain.is_draining())
    );

    let readiness = sources.readiness.snapshot();
    let _ = writeln!(
        out,
        "# HELP mss_ready Whether this pod answers its readiness probe with 200"
    );
    let _ = writeln!(out, "# TYPE mss_ready gauge");
    let _ = writeln!(out, "mss_ready {}", u8::from(readiness.ready));
    let _ = writeln!(
        out,
        "# HELP mss_dependency_ready Whether a configured dependency answered its last probe"
    );
    let _ = writeln!(out, "# TYPE mss_dependency_ready gauge");
    for dependency in readiness
        .dependencies
        .iter()
        .filter(|dependency| dependency.configured)
    {
        let _ = writeln!(
            out,
            "mss_dependency_ready{{dependency=\"{}\"}} {}",
            dependency.label,
            u8::from(dependency.ready)
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
            let answered = route(&request[..read], &sources);
            let response = format!(
                "HTTP/1.1 {}\r\n\
                 Content-Type: {}\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                answered.status,
                answered.content_type,
                answered.body.len(),
                answered.body
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
        let drain = DrainState::shared();
        let plane = TapPlane::new(TapPlaneConfig {
            default_node: None,
            local_media_address: IpAddr::from([127, 0, 0, 1]),
            advertised_media_address: IpAddr::from([127, 0, 0, 1]),
            media_ports: crate::media_ports::MediaPortAllocator::ephemeral(),
            format: AudioFormat::pcmu_8k_20ms(),
            transcode_at_tap: true,
            opus_decode_rate_hz: 16000,
            cookie_prefix: 1,
            sdp_session_id: 1,
            recording: crate::recorder::RecordingSupport::default(),
            capabilities: Arc::new(crate::rtpengine_capability::NodeCapabilityLog::new(true)),
        });
        MetricsSources {
            tap: plane.metrics(),
            controller: Arc::new(SessionController::new("test-pod")),
            pump: Some(Arc::new(PumpCounters::default())),
            keeper: Some(Arc::new(KeeperCounters::default())),
            drain: Arc::clone(&drain),
            ports: crate::media_ports::MediaPortAllocator::ephemeral(),
            readiness: crate::health::Readiness::shared(drain),
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
            "mss_events_dropped_oldest_total",
            "mss_events_abandoned_total",
            "mss_events_retried_total",
            "mss_events_retry_depth",
            "mss_events_failed_total",
            "mss_events_outbox_dropped_total",
            "mss_registry_lost_total",
            "mss_legs_stalled",
            "mss_ingest_stalls_total",
            "mss_jitter_lost_total",
            "mss_recording_upload_failures_total",
            "mss_recordings_truncated_total",
            "mss_recording_spills_total",
            "mss_recording_spill_segments_total",
            "mss_recording_salvaged_total",
            "mss_recording_frames_lost_on_adopt_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
        assert!(text.contains("mss_build_info{version="));
    }

    #[test]
    fn the_exposition_tells_the_whole_recording_story() {
        let text = render(&sources());
        for name in [
            "mss_recordings_started_total",
            "mss_recordings_stopped_total",
            "mss_recording_pauses_total",
            "mss_recording_uploads_total",
            "mss_recording_bytes_uploaded_total",
            "mss_recording_seconds_total",
            "mss_recordings_live",
            "mss_recording_groups_live",
            "mss_recording_group_members_live",
            "mss_recording_group_joins_refused_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
    }

    #[test]
    fn the_exposition_tells_the_whole_ssrc_reresolution_story() {
        let text = render(&sources());
        for name in [
            "mss_legs_unknown_ssrc",
            "mss_legs_ssrc_changes_total",
            "mss_ssrc_requeries_total",
            "mss_legs_ssrc_reresolved_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
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
        assert!(!text.contains("mss_events_retry_depth"));
        assert!(!text.contains("mss_registry_persisted_total"));
        assert!(text.contains("mss_sessions_live"));
    }

    #[test]
    fn the_drain_gauge_follows_the_flag_a_readiness_probe_will_read() {
        let sources = sources();
        assert!(render(&sources).contains("mss_draining 0"));
        sources.drain.begin();
        assert!(render(&sources).contains("mss_draining 1"));
    }

    fn probed_sources() -> MetricsSources {
        let sources = sources();
        for dependency in crate::health::DEPENDENCIES {
            sources.readiness.record_ready(dependency);
        }
        sources
    }

    fn get(path: &str, sources: &MetricsSources) -> HttpResponse {
        route(
            format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes(),
            sources,
        )
    }

    #[test]
    fn liveness_answers_while_the_process_runs_whatever_the_dependencies_do() {
        let sources = sources();
        let answered = get(HEALTHZ_PATH, &sources);
        assert_eq!(answered.status, "200 OK");
        assert_eq!(answered.body, "alive\n");
        sources.drain.begin();
        sources
            .readiness
            .record_failure(crate::health::Dependency::Redis, "connection refused");
        assert_eq!(get(HEALTHZ_PATH, &sources).status, "200 OK");
    }

    #[test]
    fn readiness_waits_for_the_first_probe_of_every_dependency() {
        let answered = get(READYZ_PATH, &sources());
        assert_eq!(answered.status, "503 Service Unavailable");
        assert!(
            answered.body.contains("not probed yet"),
            "{}",
            answered.body
        );
        assert_eq!(get(READYZ_PATH, &probed_sources()).status, "200 OK");
    }

    #[test]
    fn readiness_names_the_dependency_that_stopped_answering() {
        let sources = probed_sources();
        sources
            .readiness
            .record_failure(crate::health::Dependency::Redis, "connection refused");
        let answered = get(READYZ_PATH, &sources);
        assert_eq!(answered.status, "503 Service Unavailable");
        let first = answered.body.lines().next().unwrap();
        assert!(first.contains("redis"), "{first}");
        assert!(first.contains("connection refused"), "{first}");
    }

    #[test]
    fn readiness_turns_away_traffic_the_moment_a_drain_begins() {
        let sources = probed_sources();
        assert_eq!(get(READYZ_PATH, &sources).status, "200 OK");
        sources.drain.begin();
        let answered = get(READYZ_PATH, &sources);
        assert_eq!(answered.status, "503 Service Unavailable");
        assert!(answered.body.contains("draining"), "{}", answered.body);
    }

    #[test]
    fn the_exposition_says_whether_the_pod_is_ready_and_which_dependency_is_not() {
        let sources = probed_sources();
        sources
            .readiness
            .record_not_configured(crate::health::Dependency::Kafka);
        sources
            .readiness
            .record_failure(crate::health::Dependency::Redis, "connection refused");
        let text = render(&sources);
        assert!(text.contains("mss_ready 0"), "{text}");
        assert!(
            text.contains("mss_dependency_ready{dependency=\"redis\"} 0"),
            "{text}"
        );
        assert!(
            text.contains("mss_dependency_ready{dependency=\"rtpengine\"} 1"),
            "{text}"
        );
        assert!(!text.contains("dependency=\"kafka\""), "{text}");
    }

    #[test]
    fn only_the_three_documented_paths_answer_and_only_to_a_get() {
        let sources = probed_sources();
        assert_eq!(get(METRICS_PATH, &sources).status, "200 OK");
        assert_eq!(get("/metrics?collect=all", &sources).status, "200 OK");
        let unknown = get("/", &sources);
        assert_eq!(unknown.status, "404 Not Found");
        assert!(unknown.body.contains(READYZ_PATH), "{}", unknown.body);
        let refused = route(b"POST /metrics HTTP/1.1\r\n\r\n", &sources);
        assert_eq!(refused.status, "405 Method Not Allowed");
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

    #[tokio::test]
    async fn the_probe_paths_answer_over_the_wire_with_their_own_status_codes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sources = probed_sources();
        let readiness = Arc::clone(&sources.readiness);
        tokio::spawn(serve(listener, sources));

        assert!(fetch(address, HEALTHZ_PATH)
            .await
            .starts_with("HTTP/1.1 200 OK"));
        assert!(fetch(address, READYZ_PATH)
            .await
            .starts_with("HTTP/1.1 200 OK"));
        readiness.record_failure(crate::health::Dependency::Redis, "connection refused");
        let refused = fetch(address, READYZ_PATH).await;
        assert!(
            refused.starts_with("HTTP/1.1 503 Service Unavailable"),
            "{refused}"
        );
        assert!(
            refused.contains("redis unreachable: connection refused"),
            "{refused}"
        );
    }

    async fn fetch(address: std::net::SocketAddr, path: &str) -> String {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        socket.read_to_string(&mut response).await.unwrap();
        response
    }
}
