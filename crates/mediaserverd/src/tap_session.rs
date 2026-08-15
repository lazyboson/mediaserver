use crate::consumer_ws::{self, BridgeCommand, ConsumerConfig};
use crate::hub::{Hub, HubClient, TrackSelection};
use crate::ng_transport::{NgTransport, NgTransportConfig, TransportError};
use crate::tap_spike::{capture, wav_blob, write_wav, SpikeError, TapLeg};
use media_core::{AudioFormat, Track};
use rtpengine_ng::{
    PlayMedia, PlaySource, PlayTarget, SdpError, SubscribeRequest, SubscriptionAnswer,
    SubscriptionOffer,
};
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{info, warn};

const NODE_ENV: &str = "MSS_RTPENGINE_NODE";
const CALL_ID_ENV: &str = "MSS_TAP_CALL_ID";
const FROM_TAGS_ENV: &str = "MSS_TAP_FROM_TAGS";
const LOCAL_IP_ENV: &str = "MSS_TAP_LOCAL_IP";
const OUTPUT_ENV: &str = "MSS_TAP_OUTPUT";
const SECONDS_ENV: &str = "MSS_TAP_SECONDS";

const CONSUMER_URL_ENV: &str = "MSS_CONSUMER_URL";
const CONSUMER_ACCOUNT_ENV: &str = "MSS_CONSUMER_ACCOUNT_ID";
const CONSUMER_STREAM_SID_ENV: &str = "MSS_CONSUMER_STREAM_SID";
const INJECT_TARGET_ENV: &str = "MSS_INJECT_TARGET";
const CONSUMER_TRACKS_ENV: &str = "MSS_CONSUMER_TRACKS";
const DATAGRAM_LOG_DIR_ENV: &str = "MSS_TAP_DATAGRAM_LOG_DIR";
const LISTENERS_ENV: &str = "MSS_LISTENERS";

const DEFAULT_OUTPUT: &str = "tap.wav";
const DEFAULT_SECONDS: u64 = 30;
const DEFAULT_ACCOUNT_ID: &str = "lab";
const TARGET_DEPTH_PACKETS: u16 = 3;
const MAX_TAPPED_STREAMS: usize = 2;
const CONSUMER_QUEUE_FRAMES: usize = 200;
const INJECT_EVERYONE: &str = "everyone";
const CONSUMER_BOTH_TRACKS: &str = "both";
const BLOB_SAMPLES_PER_DATAGRAM: usize = 24_000;
const DATAGRAM_LOG_BYTES_PER_SECOND: usize = 16_000;

#[derive(Debug, Error)]
pub enum TapSessionError {
    #[error("ng transport: {0}")]
    Transport(#[from] TransportError),
    #[error("subscribe reply carried no {0}")]
    ReplyMissing(&'static str),
    #[error("subscription sdp: {0}")]
    Sdp(#[from] SdpError),
    #[error("capture: {0}")]
    Spike(#[from] SpikeError),
    #[error("socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("rtpengine offered {0} streams; the spike writes at most {MAX_TAPPED_STREAMS}")]
    TooManyStreams(usize),
    #[error("capture thread did not return its legs")]
    CaptureThreadLost,
    #[error("{env} must be set to run the tap spike")]
    MissingEnv { env: &'static str },
    #[error("{env}={value:?} is not usable: {reason}")]
    BadEnv {
        env: &'static str,
        value: String,
        reason: &'static str,
    },
}

pub struct TapSpikeRequest {
    pub node: SocketAddr,
    pub call_id: String,
    pub from_tags: Vec<String>,
    pub local_media_address: IpAddr,
    pub output: PathBuf,
    pub duration: Duration,
    pub sdp_session_id: u64,
    pub format: AudioFormat,
}

pub fn request_from_env(sdp_session_id: u64) -> Result<Option<TapSpikeRequest>, TapSessionError> {
    let Ok(call_id) = std::env::var(CALL_ID_ENV) else {
        return Ok(None);
    };
    let node = required_env(NODE_ENV)?;
    let node = node.parse().map_err(|_| TapSessionError::BadEnv {
        env: NODE_ENV,
        value: node.clone(),
        reason: "expected ip:port",
    })?;
    let local = required_env(LOCAL_IP_ENV)?;
    let local_media_address = local.parse().map_err(|_| TapSessionError::BadEnv {
        env: LOCAL_IP_ENV,
        value: local.clone(),
        reason: "expected an ip address rtpengine can route to",
    })?;
    let from_tags = std::env::var(FROM_TAGS_ENV)
        .unwrap_or_default()
        .split(',')
        .map(|tag| tag.trim().to_string())
        .filter(|tag| !tag.is_empty())
        .collect();
    let output = std::env::var(OUTPUT_ENV).unwrap_or_else(|_| DEFAULT_OUTPUT.to_string());
    let seconds = match std::env::var(SECONDS_ENV) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|seconds| *seconds > 0)
            .ok_or(TapSessionError::BadEnv {
                env: SECONDS_ENV,
                value,
                reason: "expected a positive number of seconds",
            })?,
        Err(_) => DEFAULT_SECONDS,
    };

    Ok(Some(TapSpikeRequest {
        node,
        call_id,
        from_tags,
        local_media_address,
        output: PathBuf::from(output),
        duration: Duration::from_secs(seconds),
        sdp_session_id,
        format: AudioFormat::pcmu_8k_20ms(),
    }))
}

fn required_env(env: &'static str) -> Result<String, TapSessionError> {
    std::env::var(env).map_err(|_| TapSessionError::MissingEnv { env })
}

pub async fn run(request: TapSpikeRequest, cookie_prefix: u64) -> Result<(), TapSessionError> {
    let transport = Arc::new(
        NgTransport::bind(
            SocketAddr::from(([0, 0, 0, 0], 0)),
            request.node,
            NgTransportConfig::default(),
            cookie_prefix,
        )
        .await?,
    );

    let subscribe = SubscribeRequest {
        call_id: request.call_id.clone(),
        from_tags: request.from_tags.clone(),
        mix: false,
        accept_codecs: Vec::new(),
        transcode_codecs: vec![request.format.encoding.rtpmap_name().to_string()],
        label: Some("mss-tap".to_string()),
    };
    info!(
        call_id = %request.call_id,
        from_tags = ?request.from_tags,
        node = %request.node,
        "sending NG subscribe request"
    );
    let reply = transport.subscribe_request(&subscribe).await?;
    let offer_sdp = reply
        .sdp()
        .ok_or(TapSessionError::ReplyMissing("sdp"))?
        .to_string();
    let to_tag = reply
        .to_tag()
        .ok_or(TapSessionError::ReplyMissing("to-tag"))?
        .to_string();
    let offer = SubscriptionOffer::parse(&offer_sdp)?;
    if offer.streams.len() > MAX_TAPPED_STREAMS {
        return Err(TapSessionError::TooManyStreams(offer.streams.len()));
    }
    for (index, stream) in offer.streams.iter().enumerate() {
        info!(
            index,
            label = ?stream.label,
            source = ?offer.stream_address(index),
            source_port = stream.port,
            payload_types = ?stream.payload_types,
            "rtpengine offered a tap stream"
        );
    }

    let mut sockets = Vec::with_capacity(offer.streams.len());
    let mut receive_ports = Vec::with_capacity(offer.streams.len());
    for _ in &offer.streams {
        let socket = UdpSocket::bind(SocketAddr::new(request.local_media_address, 0))?;
        receive_ports.push(socket.local_addr()?.port());
        sockets.push(socket);
    }

    let local_address = request.local_media_address.to_string();
    let answer_sdp = SubscriptionAnswer {
        session_id: request.sdp_session_id,
        local_address: &local_address,
        receive_ports: &receive_ports,
        format: request.format,
    }
    .to_sdp(&offer)?;
    info!(?receive_ports, to_tag = %to_tag, "answering the subscription");
    transport
        .subscribe_answer(&request.call_id, &to_tag, &answer_sdp)
        .await?;

    let consumer = consumer_config_from_env(&request);
    let (mut hub, hub_client) = Hub::new();

    let mut legs = Vec::with_capacity(sockets.len());
    for (index, socket) in sockets.into_iter().enumerate() {
        let telephone_event_payload_type = offer
            .streams
            .get(index)
            .and_then(|stream| stream.telephone_event())
            .map(|event| event.payload_type);
        let mut leg = TapLeg::new(
            track_for_stream(index),
            socket,
            request.format,
            TARGET_DEPTH_PACKETS,
            telephone_event_payload_type,
            request.duration,
        )?;
        if datagram_log_dir().is_some() {
            leg = leg.with_datagram_log(
                request.duration.as_secs().max(1) as usize * DATAGRAM_LOG_BYTES_PER_SECOND,
            );
        }
        legs.push(leg);
    }

    let mut listener_tasks = Vec::new();
    for (name, config) in listener_configs(&request) {
        match hub_client.attach(CONSUMER_QUEUE_FRAMES, TrackSelection::All) {
            Some(subscription) => {
                info!(listener = %name, url = %config.url, "attaching a listen-only consumer");
                listener_tasks.push((
                    name,
                    tokio::spawn(consumer_ws::run(config, subscription, None)),
                ));
            }
            None => {
                warn!(listener = %name, "the hub refused this listener; it will not receive audio")
            }
        }
    }

    let mut consumer_task = None;
    let mut injection_task = None;
    if let Some(config) = consumer {
        match hub_client.attach(CONSUMER_QUEUE_FRAMES, consumer_selection()) {
            Some(subscription) => {
                let (commands_tx, commands_rx) = mpsc::channel(8);
                info!(
                    url = %config.url,
                    stream_sid = %config.stream_sid,
                    "streaming this tap to a consumer bridge"
                );
                injection_task = Some(tokio::spawn(inject_bridge_speech(
                    Arc::clone(&transport),
                    hub_client.clone(),
                    request.call_id.clone(),
                    inject_target(&request),
                    request.format,
                    commands_rx,
                )));
                consumer_task = Some(tokio::spawn(consumer_ws::run(
                    config,
                    subscription,
                    Some(commands_tx),
                )));
            }
            None => warn!("the hub refused the consumer attach; streaming disabled"),
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let capture_stop = Arc::clone(&stop);
    let format = request.format;
    let duration = request.duration;
    let capture_thread = std::thread::Builder::new()
        .name("mss-tap-spike".to_string())
        .spawn(move || {
            let summary = capture(&mut legs, Some(&mut hub), format, duration, &capture_stop);
            let fanned_out = (hub.published(), hub.consumer_count());
            drop(hub);
            (legs, summary, fanned_out)
        })?;

    tokio::select! {
        _ = tokio::time::sleep(duration) => info!("tap spike reached its configured duration"),
        _ = tokio::signal::ctrl_c() => info!("shutdown signal during tap spike"),
    }
    stop.store(true, Ordering::Relaxed);

    let (legs, summary, (hub_published, hub_consumers)) =
        tokio::task::spawn_blocking(move || capture_thread.join())
            .await
            .map_err(|_| TapSessionError::CaptureThreadLost)?
            .map_err(|_| TapSessionError::CaptureThreadLost)?;

    match transport.unsubscribe(&request.call_id, &to_tag).await {
        Ok(_) => info!(call_id = %request.call_id, "unsubscribed"),
        Err(error) => warn!(
            %error,
            call_id = %request.call_id,
            "unsubscribe failed; rtpengine keeps the subscription until it times out"
        ),
    }

    for leg in &legs {
        let stats = leg.stats();
        info!(
            track = ?leg.track(),
            samples_captured = leg.samples().len(),
            digits_seen = %leg.digits_seen(),
            datagrams = stats.datagrams,
            frames_played = stats.pipeline.frames_played,
            frames_concealed = stats.pipeline.frames_concealed,
            frames_suppressed = stats.pipeline.frames_suppressed,
            underruns = stats.underruns,
            telephone_event_packets = stats.pipeline.telephone_events,
            dtmf_digits = stats.pipeline.dtmf_digits,
            unknown_payload_type = stats.pipeline.unknown_payload_type,
            unparsable = stats.pipeline.unparsable,
            jitter_lost = stats.jitter.lost,
            jitter_duplicates = stats.jitter.duplicates,
            jitter_late_drops = stats.jitter.late_drops,
            jitter_resets = stats.jitter.resets,
            capture_full = stats.capture_full,
            drain_batches_filled = stats.drain_batches_filled,
            recv_errors = stats.recv_errors,
            "tap leg finished"
        );
    }

    let wav = tokio::task::block_in_place(|| {
        if let Some(dir) = datagram_log_dir() {
            for leg in &legs {
                let path = dir.join(format!("tap-{:?}.dglog", leg.track()).to_lowercase());
                match std::fs::write(&path, leg.datagram_log()) {
                    Ok(()) => info!(
                        path = %path.display(),
                        bytes = leg.datagram_log().len(),
                        truncated = leg.stats().datagram_log_full,
                        "wrote the datagram log for replay fixtures"
                    ),
                    Err(error) => {
                        warn!(%error, path = %path.display(), "datagram log not written")
                    }
                }
            }
        }
        write_wav(&request.output, request.format, &legs)
    })?;
    info!(
        output = %request.output.display(),
        channels = wav.channels,
        frames = wav.frames,
        releases = summary.releases,
        reanchors = summary.reanchors,
        hub_published,
        hub_consumers,
        elapsed_ms = summary.elapsed.as_millis() as u64,
        "tap spike wrote its wav artifact"
    );

    drop(legs);
    if let Some(task) = consumer_task {
        match task.await {
            Ok(Ok(stats)) => info!(
                media_sent = stats.media_sent,
                dtmf_sent = stats.dtmf_sent,
                media_dropped = stats.media_dropped,
                inbound_media = stats.inbound_media,
                inbound_unknown_encoding = stats.inbound_unknown_encoding,
                utterances = stats.utterances,
                barges = stats.barges,
                "consumer bridge finished"
            ),
            Ok(Err(error)) => warn!(%error, "consumer bridge failed"),
            Err(_) => warn!("the consumer bridge task was lost"),
        }
    }
    for (name, task) in listener_tasks {
        match task.await {
            Ok(Ok(stats)) => info!(
                listener = %name,
                media_sent = stats.media_sent,
                dtmf_sent = stats.dtmf_sent,
                media_dropped = stats.media_dropped,
                "listener finished"
            ),
            Ok(Err(error)) => warn!(listener = %name, %error, "listener failed"),
            Err(_) => warn!(listener = %name, "listener task was lost"),
        }
    }
    if let Some(task) = injection_task {
        match task.await {
            Ok(played) => info!(
                utterances_played = played,
                "bridge speech injection finished"
            ),
            Err(_) => warn!("the injection task was lost"),
        }
    }
    Ok(())
}

fn consumer_config_from_env(request: &TapSpikeRequest) -> Option<ConsumerConfig> {
    let url = std::env::var(CONSUMER_URL_ENV).ok()?;
    let stream_sid = std::env::var(CONSUMER_STREAM_SID_ENV)
        .unwrap_or_else(|_| format!("MZ-{}", request.call_id));
    Some(ConsumerConfig {
        url,
        account_id: std::env::var(CONSUMER_ACCOUNT_ENV)
            .unwrap_or_else(|_| DEFAULT_ACCOUNT_ID.to_string()),
        call_sid: request.call_id.clone(),
        stream_sid,
        format: request.format,
        tracks: (0..request.from_tags.len().max(1))
            .map(track_for_stream)
            .filter(|track| consumer_selection().wants(*track))
            .map(|track| consumer_ws::track_name(track).to_string())
            .collect(),
        custom_parameters: HashMap::new(),
    })
}

fn listener_configs(request: &TapSpikeRequest) -> Vec<(String, ConsumerConfig)> {
    let account_id =
        std::env::var(CONSUMER_ACCOUNT_ENV).unwrap_or_else(|_| DEFAULT_ACCOUNT_ID.to_string());
    std::env::var(LISTENERS_ENV)
        .unwrap_or_default()
        .split(',')
        .filter_map(|entry| entry.trim().split_once('='))
        .filter(|(name, url)| !name.trim().is_empty() && !url.trim().is_empty())
        .map(|(name, url)| {
            let name = name.trim().to_string();
            let config = ConsumerConfig {
                url: url.trim().to_string(),
                account_id: account_id.clone(),
                call_sid: request.call_id.clone(),
                stream_sid: format!("MZ-{name}"),
                format: request.format,
                tracks: vec![
                    consumer_ws::track_name(Track::Customer).to_string(),
                    consumer_ws::track_name(Track::Agent).to_string(),
                ],
                custom_parameters: HashMap::new(),
            };
            (name, config)
        })
        .collect()
}

fn consumer_selection() -> TrackSelection {
    if std::env::var(CONSUMER_TRACKS_ENV).unwrap_or_default() == CONSUMER_BOTH_TRACKS {
        TrackSelection::All
    } else {
        TrackSelection::Only(Track::Customer)
    }
}

fn datagram_log_dir() -> Option<PathBuf> {
    std::env::var(DATAGRAM_LOG_DIR_ENV).ok().map(PathBuf::from)
}

fn inject_target(request: &TapSpikeRequest) -> PlayTarget {
    let configured = std::env::var(INJECT_TARGET_ENV).unwrap_or_default();
    if configured == INJECT_EVERYONE {
        return PlayTarget::HeardByEveryone;
    }
    match request.from_tags.first() {
        Some(tag) => PlayTarget::HeardBy(tag.clone()),
        None => PlayTarget::HeardByEveryone,
    }
}

async fn inject_bridge_speech(
    transport: Arc<NgTransport>,
    hub_client: HubClient,
    call_id: String,
    target: PlayTarget,
    format: AudioFormat,
    mut commands: mpsc::Receiver<BridgeCommand>,
) -> u64 {
    let mut played = 0;
    let mut pending = VecDeque::new();
    loop {
        let command = match pending.pop_front() {
            Some(command) => command,
            None => match commands.recv().await {
                Some(command) => command,
                None => break,
            },
        };
        match command {
            BridgeCommand::Speak(pcm) => {
                let pieces = pcm.chunks(BLOB_SAMPLES_PER_DATAGRAM).count();
                if pieces > 1 {
                    info!(
                        samples = pcm.len(),
                        pieces, "utterance exceeds one NG datagram; playing it in pieces"
                    );
                }
                for piece in pcm.chunks(BLOB_SAMPLES_PER_DATAGRAM) {
                    let blob = match wav_blob(format, piece) {
                        Ok(blob) => blob,
                        Err(error) => {
                            warn!(%error, "could not wrap bridge speech as a wav blob");
                            continue;
                        }
                    };
                    let play = PlayMedia {
                        call_id: call_id.clone(),
                        target: target.clone(),
                        source: PlaySource::Blob(blob),
                        repeat_times: None,
                        block_egress: true,
                    };
                    match transport.play_media(&play).await {
                        Ok(_) => {
                            played += 1;
                            if !hub_client.inject(piece.to_vec()) {
                                warn!("injected-audio queue full; this piece is missing from recordings");
                            }
                            info!(
                                samples = piece.len(),
                                ?target,
                                "played an utterance from the bridge into the call"
                            );
                        }
                        Err(error) => warn!(%error, "rtpengine refused the utterance"),
                    }
                    let barged = !wait_out_piece(
                        &transport,
                        &call_id,
                        &target,
                        piece_duration(format, piece.len()),
                        &mut commands,
                        &mut pending,
                    )
                    .await;
                    if barged {
                        break;
                    }
                }
            }
            BridgeCommand::Barge => match transport.stop_media(&call_id, &target).await {
                Ok(_) => info!(?target, "barge-in stopped the utterance"),
                Err(error) => warn!(%error, "rtpengine refused the barge-in"),
            },
        }
    }
    played
}

async fn wait_out_piece(
    transport: &NgTransport,
    call_id: &str,
    target: &PlayTarget,
    duration: Duration,
    commands: &mut mpsc::Receiver<BridgeCommand>,
    pending: &mut VecDeque<BridgeCommand>,
) -> bool {
    let piece_done = tokio::time::sleep(duration);
    tokio::pin!(piece_done);
    loop {
        tokio::select! {
            _ = &mut piece_done => return true,
            command = commands.recv() => match command {
                Some(BridgeCommand::Barge) => {
                    match transport.stop_media(call_id, target).await {
                        Ok(_) => info!(?target, "barge-in cut the utterance short"),
                        Err(error) => warn!(%error, "rtpengine refused the barge-in"),
                    }
                    return false;
                }
                Some(command) => pending.push_back(command),
                None => {
                    piece_done.as_mut().await;
                    return true;
                }
            }
        }
    }
}

fn piece_duration(format: AudioFormat, samples: usize) -> Duration {
    let rate = format.sample_rate_hz.max(1) as u64;
    Duration::from_millis(samples as u64 * 1000 / rate)
}

fn track_for_stream(index: usize) -> Track {
    match index {
        0 => Track::Customer,
        _ => Track::Agent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_map_to_customer_then_agent() {
        assert_eq!(track_for_stream(0), Track::Customer);
        assert_eq!(track_for_stream(1), Track::Agent);
    }

    #[test]
    fn listeners_are_parsed_as_name_equals_url_pairs() {
        let sep = "\x2f\x2f";
        std::env::set_var(
            LISTENERS_ENV,
            format!(" rtt=ws:{sep}10.0.0.1:9090{slash}ws , recorder=ws:{sep}10.0.0.2:9091{slash}ws ,,broken, ", slash = "\x2f"),
        );
        let request = TapSpikeRequest {
            node: "127.0.0.1:22222".parse().unwrap(),
            call_id: "call-7".into(),
            from_tags: vec!["a".into(), "b".into()],
            local_media_address: "127.0.0.1".parse().unwrap(),
            output: PathBuf::from("out.wav"),
            duration: Duration::from_secs(1),
            sdp_session_id: 1,
            format: AudioFormat::pcmu_8k_20ms(),
        };

        let listeners = listener_configs(&request);
        std::env::remove_var(LISTENERS_ENV);

        assert_eq!(listeners.len(), 2);
        assert_eq!(listeners[0].0, "rtt");
        assert_eq!(
            listeners[0].1.url,
            format!("ws:{sep}10.0.0.1:9090{slash}ws", slash = "\x2f")
        );
        assert_eq!(listeners[0].1.stream_sid, "MZ-rtt");
        assert_eq!(listeners[0].1.call_sid, "call-7");
        assert_eq!(listeners[0].1.tracks, vec!["inbound", "outbound"]);
        assert_eq!(listeners[1].0, "recorder");
    }

    #[test]
    fn absent_call_id_means_no_spike_requested() {
        std::env::remove_var(CALL_ID_ENV);
        assert!(request_from_env(1).unwrap().is_none());
    }
}
