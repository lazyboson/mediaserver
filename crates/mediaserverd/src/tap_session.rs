use crate::ng_transport::{NgTransport, NgTransportConfig, TransportError};
use crate::tap_spike::{capture, write_wav, SpikeError, TapLeg};
use media_core::{AudioFormat, Track};
use rtpengine_ng::{SdpError, SubscribeRequest, SubscriptionAnswer, SubscriptionOffer};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{info, warn};

const NODE_ENV: &str = "MSS_RTPENGINE_NODE";
const CALL_ID_ENV: &str = "MSS_TAP_CALL_ID";
const FROM_TAGS_ENV: &str = "MSS_TAP_FROM_TAGS";
const LOCAL_IP_ENV: &str = "MSS_TAP_LOCAL_IP";
const OUTPUT_ENV: &str = "MSS_TAP_OUTPUT";
const SECONDS_ENV: &str = "MSS_TAP_SECONDS";

const DEFAULT_OUTPUT: &str = "tap.wav";
const DEFAULT_SECONDS: u64 = 30;
const TARGET_DEPTH_PACKETS: u16 = 3;
const MAX_TAPPED_STREAMS: usize = 2;

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
    let transport = NgTransport::bind(
        SocketAddr::from(([0, 0, 0, 0], 0)),
        request.node,
        NgTransportConfig::default(),
        cookie_prefix,
    )
    .await?;

    let subscribe = SubscribeRequest {
        call_id: request.call_id.clone(),
        from_tags: request.from_tags.clone(),
        mix: false,
        accept_codecs: vec![request.format.encoding.rtpmap_name().to_string()],
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

    let mut legs = Vec::with_capacity(sockets.len());
    for (index, socket) in sockets.into_iter().enumerate() {
        let telephone_event_payload_type = offer
            .streams
            .get(index)
            .and_then(|stream| stream.telephone_event())
            .map(|event| event.payload_type);
        legs.push(TapLeg::new(
            track_for_stream(index),
            socket,
            request.format,
            TARGET_DEPTH_PACKETS,
            telephone_event_payload_type,
            request.duration,
        )?);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let capture_stop = Arc::clone(&stop);
    let format = request.format;
    let duration = request.duration;
    let capture_thread = std::thread::Builder::new()
        .name("mss-tap-spike".to_string())
        .spawn(move || {
            let summary = capture(&mut legs, format, duration, &capture_stop);
            (legs, summary)
        })?;

    tokio::select! {
        _ = tokio::time::sleep(duration) => info!("tap spike reached its configured duration"),
        _ = tokio::signal::ctrl_c() => info!("shutdown signal during tap spike"),
    }
    stop.store(true, Ordering::Relaxed);

    let (legs, summary) = tokio::task::spawn_blocking(move || capture_thread.join())
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

    let wav = write_wav(&request.output, request.format, &legs)?;
    info!(
        output = %request.output.display(),
        channels = wav.channels,
        frames = wav.frames,
        releases = summary.releases,
        reanchors = summary.reanchors,
        elapsed_ms = summary.elapsed.as_millis() as u64,
        "tap spike wrote its wav artifact"
    );
    Ok(())
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
    fn absent_call_id_means_no_spike_requested() {
        std::env::remove_var(CALL_ID_ENV);
        assert!(request_from_env(1).unwrap().is_none());
    }
}
