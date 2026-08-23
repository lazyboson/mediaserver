use crate::hub::{Subscription, TapEvent};
use crate::inline_leg::InlineEgressHandle;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use media_core::{g711, AudioFormat, Track};
use protocol::twilio::{
    DtmfInfo, Inbound, MarkInfo, MediaFormat, MediaPayload, Outbound, StartInfo,
};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

const PCMU_ENCODING: &str = "PCMU";
const UTTERANCE_IDLE: Duration = Duration::from_millis(700);
const MARK_POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_PENDING_MARKS: usize = 64;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error("websocket: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("could not serialize the {0} event")]
    Serialize(&'static str),
}

#[derive(Debug, Clone)]
pub enum BridgeCommand {
    Speak(Vec<i16>),
    Barge,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsumerStats {
    pub media_sent: u64,
    pub dtmf_sent: u64,
    pub media_dropped: u64,
    pub inbound_media: u64,
    pub inbound_unknown_encoding: u64,
    pub utterances: u64,
    pub barges: u64,
    pub text_sent: u64,
    pub inject_samples: u64,
    pub inject_dropped: u64,
    pub marks_acked: u64,
}

pub struct InlineInject {
    egress: InlineEgressHandle,
    pending: VecDeque<(String, u64)>,
}

impl InlineInject {
    pub fn new(egress: InlineEgressHandle) -> InlineInject {
        InlineInject {
            egress,
            pending: VecDeque::new(),
        }
    }

    fn speak(&self, pcm: Vec<i16>) -> bool {
        self.egress.push(pcm)
    }

    fn mark(&mut self, name: String) {
        if self.pending.len() >= MAX_PENDING_MARKS {
            self.pending.pop_front();
        }
        let watermark = self.egress.watermark();
        self.pending.push_back((name, watermark));
    }

    fn barge(&mut self) {
        self.egress.clear();
        self.pending.clear();
    }

    fn drained(&mut self) -> Vec<String> {
        let drained = self.egress.drained_samples();
        let mut acked = Vec::new();
        while let Some((_, watermark)) = self.pending.front() {
            if *watermark > drained {
                break;
            }
            let (name, _) = self.pending.pop_front().expect("the front was just read");
            acked.push(name);
        }
        acked
    }
}

pub struct ConsumerConfig {
    pub url: String,
    pub account_id: String,
    pub call_sid: String,
    pub stream_sid: String,
    pub format: AudioFormat,
    pub tracks: Vec<String>,
    pub custom_parameters: HashMap<String, String>,
    pub egress: Option<InlineEgressHandle>,
}

pub fn track_name(track: Track) -> &'static str {
    match track {
        Track::Customer => "inbound",
        Track::Agent => "outbound",
        Track::Mixed => "mixed",
    }
}

pub async fn run(
    config: ConsumerConfig,
    mut events: Subscription,
    commands: Option<mpsc::Sender<BridgeCommand>>,
    mut text: Option<mpsc::Receiver<String>>,
) -> Result<ConsumerStats, ConsumerError> {
    let (stream, _) = tokio_tungstenite::connect_async(&config.url).await?;
    info!(url = %config.url, stream_sid = %config.stream_sid, "consumer websocket connected");
    let (mut writer, mut reader) = stream.split();

    let mut stats = ConsumerStats::default();
    let mut sequence: u64 = 1;
    let mut utterance: Vec<i16> = Vec::new();
    let mut idle_deadline: Option<Instant> = None;
    let mut inline = config.egress.clone().map(InlineInject::new);

    let start = Outbound::Start {
        sequence_number: sequence.to_string(),
        stream_sid: config.stream_sid.clone(),
        start: StartInfo {
            account_id: config.account_id.clone(),
            stream_sid: config.stream_sid.clone(),
            call_sid: config.call_sid.clone(),
            tracks: config.tracks.clone(),
            media_format: MediaFormat {
                encoding: PCMU_ENCODING.to_string(),
                sample_rate: config.format.sample_rate_hz,
                channels: 1,
            },
            custom_parameters: config.custom_parameters.clone(),
        },
    };
    writer.send(encode(&start, "start")?).await?;

    let mut mark_poll = tokio::time::interval(MARK_POLL_INTERVAL);
    mark_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let marks_pending = inline
            .as_ref()
            .is_some_and(|inject| !inject.pending.is_empty());
        tokio::select! {
            _ = mark_poll.tick(), if marks_pending => {
                let acked = match inline.as_mut() {
                    Some(inject) => inject.drained(),
                    None => Vec::new(),
                };
                for name in acked {
                    stats.marks_acked += 1;
                    let message = Outbound::Mark {
                        stream_sid: config.stream_sid.clone(),
                        mark: MarkInfo { name },
                    };
                    writer.send(encode(&message, "mark")?).await?;
                }
            }
            event = events.next() => {
                let Some(event) = event else { break };
                sequence += 1;
                let message = match event {
                    TapEvent::Media { track, timestamp_ms, len, samples } => {
                        stats.media_sent += 1;
                        let mut ulaw = [0u8; crate::hub::MAX_FRAME_SAMPLES];
                        let encoded = g711::encode_into(true, &samples[..len], &mut ulaw);
                        encode(&Outbound::Media {
                            sequence_number: sequence.to_string(),
                            stream_sid: config.stream_sid.clone(),
                            media: MediaPayload {
                                track: track_name(track).to_string(),
                                timestamp: timestamp_ms.to_string(),
                                payload: BASE64.encode(&ulaw[..encoded]),
                            },
                        }, "media")?
                    }
                    TapEvent::Dtmf { track, digit } => {
                        stats.dtmf_sent += 1;
                        encode(&Outbound::Dtmf {
                            sequence_number: sequence.to_string(),
                            stream_sid: config.stream_sid.clone(),
                            dtmf: DtmfInfo {
                                track: track_name(track).to_string(),
                                digit: digit.to_string(),
                            },
                        }, "dtmf")?
                    }
                };
                writer.send(message).await?;
            }
            _ = async {
                match idle_deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                flush_utterance(&mut utterance, &mut idle_deadline, &mut stats, commands.as_ref()).await;
            }
            outbound = next_text(&mut text) => {
                stats.text_sent += 1;
                writer.send(Message::text(outbound)).await?;
            }
            inbound = reader.next() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        handle_inbound(&text, &mut utterance, &mut idle_deadline, &mut stats, commands.as_ref(), inline.as_mut()).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                }
            }
        }
    }

    sequence += 1;
    let stop = Outbound::Stop {
        sequence_number: sequence.to_string(),
        stream_sid: config.stream_sid.clone(),
    };
    writer.send(encode(&stop, "stop")?).await?;
    writer.close().await.ok();

    stats.media_dropped = events.dropped_oldest();
    Ok(stats)
}

async fn next_text(text: &mut Option<mpsc::Receiver<String>>) -> String {
    match text.as_mut() {
        Some(receiver) => match receiver.recv().await {
            Some(json) => json,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

fn encode(message: &Outbound, kind: &'static str) -> Result<Message, ConsumerError> {
    serde_json::to_string(message)
        .map(Message::text)
        .map_err(|_| ConsumerError::Serialize(kind))
}

async fn flush_utterance(
    utterance: &mut Vec<i16>,
    idle_deadline: &mut Option<Instant>,
    stats: &mut ConsumerStats,
    commands: Option<&mpsc::Sender<BridgeCommand>>,
) {
    *idle_deadline = None;
    if utterance.is_empty() {
        return;
    }
    stats.utterances += 1;
    let speech = std::mem::take(utterance);
    if let Some(commands) = commands {
        if commands.send(BridgeCommand::Speak(speech)).await.is_err() {
            warn!("nothing is listening for bridge speech; the utterance was discarded");
        }
    }
}

async fn handle_inbound(
    text: &str,
    utterance: &mut Vec<i16>,
    idle_deadline: &mut Option<Instant>,
    stats: &mut ConsumerStats,
    commands: Option<&mpsc::Sender<BridgeCommand>>,
    inline: Option<&mut InlineInject>,
) {
    let Ok(message) = serde_json::from_str::<Inbound>(text) else {
        return;
    };
    match (message, inline) {
        (Inbound::Media { media, .. }, inline) => {
            if media
                .encoding
                .as_deref()
                .is_some_and(|encoding| encoding != PCMU_ENCODING)
            {
                stats.inbound_unknown_encoding += 1;
                return;
            }
            let Ok(bytes) = BASE64.decode(media.payload.as_bytes()) else {
                stats.inbound_unknown_encoding += 1;
                return;
            };
            let Some(inject) = inline else {
                stats.inbound_media += 1;
                utterance.extend(bytes.iter().map(|byte| g711::ulaw_to_linear(*byte)));
                *idle_deadline = Some(Instant::now() + UTTERANCE_IDLE);
                return;
            };
            let rate = inject.egress.format().sample_rate_hz;
            if media.sample_rate.is_some_and(|declared| declared != rate) {
                stats.inbound_unknown_encoding += 1;
                return;
            }
            stats.inbound_media += 1;
            let pcm: Vec<i16> = bytes
                .iter()
                .map(|byte| g711::ulaw_to_linear(*byte))
                .collect();
            let samples = pcm.len() as u64;
            if inject.speak(pcm) {
                stats.inject_samples += samples;
            } else {
                stats.inject_dropped += samples;
                warn!("the inline egress queue is full; an inbound media frame was dropped");
            }
        }
        (Inbound::Mark { mark, .. }, Some(inject)) => inject.mark(mark.name),
        (Inbound::Mark { .. }, None) => {
            flush_utterance(utterance, idle_deadline, stats, commands).await;
        }
        (Inbound::Clear { .. }, Some(inject)) => {
            stats.barges += 1;
            inject.barge();
        }
        (Inbound::Clear { .. }, None) => {
            utterance.clear();
            *idle_deadline = None;
            stats.barges += 1;
            if let Some(commands) = commands {
                commands.send(BridgeCommand::Barge).await.ok();
            }
        }
        (Inbound::EndOfInteraction { .. }, _) => {
            flush_utterance(utterance, idle_deadline, stats, commands).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    const URL_SCHEME_SEPARATOR: &str = "\x2f\x2f";

    fn pcm_frame(value: i16) -> Vec<i16> {
        vec![value; 160]
    }

    #[test]
    fn tracks_use_the_legacy_gateway_names() {
        assert_eq!(track_name(Track::Customer), "inbound");
        assert_eq!(track_name(Track::Agent), "outbound");
        assert_eq!(track_name(Track::Mixed), "mixed");
    }

    #[test]
    fn media_events_carry_the_pcm_the_pipeline_released() {
        match TapEvent::media(Track::Customer, 40, &pcm_frame(0)) {
            TapEvent::Media {
                track,
                timestamp_ms,
                len,
                samples,
            } => {
                assert_eq!(track, Track::Customer);
                assert_eq!(timestamp_ms, 40);
                assert_eq!(len, 160);
                assert_eq!(samples[..len], pcm_frame(0));
            }
            _ => panic!("expected media"),
        }
    }

    #[tokio::test]
    async fn start_media_and_stop_reach_the_bridge_in_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut seen = Vec::new();
            while let Some(Ok(message)) = ws.next().await {
                if let Message::Text(text) = message {
                    seen.push(text.to_string());
                    if seen.len() == 3 {
                        break;
                    }
                }
            }
            seen
        });

        let (mut hub, client) = crate::hub::Hub::new();
        let subscription = client.attach(8, crate::hub::TrackSelection::All).unwrap();
        hub.poll_commands();
        let config = ConsumerConfig {
            url: format!("ws:{}127.0.0.1:{port}", URL_SCHEME_SEPARATOR),
            account_id: "acct-1".into(),
            call_sid: "call-1".into(),
            stream_sid: "MZ-1".into(),
            format: AudioFormat::pcmu_8k_20ms(),
            tracks: vec!["inbound".into(), "outbound".into()],
            custom_parameters: HashMap::new(),
            egress: None,
        };
        let consumer = tokio::spawn(run(config, subscription, None, None));

        hub.publish(TapEvent::media(Track::Agent, 20, &pcm_frame(0)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(hub);

        let seen = server.await.unwrap();
        let stats = consumer.await.unwrap().unwrap();

        assert!(seen[0].contains(r#""event":"start""#), "{:?}", seen[0]);
        assert!(seen[0].contains(r#""encoding":"PCMU""#), "{:?}", seen[0]);
        assert!(seen[1].contains(r#""event":"media""#), "{:?}", seen[1]);
        assert!(seen[1].contains(r#""track":"outbound""#), "{:?}", seen[1]);
        assert!(seen[1].contains(r#""sequenceNumber":"2""#), "{:?}", seen[1]);
        assert!(seen[1].contains(r#""timestamp":"20""#), "{:?}", seen[1]);
        assert!(seen[2].contains(r#""event":"stop""#), "{:?}", seen[2]);
        assert_eq!(stats.media_sent, 1);
        assert_eq!(stats.media_dropped, 0);
    }

    #[tokio::test]
    async fn send_text_reaches_the_far_end_verbatim() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut seen = Vec::new();
            while let Some(Ok(message)) = ws.next().await {
                if let Message::Text(text) = message {
                    seen.push(text.to_string());
                    if seen.len() == 3 {
                        break;
                    }
                }
            }
            seen
        });

        let (mut hub, client) = crate::hub::Hub::new();
        let subscription = client.attach(8, crate::hub::TrackSelection::All).unwrap();
        hub.poll_commands();
        let config = ConsumerConfig {
            url: format!("ws:{}127.0.0.1:{port}", URL_SCHEME_SEPARATOR),
            account_id: "acct-1".into(),
            call_sid: "call-1".into(),
            stream_sid: "MZ-1".into(),
            format: AudioFormat::pcmu_8k_20ms(),
            tracks: vec!["inbound".into()],
            custom_parameters: HashMap::new(),
            egress: None,
        };
        let (text, inbound) = mpsc::channel(4);
        let consumer = tokio::spawn(run(config, subscription, None, Some(inbound)));

        let sent = r#"{"event":"send_text","text":"hold please"}"#;
        text.send(sent.to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(hub);

        let seen = server.await.unwrap();
        let stats = consumer.await.unwrap().unwrap();

        assert!(seen[0].contains(r#""event":"start""#), "{:?}", seen[0]);
        assert_eq!(seen[1], sent);
        assert!(seen[2].contains(r#""event":"stop""#), "{:?}", seen[2]);
        assert_eq!(stats.text_sent, 1);
    }

    #[tokio::test]
    async fn inbound_media_accumulates_until_a_mark_completes_the_utterance() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut utterance = Vec::new();
        let mut idle = None;
        let mut stats = ConsumerStats::default();
        let payload = BASE64.encode([0xFFu8, 0x7F, 0x00]);

        let frame = format!(r#"{{"event":"media","media":{{"payload":"{payload}"}}}}"#);
        handle_inbound(
            &frame,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            None,
        )
        .await;
        handle_inbound(
            &frame,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            None,
        )
        .await;
        assert_eq!(stats.inbound_media, 2);
        assert!(rx.try_recv().is_err());

        handle_inbound(
            r#"{"event":"mark","mark":{"name":"utterance"}}"#,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            None,
        )
        .await;
        match rx.try_recv().unwrap() {
            BridgeCommand::Speak(pcm) => assert_eq!(pcm.len(), 6),
            other => panic!("expected speech, got {other:?}"),
        }
        assert_eq!(stats.utterances, 1);
        assert!(utterance.is_empty());
    }

    #[tokio::test]
    async fn silence_completes_an_utterance_because_the_bridge_never_sends_a_mark() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut utterance = Vec::new();
        let mut idle = None;
        let mut stats = ConsumerStats::default();
        let payload = BASE64.encode([0xFFu8, 0x7F]);
        let frame =
            format!(r#"{{"event":"media","media":{{"encoding":"PCMU","payload":"{payload}"}}}}"#);

        handle_inbound(
            &frame,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            None,
        )
        .await;
        assert!(idle.is_some());
        assert!(rx.try_recv().is_err());

        tokio::time::sleep(UTTERANCE_IDLE + Duration::from_millis(50)).await;
        if let Some(at) = idle {
            tokio::time::sleep_until(at).await;
        }
        flush_utterance(&mut utterance, &mut idle, &mut stats, Some(&tx)).await;

        assert!(matches!(rx.try_recv().unwrap(), BridgeCommand::Speak(_)));
        assert_eq!(stats.utterances, 1);
        assert!(idle.is_none());
    }

    #[tokio::test]
    async fn clear_barges_and_discards_the_half_built_utterance() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut utterance = vec![1i16, 2, 3];
        let mut idle = None;
        let mut stats = ConsumerStats::default();

        handle_inbound(
            r#"{"event":"clear","streamSid":"MZ-1"}"#,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            None,
        )
        .await;

        assert!(matches!(rx.try_recv().unwrap(), BridgeCommand::Barge));
        assert!(utterance.is_empty());
        assert_eq!(stats.barges, 1);
    }

    #[tokio::test]
    async fn an_encoding_we_cannot_decode_is_counted_not_played() {
        let (tx, _rx) = mpsc::channel(4);
        let mut utterance = Vec::new();
        let mut idle = None;
        let mut stats = ConsumerStats::default();

        handle_inbound(
            r#"{"event":"media","media":{"encoding":"audio/opus","payload":"AAAA"}}"#,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            None,
        )
        .await;

        assert_eq!(stats.inbound_unknown_encoding, 1);
        assert_eq!(stats.inbound_media, 0);
        assert!(utterance.is_empty());
    }

    fn inline_pair() -> (
        crate::inline_leg::InlineEgress,
        InlineInject,
        std::net::UdpSocket,
    ) {
        let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_nonblocking(true).unwrap();
        let ours = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        ours.set_nonblocking(true).unwrap();
        let (egress, handle) = crate::inline_leg::InlineEgress::bind(
            ours,
            peer.local_addr().unwrap(),
            AudioFormat::pcmu_8k_20ms(),
            0,
            0x4321,
            std::time::Instant::now(),
        )
        .unwrap();
        (egress, InlineInject::new(handle), peer)
    }

    fn inbound_media_frame(samples: usize) -> String {
        let payload = BASE64.encode(vec![g711::linear_to_ulaw(4_000); samples]);
        format!(r#"{{"event":"media","media":{{"payload":"{payload}"}}}}"#)
    }

    fn payloads(peer: &std::net::UdpSocket) -> Vec<Vec<u8>> {
        let mut seen = Vec::new();
        let mut buf = [0u8; 2048];
        while let Ok((len, _)) = peer.recv_from(&mut buf) {
            let packet = media_core::rtp::RtpPacket::parse(&buf[..len]).unwrap();
            seen.push(packet.payload.to_vec());
        }
        seen
    }

    #[tokio::test]
    async fn inbound_media_reaches_an_inline_leg_at_once_instead_of_becoming_an_utterance() {
        let (mut egress, mut inject, peer) = inline_pair();
        let (tx, mut rx) = mpsc::channel(4);
        let mut utterance = Vec::new();
        let mut idle = None;
        let mut stats = ConsumerStats::default();

        handle_inbound(
            &inbound_media_frame(160),
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            Some(&mut inject),
        )
        .await;

        assert_eq!(stats.inbound_media, 1);
        assert_eq!(stats.inject_samples, 160);
        assert!(
            utterance.is_empty(),
            "an inline leg is fed frame by frame, never buffered into an utterance"
        );
        assert!(idle.is_none(), "there is no utterance to time out");
        assert!(rx.try_recv().is_err(), "no playback command is issued");

        let epoch = std::time::Instant::now();
        egress.pump(epoch);
        egress.pump(epoch + Duration::from_millis(20));
        let heard = payloads(&peer);
        assert_eq!(heard.len(), 2);
        assert_eq!(heard[0][0], g711::linear_to_ulaw(4_000));
    }

    #[tokio::test]
    async fn a_clear_flushes_the_inline_egress_so_the_next_paced_frame_is_silence() {
        let (mut egress, mut inject, peer) = inline_pair();
        let (tx, _rx) = mpsc::channel(4);
        let mut utterance = Vec::new();
        let mut idle = None;
        let mut stats = ConsumerStats::default();

        handle_inbound(
            &inbound_media_frame(1_600),
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            Some(&mut inject),
        )
        .await;
        let epoch = std::time::Instant::now();
        egress.pump(epoch);
        let _ = payloads(&peer);

        handle_inbound(
            r#"{"event":"clear","streamSid":"MZ-1"}"#,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            Some(&mut inject),
        )
        .await;
        egress.pump(epoch + Duration::from_millis(20));

        assert_eq!(stats.barges, 1);
        let heard = payloads(&peer);
        assert_eq!(heard.len(), 1);
        assert_eq!(heard[0][0], g711::linear_to_ulaw(0));
    }

    #[tokio::test]
    async fn a_mark_is_acked_only_once_its_audio_has_drained_out_of_the_egress() {
        let (mut egress, mut inject, _peer) = inline_pair();
        let (tx, _rx) = mpsc::channel(4);
        let mut utterance = Vec::new();
        let mut idle = None;
        let mut stats = ConsumerStats::default();

        handle_inbound(
            &inbound_media_frame(320),
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            Some(&mut inject),
        )
        .await;
        handle_inbound(
            r#"{"event":"mark","mark":{"name":"prompt-done"}}"#,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
            Some(&mut inject),
        )
        .await;

        assert!(
            inject.drained().is_empty(),
            "the mark waits while its audio is still queued"
        );
        let epoch = std::time::Instant::now();
        let mut acked = Vec::new();
        for tick in 0..6 {
            egress.pump(epoch + Duration::from_millis(tick * 20));
            acked.extend(inject.drained());
        }
        assert_eq!(acked, vec!["prompt-done".to_string()]);
    }
}
