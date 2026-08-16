use crate::hub::{Subscription, TapEvent};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use media_core::{g711, AudioFormat, Track};
use protocol::twilio::{DtmfInfo, Inbound, MediaFormat, MediaPayload, Outbound, StartInfo};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

const PCMU_ENCODING: &str = "PCMU";
const UTTERANCE_IDLE: Duration = Duration::from_millis(700);

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
}

pub struct ConsumerConfig {
    pub url: String,
    pub account_id: String,
    pub call_sid: String,
    pub stream_sid: String,
    pub format: AudioFormat,
    pub tracks: Vec<String>,
    pub custom_parameters: HashMap<String, String>,
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

    loop {
        tokio::select! {
            event = events.next() => {
                let Some(event) = event else { break };
                sequence += 1;
                let message = match event {
                    TapEvent::Media { track, timestamp_ms, len, bytes } => {
                        stats.media_sent += 1;
                        encode(&Outbound::Media {
                            sequence_number: sequence.to_string(),
                            stream_sid: config.stream_sid.clone(),
                            media: MediaPayload {
                                track: track_name(track).to_string(),
                                timestamp: timestamp_ms.to_string(),
                                payload: BASE64.encode(&bytes[..len]),
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
                        handle_inbound(&text, &mut utterance, &mut idle_deadline, &mut stats, commands.as_ref()).await;
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
) {
    let Ok(message) = serde_json::from_str::<Inbound>(text) else {
        return;
    };
    match message {
        Inbound::Media { media, .. } => {
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
            stats.inbound_media += 1;
            utterance.extend(bytes.iter().map(|byte| g711::ulaw_to_linear(*byte)));
            *idle_deadline = Some(Instant::now() + UTTERANCE_IDLE);
        }
        Inbound::Mark { .. } => {
            flush_utterance(utterance, idle_deadline, stats, commands).await;
        }
        Inbound::Clear { .. } => {
            utterance.clear();
            *idle_deadline = None;
            stats.barges += 1;
            if let Some(commands) = commands {
                commands.send(BridgeCommand::Barge).await.ok();
            }
        }
        Inbound::EndOfInteraction { .. } => {
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
    fn tracks_use_the_the legacy media gateway_names() {
        assert_eq!(track_name(Track::Customer), "inbound");
        assert_eq!(track_name(Track::Agent), "outbound");
        assert_eq!(track_name(Track::Mixed), "mixed");
    }

    #[test]
    fn media_events_carry_ulaw_encoded_frames() {
        match TapEvent::media(Track::Customer, 40, &pcm_frame(0), true) {
            TapEvent::Media {
                track,
                timestamp_ms,
                len,
                bytes,
            } => {
                assert_eq!(track, Track::Customer);
                assert_eq!(timestamp_ms, 40);
                assert_eq!(len, 160);
                assert_eq!(bytes[0], g711::linear_to_ulaw(0));
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
        };
        let consumer = tokio::spawn(run(config, subscription, None, None));

        hub.publish(TapEvent::media(Track::Agent, 20, &pcm_frame(0), true));
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
        handle_inbound(&frame, &mut utterance, &mut idle, &mut stats, Some(&tx)).await;
        handle_inbound(&frame, &mut utterance, &mut idle, &mut stats, Some(&tx)).await;
        assert_eq!(stats.inbound_media, 2);
        assert!(rx.try_recv().is_err());

        handle_inbound(
            r#"{"event":"mark","mark":{"name":"utterance"}}"#,
            &mut utterance,
            &mut idle,
            &mut stats,
            Some(&tx),
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

        handle_inbound(&frame, &mut utterance, &mut idle, &mut stats, Some(&tx)).await;
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
        )
        .await;

        assert_eq!(stats.inbound_unknown_encoding, 1);
        assert_eq!(stats.inbound_media, 0);
        assert!(utterance.is_empty());
    }
}
