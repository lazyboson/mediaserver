use control_api::proto;
use control_api::proto::media_control_client::MediaControlClient;
use control_api::proto::media_stream_client::MediaStreamClient;
use control_api::proto::{consumer_to_server, server_to_consumer};
use media_core::g711;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const USAGE: &str = "\
mss_stream_probe <endpoint> <external-id> <out.wav> [seconds]

Attaches a grpc-stream consumer to a session that is already tapped, dials the
MediaStream data plane with a ConsumerHello, decodes the frames it is served
and writes them as one wav per track.

env: MSS_AUTH_TOKEN          bearer for MediaControl and the hello token
     MSS_PROBE_TRACKS        customer (default) | agent | mixed | all
     MSS_PROBE_ENCODING      l16 (default) | pcmu | pcma
     MSS_PROBE_RATE          8000 | 16000 (default) | 48000
     MSS_PROBE_LABEL         attachment label, default grpc-probe
     MSS_PROBE_KEEP          set to keep the attachment instead of detaching";

const OUTBOUND_DEPTH: usize = 8;

struct Heard {
    samples: Vec<i16>,
    frames: u64,
    bytes: u64,
}

impl Heard {
    fn new() -> Heard {
        Heard {
            samples: Vec::new(),
            frames: 0,
            bytes: 0,
        }
    }
}

fn selector_of(tracks: &str) -> Result<proto::TrackSelector, String> {
    let select = match tracks {
        "all" => proto::track_selector::Select::All(true),
        "customer" | "agent" | "mixed" => proto::track_selector::Select::Only(tracks.to_string()),
        other => return Err(format!("unknown track selection {other}")),
    };
    Ok(proto::TrackSelector {
        select: Some(select),
    })
}

fn encoding_of(name: &str) -> Result<proto::Encoding, String> {
    match name {
        "l16" => Ok(proto::Encoding::L16),
        "pcmu" => Ok(proto::Encoding::Pcmu),
        "pcma" => Ok(proto::Encoding::Pcma),
        other => Err(format!(
            "unknown encoding {other}; l16, pcmu and pcma are served"
        )),
    }
}

fn decode(encoding: proto::Encoding, payload: &[u8], out: &mut Vec<i16>) -> Result<(), String> {
    match encoding {
        proto::Encoding::L16 => {
            if !payload.len().is_multiple_of(2) {
                return Err("an L16 frame arrived with a half sample in it".to_string());
            }
            out.extend(
                payload
                    .chunks_exact(2)
                    .map(|pair| i16::from_le_bytes([pair[0], pair[1]])),
            );
            Ok(())
        }
        proto::Encoding::Pcmu => {
            out.extend(payload.iter().map(|byte| g711::ulaw_to_linear(*byte)));
            Ok(())
        }
        proto::Encoding::Pcma => {
            out.extend(payload.iter().map(|byte| g711::alaw_to_linear(*byte)));
            Ok(())
        }
        other => Err(format!("this probe cannot decode {other:?}")),
    }
}

fn rms(samples: &[i16]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum: f64 = samples
        .iter()
        .map(|sample| f64::from(*sample) * f64::from(*sample))
        .sum();
    (sum / samples.len() as f64).sqrt()
}

fn peak(samples: &[i16]) -> i32 {
    samples
        .iter()
        .map(|sample| i32::from(*sample).abs())
        .max()
        .unwrap_or(0)
}

fn track_path(base: &Path, track: &str, single: bool) -> PathBuf {
    if single {
        return base.to_path_buf();
    }
    let stem = base
        .file_stem()
        .map(|stem| stem.to_string_lossy().to_string())
        .unwrap_or_else(|| "tap".to_string());
    let extension = base
        .extension()
        .map(|extension| extension.to_string_lossy().to_string())
        .unwrap_or_else(|| "wav".to_string());
    base.with_file_name(format!("{stem}-{track}.{extension}"))
}

fn write_wav(path: &Path, sample_rate_hz: u32, samples: &[i16]) -> Result<(), String> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut writer =
        hound::WavWriter::create(path, spec).map_err(|error| format!("wav create: {error}"))?;
    for sample in samples {
        writer
            .write_sample(*sample)
            .map_err(|error| format!("wav body: {error}"))?;
    }
    writer
        .finalize()
        .map_err(|error| format!("wav finalize: {error}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let endpoint = args[0].clone();
    let external_id = args[1].clone();
    let out_path = PathBuf::from(&args[2]);
    let seconds: u64 = args
        .get(3)
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);

    let token = std::env::var("MSS_AUTH_TOKEN").unwrap_or_default();
    let tracks = std::env::var("MSS_PROBE_TRACKS").unwrap_or_else(|_| "customer".to_string());
    let encoding =
        encoding_of(&std::env::var("MSS_PROBE_ENCODING").unwrap_or_else(|_| "l16".to_string()))?;
    let requested_rate: u32 = std::env::var("MSS_PROBE_RATE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16_000);
    let label = std::env::var("MSS_PROBE_LABEL").unwrap_or_else(|_| "grpc-probe".to_string());
    let keep = std::env::var("MSS_PROBE_KEEP").is_ok();
    let selector = selector_of(&tracks)?;
    let encoding_name = format!("{encoding:?}").to_lowercase();
    let wanted_format = proto::AudioFormat {
        encoding: encoding as i32,
        sample_rate_hz: requested_rate,
        channels: 1,
        ptime_ms: 20,
    };

    let channel = control_api::tonic::transport::Endpoint::from_shared(endpoint.clone())?
        .connect()
        .await?;
    let bearer = token.clone();
    let mut control = MediaControlClient::with_interceptor(
        channel,
        move |mut request: control_api::tonic::Request<()>| {
            if !bearer.is_empty() {
                let value = format!("Bearer {bearer}").parse().map_err(|_| {
                    control_api::tonic::Status::invalid_argument(
                        "MSS_AUTH_TOKEN is not a legal header value",
                    )
                })?;
                request.metadata_mut().insert("authorization", value);
            }
            Ok(request)
        },
    );

    let attached = control
        .attach(proto::AttachRequest {
            session: Some(proto::SessionRef {
                id: Some(proto::session_ref::Id::ExternalId(external_id.clone())),
            }),
            transport: proto::Transport::GrpcStream as i32,
            capabilities: vec![
                proto::Capability::Sink as i32,
                proto::Capability::Events as i32,
            ],
            selector: Some(selector),
            format: Some(wanted_format),
            authoritative: false,
            label: label.clone(),
            endpoint: String::new(),
            metadata: [("streamSid".to_string(), format!("MZ-{label}"))]
                .into_iter()
                .collect(),
            idempotency_key: String::new(),
        })
        .await?
        .into_inner();
    println!(
        "probe: attached {} to {external_id} as {tracks} in {encoding_name} at {requested_rate} Hz",
        attached.attachment_id
    );

    let mut stream = MediaStreamClient::connect(endpoint.clone()).await?;
    let (to_server, outbound) = mpsc::channel(OUTBOUND_DEPTH);
    to_server
        .send(proto::ConsumerToServer {
            msg: Some(consumer_to_server::Msg::Hello(proto::ConsumerHello {
                attachment_id: attached.attachment_id.clone(),
                token: token.clone(),
                requested_format: Some(wanted_format),
            })),
        })
        .await?;
    let mut inbound = stream
        .subscribe(ReceiverStream::new(outbound))
        .await?
        .into_inner();

    let mut heard: BTreeMap<String, Heard> = BTreeMap::new();
    let mut digits: Vec<String> = Vec::new();
    let mut texts: u64 = 0;
    let mut wav_rate = requested_rate;
    let mut ended = "the run window ended".to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            message = inbound.message() => match message {
                Ok(Some(frame)) => match frame.msg {
                    Some(server_to_consumer::Msg::Start(start)) => {
                        if let Some(served) = start.format {
                            wav_rate = served.sample_rate_hz;
                            println!(
                                "probe: stream started session={} sid={} tracks={:?} rate={} encoding={:?}",
                                start.session_id,
                                start.stream_sid,
                                start.tracks,
                                served.sample_rate_hz,
                                proto::Encoding::try_from(served.encoding)
                            );
                        }
                    }
                    Some(server_to_consumer::Msg::Frame(audio)) => {
                        let entry = heard.entry(audio.track.clone()).or_insert_with(Heard::new);
                        entry.frames += 1;
                        entry.bytes += audio.payload.len() as u64;
                        decode(encoding, &audio.payload, &mut entry.samples)?;
                    }
                    Some(server_to_consumer::Msg::Dtmf(dtmf)) => {
                        println!("probe: dtmf {} on {}", dtmf.digit, dtmf.track);
                        digits.push(dtmf.digit);
                    }
                    Some(server_to_consumer::Msg::Text(text)) => {
                        texts += 1;
                        println!("probe: text {}", text.json);
                    }
                    Some(server_to_consumer::Msg::Stop(stop)) => {
                        ended = format!("the server stopped the stream: {}", stop.reason);
                        break;
                    }
                    None => println!("probe: a server message carried no payload"),
                },
                Ok(None) => {
                    ended = "the server ended the stream".to_string();
                    break;
                }
                Err(status) => {
                    ended = format!("{:?}: {}", status.code(), status.message());
                    break;
                }
            },
        }
    }
    drop(to_server);
    println!("probe: {ended}");

    let single = heard.len() <= 1;
    if heard.is_empty() {
        println!("probe: no audio arrived, so no wav was written");
    }
    for (track, held) in &heard {
        let path = track_path(&out_path, track, single);
        write_wav(&path, wav_rate, &held.samples)?;
        println!(
            "probe: {} track={track} frames={} bytes={} samples={} seconds={:.2} rms={:.1} peak={}",
            path.display(),
            held.frames,
            held.bytes,
            held.samples.len(),
            held.samples.len() as f64 / f64::from(wav_rate),
            rms(&held.samples),
            peak(&held.samples)
        );
    }
    println!("probe: dtmf={digits:?} text_frames={texts}");

    if keep {
        println!("probe: keeping attachment {}", attached.attachment_id);
        return Ok(());
    }
    control
        .detach(proto::AttachmentRef {
            attachment_id: attached.attachment_id.clone(),
        })
        .await?;
    println!("probe: detached {}", attached.attachment_id);
    Ok(())
}
