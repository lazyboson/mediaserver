use control_api::proto;
use control_api::proto::media_control_client::MediaControlClient;

const USAGE: &str = "\
mss_ctl <endpoint> create <external-id> <call-id> <from-tag|-> [rtpengine-node]
   a from-tag of - lets MSS resolve the call's participants from rtpengine
mss_ctl <endpoint> describe <external-id>
mss_ctl <endpoint> attach <external-id> <ws-url> [label] [authoritative]
mss_ctl <endpoint> record <external-id> <account/recording.wav> [label] [group] [track]
   the endpoint is the frozen recording identity, and the object key
   a group joins this recording to the other members of that recording
   group on this pod, and then label names the participant's own file
   under account/recording/, with track one of customer|agent|all
mss_ctl <endpoint> pause <attachment-id> <true|false>
mss_ctl <endpoint> detach <attachment-id>
mss_ctl <endpoint> play <external-id> <wav-path> [target-tag]
mss_ctl <endpoint> destroy <external-id>";

fn track_selector(track: Option<&String>) -> proto::TrackSelector {
    let select = match track.map(String::as_str) {
        None | Some("all") | Some("") => proto::track_selector::Select::All(true),
        Some(only) => proto::track_selector::Select::Only(only.to_string()),
    };
    proto::TrackSelector {
        select: Some(select),
    }
}

fn reference(external_id: &str) -> proto::SessionRef {
    proto::SessionRef {
        id: Some(proto::session_ref::Id::ExternalId(external_id.to_string())),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let channel = control_api::tonic::transport::Endpoint::from_shared(args[0].clone())?
        .connect()
        .await?;
    let token = std::env::var("MSS_AUTH_TOKEN").ok();
    let mut client = MediaControlClient::with_interceptor(
        channel,
        move |mut request: control_api::tonic::Request<()>| {
            if let Some(token) = &token {
                let value = format!("Bearer {token}").parse().map_err(|_| {
                    control_api::tonic::Status::invalid_argument(
                        "MSS_AUTH_TOKEN is not a legal header value",
                    )
                })?;
                request.metadata_mut().insert("authorization", value);
            }
            Ok(request)
        },
    );

    let outcome = match args[1].as_str() {
        "create" => {
            if args.len() < 5 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            client
                .create_session(proto::CreateSessionRequest {
                    external_id: args[2].clone(),
                    kind: proto::SessionKind::Tap as i32,
                    call_id: args[3].clone(),
                    from_tags: if args[4] == "-" {
                        Vec::new()
                    } else {
                        args[4].split(',').map(str::to_string).collect()
                    },
                    rtpengine_node: args.get(5).cloned().unwrap_or_default(),
                    mix: false,
                    idempotency_key: String::new(),
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "describe" => client
            .describe_session(reference(&args[2]))
            .await
            .map(|response| format!("{:?}", response.into_inner())),
        "attach" => {
            if args.len() < 4 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            client
                .attach(proto::AttachRequest {
                    session: Some(reference(&args[2])),
                    transport: proto::Transport::WsTwilio as i32,
                    capabilities: vec![
                        proto::Capability::Sink as i32,
                        proto::Capability::Events as i32,
                    ],
                    selector: None,
                    format: None,
                    authoritative: args
                        .get(5)
                        .map(|flag| flag == "authoritative")
                        .unwrap_or(false),
                    label: args
                        .get(4)
                        .cloned()
                        .unwrap_or_else(|| "consumer".to_string()),
                    endpoint: args[3].clone(),
                    group: String::new(),
                    metadata: Default::default(),
                    idempotency_key: String::new(),
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "record" => {
            if args.len() < 4 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            client
                .attach(proto::AttachRequest {
                    session: Some(reference(&args[2])),
                    transport: proto::Transport::FileS3 as i32,
                    capabilities: vec![proto::Capability::Sink as i32],
                    selector: Some(track_selector(args.get(6))),
                    format: None,
                    authoritative: false,
                    label: args
                        .get(4)
                        .cloned()
                        .unwrap_or_else(|| "recorder".to_string()),
                    endpoint: args[3].clone(),
                    group: args.get(5).cloned().unwrap_or_default(),
                    metadata: Default::default(),
                    idempotency_key: String::new(),
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "pause" => {
            if args.len() < 4 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            client
                .update_attachment(proto::UpdateAttachmentRequest {
                    attachment_id: args[2].clone(),
                    paused: Some(args[3] == "true"),
                    selector: None,
                    format: None,
                    idempotency_key: String::new(),
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "detach" => client
            .detach(proto::AttachmentRef {
                attachment_id: args[2].clone(),
            })
            .await
            .map(|response| format!("{:?}", response.into_inner())),
        "play" => {
            if args.len() < 4 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            let blob = std::fs::read(&args[3])?;
            client
                .start_playback(proto::StartPlaybackRequest {
                    session: Some(reference(&args[2])),
                    source: Some(proto::start_playback_request::Source::Blob(blob)),
                    target_tag: args.get(4).cloned().unwrap_or_default(),
                    repeat_times: 0,
                    block_egress: false,
                    requested_by: String::new(),
                    idempotency_key: String::new(),
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "destroy" => client
            .destroy_session(reference(&args[2]))
            .await
            .map(|response| format!("{:?}", response.into_inner())),
        other => {
            eprintln!("unknown command {other}\n{USAGE}");
            std::process::exit(2);
        }
    };

    match outcome {
        Ok(message) => println!("ok: {message}"),
        Err(status) => {
            println!("{:?}: {}", status.code(), status.message());
            std::process::exit(1);
        }
    }
    Ok(())
}
