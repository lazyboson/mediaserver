use control_api::proto;
use control_api::proto::media_control_client::MediaControlClient;

const USAGE: &str = "\
mss_ctl <endpoint> create <external-id> <call-id> <from-tag|-> [rtpengine-node]
   a from-tag of - lets MSS resolve the call's participants from rtpengine
mss_ctl <endpoint> create <external-id> --kind mix --group <conference>
   creates the conference ROOM as a session with no leg: its clock is the
   conference's, and a record attachment on it records the room from the
   conference's open to its close, whoever comes and goes (D20)
mss_ctl <endpoint> inline <external-id> <call-id> <sdp-offer-file> [conference-group]
   creates an INLINE session: MSS binds an rtp socket, answers the offer and
   prints the answer sdp for the caller to put in its SIP dialog
mss_ctl <endpoint> describe <external-id>
mss_ctl <endpoint> attach <external-id> <ws-url> [label] [authoritative]
mss_ctl <endpoint> consume <external-id> [label] [track]
   a grpc-stream consumer with SINK and EVENTS: it subscribes on
   MediaStream.Subscribe and may report speech back
mss_ctl <endpoint> record <external-id> <account/recording.wav> [label] [group] [track]
   the endpoint is the frozen recording identity, and the object key
   a group joins this recording to the other members of that recording
   group on this pod, and then label names the participant's own file
   under account/recording/, with track one of customer|agent|all
mss_ctl <endpoint> pause <attachment-id> <true|false>
mss_ctl <endpoint> mix <attachment-id> <own|all|member-external-id> [include|exclude] [inject|leg]
   routes an INJECT attachment's audio inside its conference: own is private
   playback into its own leg, a member's external id is a whisper only that
   member hears, all is the barge flip; include|exclude decides whether the
   conference's mixed track carries it (default: include for a whisper or a
   barge, exclude for private playback); leg routes the member's OWN rtp
   instead of the injected audio, which is the coach-listen shape
mss_ctl <endpoint> member <attachment-id> <mute|deaf|hold|ttl> <on|off|ms> [more pairs]
   member verbs ride on any attachment of the member's session: mute silences
   that member everywhere, deaf silences the room into their ear, hold is both
   and leaves the playback path open for hold audio; ttl <ms> leases every flag
   set on in the same call, so MSS lifts it by itself if nothing refreshes it
   (0 or absent = it holds until an off, which is the pod default unless
   MSS_MEMBER_STATE_TTL_SECS says otherwise)
mss_ctl <endpoint> detach <attachment-id>
mss_ctl <endpoint> play <external-id> <wav-path> [target-tag]
   on an inline leg the target is own (its own ear, the default) or all
   (a prompt into the whole conference the leg is seated in)
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

fn flagged(args: &[String]) -> (std::collections::BTreeMap<String, String>, Vec<String>) {
    let mut flags = std::collections::BTreeMap::new();
    let mut positional = Vec::new();
    let mut at = 0;
    while at < args.len() {
        match args[at].strip_prefix("--") {
            Some(name) => {
                flags.insert(
                    name.to_string(),
                    args.get(at + 1).cloned().unwrap_or_default(),
                );
                at += 2;
            }
            None => {
                positional.push(args[at].clone());
                at += 1;
            }
        }
    }
    (flags, positional)
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
            let (flags, positional) = flagged(&args[2..]);
            let room = flags.get("kind").map(String::as_str) == Some("mix");
            let group = flags.get("group").cloned().unwrap_or_default();
            if positional.is_empty()
                || (room && group.is_empty())
                || (!room && positional.len() < 3)
            {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            let request = match room {
                true => proto::CreateSessionRequest {
                    external_id: positional[0].clone(),
                    kind: proto::SessionKind::Mix as i32,
                    call_id: String::new(),
                    from_tags: Vec::new(),
                    rtpengine_node: String::new(),
                    mix: false,
                    idempotency_key: String::new(),
                    sdp_offer: String::new(),
                    group,
                },
                false => proto::CreateSessionRequest {
                    external_id: positional[0].clone(),
                    kind: proto::SessionKind::Tap as i32,
                    call_id: positional[1].clone(),
                    from_tags: if positional[2] == "-" {
                        Vec::new()
                    } else {
                        positional[2].split(',').map(str::to_string).collect()
                    },
                    rtpengine_node: positional.get(3).cloned().unwrap_or_default(),
                    mix: false,
                    idempotency_key: String::new(),
                    sdp_offer: String::new(),
                    group,
                },
            };
            client
                .create_session(request)
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "inline" => {
            if args.len() < 5 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            let offer = std::fs::read_to_string(&args[4])?;
            client
                .create_session(proto::CreateSessionRequest {
                    external_id: args[2].clone(),
                    kind: proto::SessionKind::Inline as i32,
                    call_id: args[3].clone(),
                    from_tags: Vec::new(),
                    rtpengine_node: String::new(),
                    mix: false,
                    idempotency_key: String::new(),
                    sdp_offer: offer,
                    group: args.get(5).cloned().unwrap_or_default(),
                })
                .await
                .map(|response| response.into_inner().sdp_answer)
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
        "consume" => {
            if args.len() < 3 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            client
                .attach(proto::AttachRequest {
                    session: Some(reference(&args[2])),
                    transport: proto::Transport::GrpcStream as i32,
                    capabilities: vec![
                        proto::Capability::Sink as i32,
                        proto::Capability::Events as i32,
                    ],
                    selector: Some(track_selector(args.get(4))),
                    format: None,
                    authoritative: false,
                    label: args
                        .get(3)
                        .cloned()
                        .unwrap_or_else(|| "grpc-consumer".to_string()),
                    endpoint: String::new(),
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
                    metadata: Default::default(),
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "mix" => {
            if args.len() < 4 {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("mix_target".to_string(), args[3].clone());
            for qualifier in args.iter().skip(4) {
                let key = match qualifier.as_str() {
                    "include" | "exclude" => "mix_monitor",
                    "inject" | "leg" => "mix_source",
                    other => {
                        eprintln!("mix takes include|exclude and inject|leg, not {other:?}");
                        std::process::exit(2);
                    }
                };
                metadata.insert(key.to_string(), qualifier.clone());
            }
            client
                .update_attachment(proto::UpdateAttachmentRequest {
                    attachment_id: args[2].clone(),
                    paused: None,
                    selector: None,
                    format: None,
                    idempotency_key: String::new(),
                    metadata,
                })
                .await
                .map(|response| format!("{:?}", response.into_inner()))
        }
        "member" => {
            if args.len() < 5 || !(args.len() - 3).is_multiple_of(2) {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
            let mut metadata = std::collections::HashMap::new();
            for verb in args[3..].chunks(2) {
                let key = match verb[0].as_str() {
                    "mute" => "member_mute",
                    "deaf" => "member_deaf",
                    "hold" => "member_hold",
                    "ttl" => "member_state_ttl_ms",
                    other => {
                        eprintln!("member takes mute|deaf|hold|ttl, not {other:?}");
                        std::process::exit(2);
                    }
                };
                metadata.insert(key.to_string(), verb[1].clone());
            }
            client
                .update_attachment(proto::UpdateAttachmentRequest {
                    attachment_id: args[2].clone(),
                    paused: None,
                    selector: None,
                    format: None,
                    idempotency_key: String::new(),
                    metadata,
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
