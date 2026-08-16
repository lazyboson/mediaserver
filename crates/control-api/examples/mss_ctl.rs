use control_api::proto;
use control_api::proto::media_control_client::MediaControlClient;

const USAGE: &str = "\
mss_ctl <endpoint> create <external-id> <call-id> <from-tag> [rtpengine-node]
mss_ctl <endpoint> describe <external-id>
mss_ctl <endpoint> attach <external-id> <ws-url> [label] [authoritative]
mss_ctl <endpoint> destroy <external-id>";

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
    let mut client = MediaControlClient::connect(args[0].clone()).await?;

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
                    from_tags: args[4].split(',').map(str::to_string).collect(),
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
                    metadata: Default::default(),
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
