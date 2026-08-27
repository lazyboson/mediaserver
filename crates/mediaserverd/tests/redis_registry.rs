use std::time::Duration;

const URL_ENV: &str = "MSS_TEST_REDIS_URL";

fn url() -> Option<String> {
    std::env::var(URL_ENV).ok()
}

fn namespace(test: &str) -> String {
    format!("mss-itest-{test}")
}

async fn wipe(url: &str, external_id: &str) {
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: Result<(), _> = redis::pipe()
        .del(format!("mss-itest-{external_id}:session:{external_id}"))
        .ignore()
        .del(format!("mss-itest-{external_id}:lease:{external_id}"))
        .ignore()
        .srem(format!("mss-itest-{external_id}:sessions"), external_id)
        .ignore()
        .query_async::<()>(&mut connection)
        .await;
}

async fn lease_ttl(url: &str, external_id: &str) -> i64 {
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("TTL")
        .arg(format!("mss-itest-{external_id}:lease:{external_id}"))
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn expire_now(url: &str, external_id: &str) {
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("DEL")
        .arg(format!("mss-itest-{external_id}:lease:{external_id}"))
        .query_async(&mut connection)
        .await
        .unwrap();
}

#[allow(dead_code)]
#[path = "../src/session_store.rs"]
mod session_store;

use session_store::{
    GroupJoin, GroupRecord, PersistedAttachment, PersistedFormat, PersistedSession,
    RedisSessionStore, SessionStore,
};
use std::collections::BTreeMap;

fn session(external_id: &str, owner: &str) -> PersistedSession {
    PersistedSession {
        external_id: external_id.to_string(),
        kind: 1,
        call_id: "call-redis".to_string(),
        from_tags: vec!["from-a".to_string(), "from-b".to_string()],
        rtpengine_node: "10.0.0.5:22222".to_string(),
        owner: owner.to_string(),
        subscription_tag: "tap-tag-redis".to_string(),
        attachments: vec![PersistedAttachment {
            label: "rtt".to_string(),
            transport: 1,
            endpoint: "wss-rtt-endpoint".to_string(),
            capabilities: vec![1, 2],
            selector: Some("customer".to_string()),
            authoritative: true,
            paused: true,
            group: "conf-redis".to_string(),
            format: Some(PersistedFormat {
                encoding: 3,
                sample_rate_hz: 16_000,
                channels: 1,
                ptime_ms: 20,
            }),
            recording: None,
            metadata: BTreeMap::from([("accountId".to_string(), "acct-1".to_string())]),
        }],
    }
}

#[tokio::test]
async fn a_session_round_trips_through_redis_without_losing_what_rebuilds_it() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-roundtrip";
    wipe(&url, external_id).await;

    let store = RedisSessionStore::connect_in(&url, &namespace(external_id))
        .await
        .unwrap();
    let original = session(external_id, "pod-a");
    store.upsert(&original).await.unwrap();

    expire_now(&url, external_id).await;
    let claimed = store.claim_unleased("pod-b", 8).await.unwrap();
    let restored = claimed
        .into_iter()
        .find(|held| held.external_id == external_id)
        .expect("the session should be adoptable once its lease expired");

    assert_eq!(restored.call_id, original.call_id);
    assert_eq!(restored.from_tags, original.from_tags);
    assert_eq!(restored.rtpengine_node, original.rtpengine_node);
    assert_eq!(restored.owner, "pod-b");
    assert_eq!(
        restored.subscription_tag, original.subscription_tag,
        "without the tap's to-tag the adopter cannot cancel the dead pod's subscription"
    );
    assert_eq!(restored.attachments, original.attachments);

    wipe(&url, external_id).await;
}

#[tokio::test]
async fn the_lease_carries_a_ttl_so_a_dead_pod_releases_its_sessions() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-ttl";
    wipe(&url, external_id).await;

    let store = RedisSessionStore::connect_in(&url, &namespace(external_id))
        .await
        .unwrap();
    store.upsert(&session(external_id, "pod-a")).await.unwrap();

    let ttl = lease_ttl(&url, external_id).await;
    assert!(
        ttl > 0 && ttl <= session_store::LEASE_TTL.as_secs() as i64,
        "lease ttl was {ttl}; a lease without expiry would strand the session forever"
    );

    wipe(&url, external_id).await;
}

#[tokio::test]
async fn two_pods_racing_for_one_orphan_produce_exactly_one_owner() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-race";
    wipe(&url, external_id).await;

    let seeder = RedisSessionStore::connect_in(&url, &namespace(external_id))
        .await
        .unwrap();
    seeder.upsert(&session(external_id, "pod-a")).await.unwrap();
    expire_now(&url, external_id).await;

    let contenders = 6;
    let mut winners = Vec::new();
    let mut tasks = Vec::new();
    for index in 0..contenders {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let store = RedisSessionStore::connect_in(&url, "mss-itest-req-race")
                .await
                .unwrap();
            let claimed = store
                .claim_unleased(&format!("pod-{index}"), 8)
                .await
                .unwrap();
            claimed
                .into_iter()
                .filter(|held| held.external_id == "req-race")
                .count()
        }));
    }
    for task in tasks {
        winners.push(task.await.unwrap());
    }

    assert_eq!(
        winners.iter().sum::<usize>(),
        1,
        "{contenders} pods raced and {:?} of them adopted the same call, which would tap it twice",
        winners
    );

    wipe(&url, external_id).await;
}

#[tokio::test]
async fn renewal_is_refused_for_a_pod_that_does_not_hold_the_lease() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-renew";
    wipe(&url, external_id).await;

    let store = RedisSessionStore::connect_in(&url, &namespace(external_id))
        .await
        .unwrap();
    store.upsert(&session(external_id, "pod-a")).await.unwrap();

    assert!(store.renew(external_id, "pod-a").await.unwrap());
    assert!(!store.renew(external_id, "pod-b").await.unwrap());

    expire_now(&url, external_id).await;
    assert!(
        !store.renew(external_id, "pod-a").await.unwrap(),
        "renewal revived an expired lease, so the pod would keep a session another pod adopted"
    );

    wipe(&url, external_id).await;
}

#[tokio::test]
async fn forgetting_a_session_removes_it_from_the_index_too() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-forget";
    wipe(&url, external_id).await;

    let store = RedisSessionStore::connect_in(&url, &namespace(external_id))
        .await
        .unwrap();
    store.upsert(&session(external_id, "pod-a")).await.unwrap();
    store.forget(external_id).await.unwrap();
    expire_now(&url, external_id).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let claimed = store.claim_unleased("pod-b", 8).await.unwrap();
    assert!(
        !claimed.iter().any(|held| held.external_id == external_id),
        "an ended call was still adoptable, which would create a tap for a call that is over"
    );
}

#[tokio::test]
async fn a_partitioned_pod_cannot_take_a_lease_back_by_persisting() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-nosteal";
    wipe(&url, external_id).await;

    let store = RedisSessionStore::connect_in(&url, &namespace(external_id))
        .await
        .unwrap();
    store.upsert(&session(external_id, "pod-a")).await.unwrap();
    expire_now(&url, external_id).await;
    assert_eq!(store.claim_unleased("pod-b", 8).await.unwrap().len(), 1);

    store.upsert(&session(external_id, "pod-a")).await.unwrap();

    assert!(
        !store.renew(external_id, "pod-a").await.unwrap(),
        "the old owner persisted and took the lease back; both pods would keep tapping"
    );
    assert!(store.renew(external_id, "pod-b").await.unwrap());

    wipe(&url, external_id).await;
}

#[tokio::test]
async fn a_record_written_before_the_tap_tag_existed_is_still_adoptable() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let external_id = "req-legacy";
    wipe(&url, external_id).await;

    let namespace = namespace(external_id);
    let client = redis::Client::open(url.clone()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let legacy = format!(
        concat!(
            r#"{{"external_id":"{id}","kind":1,"call_id":"call-legacy","#,
            r#""from_tags":["from-a"],"rtpengine_node":"10.0.0.5:22222","#,
            r#""owner":"pod-a","attachments":[{{"label":"rtt","transport":1,"#,
            r#""endpoint":"wss-rtt-endpoint","capabilities":[1],"selector":null,"#,
            r#""authoritative":true,"paused":false,"metadata":{{}}}}]}}"#
        ),
        id = external_id
    );
    let _: () = redis::cmd("SET")
        .arg(format!("{namespace}:session:{external_id}"))
        .arg(&legacy)
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: () = redis::cmd("SADD")
        .arg(format!("{namespace}:sessions"))
        .arg(external_id)
        .query_async(&mut connection)
        .await
        .unwrap();

    let store = RedisSessionStore::connect_in(&url, &namespace)
        .await
        .unwrap();
    let claimed = store.claim_unleased("pod-b", 8).await.unwrap();
    let restored = claimed
        .into_iter()
        .find(|held| held.external_id == external_id)
        .expect("a record from before this field existed must still be adoptable");
    assert_eq!(restored.subscription_tag, "");
    assert_eq!(
        restored.attachments[0].format, None,
        "a record from before the format was persisted must still be adoptable"
    );

    wipe(&url, external_id).await;
}

async fn wipe_group(url: &str, group: &str) {
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: Result<(), _> = redis::pipe()
        .del(format!("mss-itest-{group}:group:{group}"))
        .ignore()
        .del(format!("mss-itest-{group}:group:{group}:members"))
        .ignore()
        .query_async::<()>(&mut connection)
        .await;
}

async fn group_ttl(url: &str, group: &str) -> i64 {
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("TTL")
        .arg(format!("mss-itest-{group}:group:{group}"))
        .query_async(&mut connection)
        .await
        .unwrap()
}

#[tokio::test]
async fn two_pods_opening_one_recording_group_agree_on_one_anchor() {
    let Some(url) = url() else {
        eprintln!("{URL_ENV} not set; skipping");
        return;
    };
    let group = "acct-race/conf-race";
    wipe_group(&url, group).await;

    let contenders: u64 = 6;
    let mut tasks = Vec::new();
    for index in 0..contenders {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let store = RedisSessionStore::connect_in(&url, &namespace(group))
                .await
                .unwrap();
            let owner = format!("pod-{index}");
            let proposed = GroupRecord::opening_at(
                std::time::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000 + index * 1_000),
                "rec-race",
                "wav",
                &owner,
            );
            let participants = vec![format!("acct-race/rec-race/party-{index}.wav")];
            store
                .open_or_join_group(group, &proposed, &participants, false)
                .await
                .unwrap()
        }));
    }
    let mut anchors = Vec::new();
    let mut creators = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            GroupJoin::Joined(record) => {
                anchors.push(record.opened_at_unix_ms);
                creators.push(record.created_by);
            }
            other => panic!("a distinct participant label must always be seated: {other:?}"),
        }
    }

    assert_eq!(anchors.len(), contenders as usize);
    creators.sort();
    creators.dedup();
    assert_eq!(
        creators.len(),
        1,
        "SET NX must leave exactly one creator, not {creators:?}"
    );
    let first = anchors[0];
    assert!(
        anchors.iter().all(|held| *held == first),
        "every member of one group must anchor on one instant: {anchors:?}"
    );

    let ttl = group_ttl(&url, group).await;
    assert!(
        ttl > 0 && ttl <= session_store::GROUP_RECORD_TTL.as_secs() as i64,
        "group ttl was {ttl}; a group record without expiry would outlive every recording"
    );

    let store = RedisSessionStore::connect_in(&url, &namespace(group))
        .await
        .unwrap();
    let held = store
        .open_or_join_group(
            group,
            &GroupRecord::opening_at(std::time::SystemTime::now(), "rec-race", "wav", "pod-late"),
            &["acct-race/rec-race/party-0.wav".to_string()],
            false,
        )
        .await
        .unwrap();
    match held {
        GroupJoin::ParticipantHeld { participant, owner } => {
            assert_eq!(participant, "acct-race/rec-race/party-0.wav");
            assert!(
                !owner.is_empty(),
                "the refusal must name the pod holding it"
            );
        }
        other => panic!("a reused participant label must be refused: {other:?}"),
    }

    let other_recording = store
        .open_or_join_group(
            group,
            &GroupRecord::opening_at(std::time::SystemTime::now(), "rec-other", "wav", "pod-late"),
            &["acct-race/rec-other/party-9.wav".to_string()],
            false,
        )
        .await
        .unwrap();
    match other_recording {
        GroupJoin::RecordsAnother(record) => assert_eq!(record.recording_id, "rec-race"),
        other => panic!("one group writes one recording: {other:?}"),
    }

    for index in 0..contenders {
        store
            .leave_group(group, &[format!("acct-race/rec-race/party-{index}.wav")])
            .await
            .unwrap();
    }
    assert_eq!(
        group_ttl(&url, group).await,
        -2,
        "the group record must be gone once its last member left"
    );

    wipe_group(&url, group).await;
}
