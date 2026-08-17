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

use session_store::{PersistedAttachment, PersistedSession, RedisSessionStore, SessionStore};
use std::collections::BTreeMap;

fn session(external_id: &str, owner: &str) -> PersistedSession {
    PersistedSession {
        external_id: external_id.to_string(),
        kind: 1,
        call_id: "call-redis".to_string(),
        from_tags: vec!["from-a".to_string(), "from-b".to_string()],
        rtpengine_node: "10.0.0.5:22222".to_string(),
        owner: owner.to_string(),
        attachments: vec![PersistedAttachment {
            label: "rtt".to_string(),
            transport: 1,
            endpoint: "wss-rtt-endpoint".to_string(),
            capabilities: vec![1, 2],
            selector: Some("customer".to_string()),
            authoritative: true,
            paused: true,
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
