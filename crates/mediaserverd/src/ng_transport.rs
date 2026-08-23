use rtpengine_ng::{
    NgClient, NgError, NgReply, PlayMedia, PlayTarget, RtpengineStatistics, SubscribeRequest,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub const MAX_NG_DATAGRAM: usize = 65_535;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("no reply from rtpengine at {node} after {attempts} attempts")]
    Timeout { node: SocketAddr, attempts: u32 },
    #[error("socket error talking to rtpengine: {0}")]
    Io(#[from] std::io::Error),
    #[error("rtpengine protocol error: {0}")]
    Ng(#[from] NgError),
    #[error("transport reader stopped before the reply arrived")]
    ReaderStopped,
}

#[derive(Debug, Clone)]
pub struct NgTransportConfig {
    pub request_timeout: Duration,
    pub max_attempts: u32,
    pub unhealthy_after_consecutive_timeouts: u32,
}

impl Default for NgTransportConfig {
    fn default() -> Self {
        NgTransportConfig {
            request_timeout: Duration::from_millis(500),
            max_attempts: 3,
            unhealthy_after_consecutive_timeouts: 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeHealth {
    pub healthy: bool,
    pub consecutive_timeouts: u32,
    pub replies: u64,
    pub timeouts: u64,
}

#[derive(Debug, Default)]
struct HealthCounters {
    consecutive_timeouts: AtomicU32,
    replies: AtomicU64,
    timeouts: AtomicU64,
}

impl HealthCounters {
    fn record_reply(&self) {
        self.consecutive_timeouts.store(0, Ordering::Relaxed);
        self.replies.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timeout(&self) {
        self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed);
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, unhealthy_after: u32) -> NodeHealth {
        let consecutive_timeouts = self.consecutive_timeouts.load(Ordering::Relaxed);
        NodeHealth {
            healthy: consecutive_timeouts < unhealthy_after,
            consecutive_timeouts,
            replies: self.replies.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
        }
    }
}

static COOKIE_SERIAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct CookieSequence {
    prefix: u64,
}

impl CookieSequence {
    pub fn new(prefix: u64) -> Self {
        CookieSequence { prefix }
    }

    pub fn next_cookie(&self) -> Vec<u8> {
        let serial = COOKIE_SERIAL.fetch_add(1, Ordering::Relaxed);
        format!("{:x}-{:x}", self.prefix, serial).into_bytes()
    }
}

type ReplySender = oneshot::Sender<Result<NgReply, NgError>>;

#[derive(Debug, Default)]
struct PendingReplies {
    waiters: Mutex<HashMap<Vec<u8>, ReplySender>>,
}

impl PendingReplies {
    fn insert(&self, cookie: Vec<u8>, sender: ReplySender) {
        self.locked().insert(cookie, sender);
    }

    fn take(&self, cookie: &[u8]) -> Option<ReplySender> {
        self.locked().remove(cookie)
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, ReplySender>> {
        self.waiters.lock().unwrap_or_else(|e| e.into_inner())
    }
}

struct PendingGuard<'a> {
    pending: &'a PendingReplies,
    cookie: Vec<u8>,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.take(&self.cookie);
    }
}

pub struct NgTransport {
    socket: Arc<UdpSocket>,
    node: SocketAddr,
    config: NgTransportConfig,
    cookies: CookieSequence,
    pending: Arc<PendingReplies>,
    health: Arc<HealthCounters>,
    reader: JoinHandle<()>,
}

impl NgTransport {
    pub async fn bind(
        local: SocketAddr,
        node: SocketAddr,
        config: NgTransportConfig,
        cookie_prefix: u64,
    ) -> Result<Self, TransportError> {
        let socket = Arc::new(UdpSocket::bind(local).await?);
        let pending = Arc::new(PendingReplies::default());
        let reader = tokio::spawn(read_replies(Arc::clone(&socket), Arc::clone(&pending)));
        Ok(NgTransport {
            socket,
            node,
            config,
            cookies: CookieSequence::new(cookie_prefix),
            pending,
            health: Arc::new(HealthCounters::default()),
            reader,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.socket.local_addr()?)
    }

    pub fn health(&self) -> NodeHealth {
        self.health
            .snapshot(self.config.unhealthy_after_consecutive_timeouts)
    }

    pub async fn ping(&self) -> Result<NgReply, TransportError> {
        self.roundtrip(NgClient::ping).await
    }

    pub async fn version(&self) -> Result<NgReply, TransportError> {
        self.roundtrip(NgClient::version).await
    }

    pub async fn statistics(&self) -> Result<RtpengineStatistics, TransportError> {
        let reply = self.roundtrip(NgClient::statistics).await?;
        Ok(RtpengineStatistics::from_reply(&reply)?)
    }

    pub async fn subscribe_request(
        &self,
        request: &SubscribeRequest,
    ) -> Result<NgReply, TransportError> {
        self.roundtrip(|cookie| NgClient::subscribe_request(cookie, request))
            .await
    }

    pub async fn subscribe_answer(
        &self,
        call_id: &str,
        to_tag: &str,
        sdp: &str,
    ) -> Result<NgReply, TransportError> {
        self.roundtrip(|cookie| NgClient::subscribe_answer(cookie, call_id, to_tag, sdp))
            .await
    }

    pub async fn unsubscribe(
        &self,
        call_id: &str,
        to_tag: &str,
    ) -> Result<NgReply, TransportError> {
        self.roundtrip(|cookie| NgClient::unsubscribe(cookie, call_id, to_tag))
            .await
    }

    pub async fn query(&self, call_id: &str) -> Result<NgReply, TransportError> {
        self.roundtrip(|cookie| NgClient::query(cookie, call_id))
            .await
    }

    pub async fn play_media(&self, play: &PlayMedia) -> Result<NgReply, TransportError> {
        self.roundtrip(|cookie| NgClient::play_media(cookie, play))
            .await
    }

    pub async fn stop_media(
        &self,
        call_id: &str,
        target: &PlayTarget,
    ) -> Result<NgReply, TransportError> {
        self.roundtrip(|cookie| NgClient::stop_media(cookie, call_id, target))
            .await
    }

    async fn roundtrip<F>(&self, build_datagram: F) -> Result<NgReply, TransportError>
    where
        F: FnOnce(&[u8]) -> Vec<u8>,
    {
        let cookie = self.cookies.next_cookie();
        let datagram = build_datagram(&cookie);
        let (sender, mut receiver) = oneshot::channel();
        self.pending.insert(cookie.clone(), sender);
        let _guard = PendingGuard {
            pending: &self.pending,
            cookie: cookie.clone(),
        };

        for attempt in 1..=self.config.max_attempts {
            self.socket.send_to(&datagram, self.node).await?;
            match tokio::time::timeout(self.config.request_timeout, &mut receiver).await {
                Ok(Ok(reply)) => {
                    self.health.record_reply();
                    return reply.map_err(TransportError::Ng);
                }
                Ok(Err(_)) => return Err(TransportError::ReaderStopped),
                Err(_) => debug!(
                    node = %self.node,
                    attempt,
                    max_attempts = self.config.max_attempts,
                    "ng request timed out, retransmitting with the same cookie"
                ),
            }
        }

        self.health.record_timeout();
        warn!(
            node = %self.node,
            attempts = self.config.max_attempts,
            "ng node did not reply"
        );
        Err(TransportError::Timeout {
            node: self.node,
            attempts: self.config.max_attempts,
        })
    }
}

impl Drop for NgTransport {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

async fn read_replies(socket: Arc<UdpSocket>, pending: Arc<PendingReplies>) {
    let mut buf = vec![0u8; MAX_NG_DATAGRAM];
    loop {
        let received = match socket.recv_from(&mut buf).await {
            Ok((len, _from)) => len,
            Err(error) => {
                warn!(%error, "ng reader socket failed");
                return;
            }
        };
        let datagram = &buf[..received];
        let Ok((cookie, _)) = NgClient::split_cookie(datagram) else {
            debug!(bytes = received, "ng datagram without cookie separator");
            continue;
        };
        match pending.take(cookie) {
            Some(sender) => {
                let _ = sender.send(NgClient::parse_reply(datagram));
            }
            None => debug!(
                cookie = %String::from_utf8_lossy(cookie),
                "ng reply for unknown cookie"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeRtpengine {
        addr: SocketAddr,
        received: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl FakeRtpengine {
        async fn spawn<F>(responder: F) -> FakeRtpengine
        where
            F: Fn(usize, &[u8]) -> Vec<Vec<u8>> + Send + 'static,
        {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            let received = Arc::new(Mutex::new(Vec::new()));
            let log = Arc::clone(&received);
            tokio::spawn(async move {
                let mut buf = vec![0u8; MAX_NG_DATAGRAM];
                let mut count = 0usize;
                loop {
                    let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                        return;
                    };
                    count += 1;
                    log.lock().unwrap().push(buf[..len].to_vec());
                    for reply in responder(count, &buf[..len]) {
                        let _ = socket.send_to(&reply, from).await;
                    }
                }
            });
            FakeRtpengine { addr, received }
        }

        fn cookies(&self) -> Vec<Vec<u8>> {
            self.received
                .lock()
                .unwrap()
                .iter()
                .map(|d| NgClient::split_cookie(d).unwrap().0.to_vec())
                .collect()
        }

        fn request_count(&self) -> usize {
            self.received.lock().unwrap().len()
        }
    }

    fn reply_to(datagram: &[u8], body: &str) -> Vec<u8> {
        let (cookie, _) = NgClient::split_cookie(datagram).unwrap();
        let mut out = cookie.to_vec();
        out.push(b' ');
        out.extend_from_slice(body.as_bytes());
        out
    }

    fn fast_config() -> NgTransportConfig {
        NgTransportConfig {
            request_timeout: Duration::from_millis(60),
            max_attempts: 3,
            unhealthy_after_consecutive_timeouts: 1,
        }
    }

    async fn transport_to(node: SocketAddr, config: NgTransportConfig) -> NgTransport {
        NgTransport::bind("127.0.0.1:0".parse().unwrap(), node, config, 0xABCD)
            .await
            .unwrap()
    }

    #[test]
    fn two_transports_on_one_pod_never_share_a_cookie() {
        let first = CookieSequence::new(0xABCD);
        let second = CookieSequence::new(0xABCD);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..8 {
            assert!(seen.insert(first.next_cookie()));
            assert!(seen.insert(second.next_cookie()));
        }
        assert_eq!(seen.len(), 16);
    }

    #[tokio::test]
    async fn ping_reply_is_correlated_by_cookie() {
        let fake =
            FakeRtpengine::spawn(|_, datagram| vec![reply_to(datagram, "d6:result4:ponge")]).await;
        let transport = transport_to(fake.addr, fast_config()).await;

        let reply = transport.ping().await.unwrap();
        assert_eq!(reply.cookie, fake.cookies()[0]);
        assert_eq!(transport.health().replies, 1);
        assert!(transport.health().healthy);
    }

    #[tokio::test]
    async fn statistics_arrives_as_the_kernel_and_userspace_split() {
        let fake = FakeRtpengine::spawn(|_, datagram| {
            vec![reply_to(
                datagram,
                "d6:result2:ok10:statisticsd15:totalstatisticsd\
14:relayedpacketsi90e21:relayedpackets_kerneli90e19:relayedpackets_useri0e6:uptime3:512e\
17:currentstatisticsd17:packetrate_kerneli51eeee",
            )]
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        let statistics = transport.statistics().await.unwrap();
        assert_eq!(statistics.totals.packets_in_kernel, 90);
        assert_eq!(statistics.uptime_seconds, Some(512));
        assert_eq!(
            statistics.kernel_forwarding(),
            rtpengine_ng::KernelForwarding::ForwardingInKernelNow
        );
        let sent = String::from_utf8_lossy(&fake.received.lock().unwrap()[0]).to_string();
        assert!(sent.contains("7:command10:statistics"), "{sent}");
    }

    #[tokio::test]
    async fn an_rtpengine_that_does_not_know_the_version_command_says_so_instead_of_hanging() {
        let fake = FakeRtpengine::spawn(|_, datagram| {
            vec![reply_to(
                datagram,
                "d12:error-reason20:Unrecognized command6:result5:errore",
            )]
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        match transport.version().await {
            Err(TransportError::Ng(NgError::Remote(reason))) => {
                assert_eq!(reason, rtpengine_ng::UNRECOGNIZED_COMMAND)
            }
            other => panic!("expected the remote refusal, got {other:?}"),
        }
        assert_eq!(fake.request_count(), 1);
    }

    #[tokio::test]
    async fn a_statistics_reply_without_a_statistics_dict_is_an_error_not_a_zeroed_report() {
        let fake =
            FakeRtpengine::spawn(|_, datagram| vec![reply_to(datagram, "d6:result2:oke")]).await;
        let transport = transport_to(fake.addr, fast_config()).await;

        match transport.statistics().await {
            Err(TransportError::Ng(NgError::MissingField(field))) => {
                assert_eq!(field, "statistics")
            }
            other => panic!("expected a missing field error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_reply_surfaces_sdp_and_to_tag() {
        let fake = FakeRtpengine::spawn(|_, datagram| {
            vec![reply_to(
                datagram,
                "d6:result2:ok3:sdp26:v=0\r\nm=audio 30000 RTP/AVP6:to-tag5:t-abce",
            )]
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        let request = SubscribeRequest {
            call_id: "call-1".into(),
            from_tags: vec!["tagA".into(), "tagB".into()],
            ..Default::default()
        };
        let reply = transport.subscribe_request(&request).await.unwrap();
        assert_eq!(reply.to_tag(), Some("t-abc"));
        assert!(reply.sdp().unwrap().starts_with("v=0"));
    }

    #[tokio::test]
    async fn error_reply_reaches_the_caller_instead_of_timing_out() {
        let fake = FakeRtpengine::spawn(|_, datagram| {
            vec![reply_to(
                datagram,
                "d12:error-reason15:Unknown call-id6:result5:errore",
            )]
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        match transport.unsubscribe("gone", "t-1").await {
            Err(TransportError::Ng(NgError::Remote(reason))) => {
                assert_eq!(reason, "Unknown call-id")
            }
            other => panic!("expected remote error, got {other:?}"),
        }
        assert_eq!(fake.request_count(), 1);
    }

    #[tokio::test]
    async fn retransmits_the_same_cookie_until_the_node_answers() {
        let fake = FakeRtpengine::spawn(|count, datagram| {
            if count < 3 {
                Vec::new()
            } else {
                vec![reply_to(datagram, "d6:result4:ponge")]
            }
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        transport.ping().await.unwrap();

        let cookies = fake.cookies();
        assert_eq!(cookies.len(), 3);
        assert_eq!(cookies[0], cookies[1]);
        assert_eq!(cookies[1], cookies[2]);
        assert_eq!(transport.health().timeouts, 0);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_and_marks_the_node_unhealthy() {
        let fake = FakeRtpengine::spawn(|_, _| Vec::new()).await;
        let transport = transport_to(fake.addr, fast_config()).await;

        match transport.ping().await {
            Err(TransportError::Timeout { attempts, .. }) => assert_eq!(attempts, 3),
            other => panic!("expected timeout, got {other:?}"),
        }
        assert_eq!(fake.request_count(), 3);
        let health = transport.health();
        assert!(!health.healthy);
        assert_eq!(health.consecutive_timeouts, 1);
        assert_eq!(health.replies, 0);
    }

    #[tokio::test]
    async fn stray_and_duplicate_replies_do_not_disturb_pending_requests() {
        let fake = FakeRtpengine::spawn(|_, datagram| {
            vec![
                b"unknown-cookie d6:result4:ponge".to_vec(),
                b"no-separator-at-all".to_vec(),
                reply_to(datagram, "d6:result4:ponge"),
                reply_to(datagram, "d6:result4:ponge"),
            ]
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        transport.ping().await.unwrap();
        transport.ping().await.unwrap();
        assert_eq!(transport.health().replies, 2);
    }

    #[tokio::test]
    async fn concurrent_requests_each_get_their_own_reply() {
        let fake = FakeRtpengine::spawn(|count, datagram| {
            let label = if count == 1 { "first" } else { "second" };
            vec![reply_to(
                datagram,
                &format!("d6:result2:ok6:to-tag{}:{}e", label.len(), label),
            )]
        })
        .await;
        let transport = transport_to(fake.addr, fast_config()).await;

        let one = transport.subscribe_answer("call-1", "t-1", "v=0\r\n");
        let two = transport.subscribe_answer("call-2", "t-2", "v=0\r\n");
        let (first, second) = tokio::join!(one, two);

        let tags = [
            first.unwrap().to_tag().unwrap().to_string(),
            second.unwrap().to_tag().unwrap().to_string(),
        ];
        assert!(tags.contains(&"first".to_string()), "{tags:?}");
        assert!(tags.contains(&"second".to_string()), "{tags:?}");
    }

    #[tokio::test]
    async fn cookies_are_unique_per_request() {
        let sequence = CookieSequence::new(0x1234);
        let first = sequence.next_cookie();
        let second = sequence.next_cookie();
        assert_ne!(first, second);
        assert!(first.starts_with(b"1234-"));
        assert!(second.starts_with(b"1234-"));
        let serial = |cookie: &[u8]| {
            let text = std::str::from_utf8(cookie).unwrap().to_string();
            u64::from_str_radix(text.split('-').nth(1).unwrap(), 16).unwrap()
        };
        assert!(serial(&second) > serial(&first));
        assert!(!first.contains(&b' '));
    }

    #[tokio::test]
    async fn pending_waiters_are_released_when_a_request_ends() {
        let fake = FakeRtpengine::spawn(|_, _| Vec::new()).await;
        let transport = transport_to(fake.addr, fast_config()).await;

        assert!(transport.ping().await.is_err());
        assert!(transport.pending.locked().is_empty());
    }
}
