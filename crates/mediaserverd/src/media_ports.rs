use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

pub const PORT_MIN_ENV: &str = "MSS_MEDIA_PORT_MIN";
pub const PORT_MAX_ENV: &str = "MSS_MEDIA_PORT_MAX";
pub const ADVERTISE_IP_ENV: &str = "MSS_MEDIA_ADVERTISE_IP";

const BIND_ATTEMPTS_PER_REQUEST: usize = 64;

#[derive(Debug, thiserror::Error)]
pub enum MediaPortError {
    #[error("every port in {min}-{max} is taken by a live media socket")]
    RangeExhausted { min: u16, max: u16 },
    #[error(
        "no port in {min}-{max} could be bound after {attempts} tries; another process holds them"
    )]
    RangeUnbindable { min: u16, max: u16, attempts: usize },
    #[error("media socket: {0}")]
    Bind(#[from] std::io::Error),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PortCounters {
    pub in_use: u64,
    pub free: u64,
    pub capacity: u64,
    pub exhausted: u64,
    pub bind_conflicts: u64,
}

struct Range {
    min: u16,
    max: u16,
    capacity: u64,
    free: Mutex<VecDeque<u16>>,
}

pub struct MediaPortAllocator {
    range: Option<Range>,
    in_use: AtomicU64,
    exhausted: AtomicU64,
    bind_conflicts: AtomicU64,
}

pub struct PortLease {
    allocator: Arc<MediaPortAllocator>,
    port: u16,
}

impl PortLease {
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for PortLease {
    fn drop(&mut self) {
        self.allocator.release(self.port);
    }
}

pub struct BoundMediaSocket {
    pub socket: UdpSocket,
    pub port: u16,
    pub lease: PortLease,
}

impl MediaPortAllocator {
    pub fn ephemeral() -> Arc<MediaPortAllocator> {
        Arc::new(MediaPortAllocator {
            range: None,
            in_use: AtomicU64::new(0),
            exhausted: AtomicU64::new(0),
            bind_conflicts: AtomicU64::new(0),
        })
    }

    pub fn over_range(min: u16, max: u16) -> Arc<MediaPortAllocator> {
        let first_even = if min.is_multiple_of(2) {
            min
        } else {
            min.saturating_add(1)
        };
        let free: VecDeque<u16> = (first_even..=max).step_by(2).collect();
        let capacity = free.len() as u64;
        Arc::new(MediaPortAllocator {
            range: Some(Range {
                min,
                max,
                capacity,
                free: Mutex::new(free),
            }),
            in_use: AtomicU64::new(0),
            exhausted: AtomicU64::new(0),
            bind_conflicts: AtomicU64::new(0),
        })
    }

    pub fn from_env() -> Arc<MediaPortAllocator> {
        let min = read_port(PORT_MIN_ENV);
        let max = read_port(PORT_MAX_ENV);
        match (min, max) {
            (Some(min), Some(max)) if min <= max => {
                let allocator = MediaPortAllocator::over_range(min, max);
                info!(
                    env_min = PORT_MIN_ENV,
                    env_max = PORT_MAX_ENV,
                    min,
                    max,
                    rtp_sockets = allocator.counters().capacity,
                    "media sockets bind inside this port range, even ports only"
                );
                allocator
            }
            (Some(min), Some(max)) => {
                warn!(
                    env_min = PORT_MIN_ENV,
                    env_max = PORT_MAX_ENV,
                    min,
                    max,
                    "the media port range is inverted; falling back to ephemeral ports"
                );
                MediaPortAllocator::ephemeral()
            }
            (None, None) => {
                info!(
                    env_min = PORT_MIN_ENV,
                    env_max = PORT_MAX_ENV,
                    value = "unset",
                    "media sockets take ephemeral ports from the kernel"
                );
                MediaPortAllocator::ephemeral()
            }
            _ => {
                warn!(
                    env_min = PORT_MIN_ENV,
                    env_max = PORT_MAX_ENV,
                    "a media port range needs both bounds; falling back to ephemeral ports"
                );
                MediaPortAllocator::ephemeral()
            }
        }
    }

    pub fn bind(self: &Arc<Self>, address: IpAddr) -> Result<BoundMediaSocket, MediaPortError> {
        let Some(range) = self.range.as_ref() else {
            let socket = UdpSocket::bind(SocketAddr::new(address, 0))?;
            let port = socket.local_addr()?.port();
            self.in_use.fetch_add(1, Ordering::Relaxed);
            return Ok(BoundMediaSocket {
                socket,
                port,
                lease: PortLease {
                    allocator: Arc::clone(self),
                    port,
                },
            });
        };

        let mut conflicts = 0usize;
        let mut rejected = Vec::new();
        let outcome = loop {
            let Some(candidate) = self.take_free() else {
                break Err(MediaPortError::RangeExhausted {
                    min: range.min,
                    max: range.max,
                });
            };
            match UdpSocket::bind(SocketAddr::new(address, candidate)) {
                Ok(socket) => {
                    self.in_use.fetch_add(1, Ordering::Relaxed);
                    break Ok(BoundMediaSocket {
                        socket,
                        port: candidate,
                        lease: PortLease {
                            allocator: Arc::clone(self),
                            port: candidate,
                        },
                    });
                }
                Err(error) => {
                    conflicts += 1;
                    self.bind_conflicts.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        port = candidate,
                        %error,
                        "a port in the media range could not be bound; trying the next one"
                    );
                    rejected.push(candidate);
                    if conflicts >= BIND_ATTEMPTS_PER_REQUEST {
                        break Err(MediaPortError::RangeUnbindable {
                            min: range.min,
                            max: range.max,
                            attempts: conflicts,
                        });
                    }
                }
            }
        };
        self.return_all(rejected);
        if outcome.is_err() {
            self.exhausted.fetch_add(1, Ordering::Relaxed);
        }
        outcome
    }

    pub fn counters(&self) -> PortCounters {
        let (free, capacity) = match self.range.as_ref() {
            Some(range) => (
                range
                    .free
                    .lock()
                    .map(|free| free.len() as u64)
                    .unwrap_or_default(),
                range.capacity,
            ),
            None => (0, 0),
        };
        PortCounters {
            in_use: self.in_use.load(Ordering::Relaxed),
            free,
            capacity,
            exhausted: self.exhausted.load(Ordering::Relaxed),
            bind_conflicts: self.bind_conflicts.load(Ordering::Relaxed),
        }
    }

    fn take_free(&self) -> Option<u16> {
        let range = self.range.as_ref()?;
        let mut free = range.free.lock().ok()?;
        free.pop_front()
    }

    fn return_all(&self, ports: Vec<u16>) {
        if ports.is_empty() {
            return;
        }
        if let Some(range) = self.range.as_ref() {
            if let Ok(mut free) = range.free.lock() {
                for port in ports {
                    free.push_back(port);
                }
            }
        }
    }

    fn release(&self, port: u16) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        if let Some(range) = self.range.as_ref() {
            if let Ok(mut free) = range.free.lock() {
                if !free.contains(&port) {
                    free.push_back(port);
                }
            }
        }
    }
}

fn read_port(env: &str) -> Option<u16> {
    let configured = std::env::var(env).ok()?;
    let configured = configured.trim().to_string();
    if configured.is_empty() {
        return None;
    }
    match configured.parse::<u16>() {
        Ok(port) if port > 0 => Some(port),
        _ => {
            warn!(
                env,
                configured = %configured,
                "a media port bound must be a port number; ignoring it"
            );
            None
        }
    }
}

pub fn advertise_address(local: IpAddr) -> IpAddr {
    let configured = std::env::var(ADVERTISE_IP_ENV)
        .ok()
        .filter(|configured| !configured.trim().is_empty());
    let Some(configured) = configured else {
        info!(
            env = ADVERTISE_IP_ENV,
            value = "unset",
            advertised = %local,
            "peers are told the address media sockets bind to"
        );
        return local;
    };
    match configured.trim().parse::<IpAddr>() {
        Ok(address) => {
            info!(
                env = ADVERTISE_IP_ENV,
                advertised = %address,
                bound = %local,
                "peers are told this address while media sockets bind to the local one"
            );
            address
        }
        Err(_) => {
            warn!(
                env = ADVERTISE_IP_ENV,
                configured = %configured,
                advertised = %local,
                "the advertised media address must be an ip address; using the local one"
            );
            local
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOOPBACK: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    #[test]
    fn a_range_hands_out_even_ports_only() {
        let allocator = MediaPortAllocator::over_range(41001, 41008);
        let mut held = Vec::new();
        while let Ok(bound) = allocator.bind(LOOPBACK) {
            held.push(bound);
        }
        let ports: Vec<u16> = held.iter().map(|bound| bound.port).collect();
        assert_eq!(ports, vec![41002, 41004, 41006, 41008]);
    }

    #[test]
    fn exhausting_the_range_is_an_error_the_metrics_count() {
        let allocator = MediaPortAllocator::over_range(41100, 41103);
        let first = allocator.bind(LOOPBACK).expect("41100");
        let second = allocator.bind(LOOPBACK).expect("41102");
        let error = match allocator.bind(LOOPBACK) {
            Ok(_) => panic!("the range holds two rtp ports"),
            Err(error) => error,
        };
        assert!(
            matches!(error, MediaPortError::RangeExhausted { .. }),
            "{error}"
        );
        let counters = allocator.counters();
        assert_eq!(counters.in_use, 2);
        assert_eq!(counters.capacity, 2);
        assert_eq!(counters.free, 0);
        assert_eq!(counters.exhausted, 1);
        drop((first, second));
    }

    #[test]
    fn a_closed_socket_returns_its_port_to_the_range() {
        let allocator = MediaPortAllocator::over_range(41200, 41201);
        let first = allocator.bind(LOOPBACK).expect("41200");
        assert_eq!(first.port, 41200);
        assert!(allocator.bind(LOOPBACK).is_err());
        drop(first);
        assert_eq!(allocator.counters().in_use, 0);
        let again = allocator.bind(LOOPBACK).expect("the freed port comes back");
        assert_eq!(again.port, 41200);
        assert_eq!(allocator.counters().in_use, 1);
    }

    #[test]
    fn a_port_another_process_holds_is_skipped_and_counted() {
        let squatter = UdpSocket::bind(SocketAddr::new(LOOPBACK, 41300)).expect("a squatter");
        let allocator = MediaPortAllocator::over_range(41300, 41303);
        let bound = allocator.bind(LOOPBACK).expect("the next even port");
        assert_eq!(bound.port, 41302);
        assert_eq!(allocator.counters().bind_conflicts, 1);
        assert_eq!(allocator.counters().in_use, 1);
        drop(squatter);
    }

    #[test]
    fn no_range_configured_keeps_ephemeral_binds() {
        let allocator = MediaPortAllocator::ephemeral();
        let bound = allocator.bind(LOOPBACK).expect("an ephemeral socket");
        assert!(bound.port > 0);
        let counters = allocator.counters();
        assert_eq!(counters.in_use, 1);
        assert_eq!(counters.capacity, 0);
        drop(bound);
        assert_eq!(allocator.counters().in_use, 0);
    }
}
