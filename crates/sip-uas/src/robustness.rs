use crate::timing::{Reliability, Timings};
use crate::transaction::ServerTransactions;
use std::net::SocketAddr;
use std::time::Duration;

pub struct Mutator {
    state: u64,
}

impl Mutator {
    pub fn seeded(seed: u64) -> Self {
        Mutator { state: seed | 1 }
    }

    fn next(&mut self) -> u64 {
        let mut state = self.state;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.state = state;
        state
    }

    fn upto(&mut self, ceiling: usize) -> usize {
        if ceiling == 0 {
            0
        } else {
            (self.next() % ceiling as u64) as usize
        }
    }

    pub fn mutate(&mut self, seed: &[u8]) -> Vec<u8> {
        let mut datagram = seed.to_vec();
        let rounds = 1 + self.upto(6);
        for _ in 0..rounds {
            if datagram.is_empty() {
                datagram.push(self.next() as u8);
                continue;
            }
            match self.next() % 8 {
                0 => {
                    let at = self.upto(datagram.len());
                    datagram[at] ^= 1 << (self.upto(8));
                }
                1 => {
                    let at = self.upto(datagram.len());
                    datagram.truncate(at);
                }
                2 => {
                    let at = self.upto(datagram.len());
                    datagram.insert(at, 0);
                }
                3 => {
                    let at = self.upto(datagram.len());
                    datagram.insert(at, b'\r');
                }
                4 => {
                    let long = vec![b'A'; 1 + self.upto(2048)];
                    let at = self.upto(datagram.len());
                    datagram.splice(at..at, long);
                }
                5 => {
                    let at = self.upto(datagram.len());
                    datagram.splice(at..at, b"Content-Length: 4294967295\r\n".iter().copied());
                }
                6 => {
                    let half = datagram.len() / 2;
                    let head: Vec<u8> = datagram[..half].to_vec();
                    datagram.extend_from_slice(&head);
                }
                _ => {
                    let at = self.upto(datagram.len());
                    datagram.splice(at..at, b";;;=%00\x7f\xff".iter().copied());
                }
            }
        }
        datagram
    }
}

pub fn sweep(seeds: &[Vec<u8>], rounds: usize, from: SocketAddr) -> usize {
    let mut mutator = Mutator::seeded(0x5150_5350_4d53_5300);
    let mut layer = ServerTransactions::new(Timings::default(), Reliability::Unreliable);
    let mut delivered = 0;
    for round in 0..rounds {
        let seed = &seeds[round % seeds.len()];
        let datagram = mutator.mutate(seed);
        let now = Duration::from_millis(round as u64 * 10);
        delivered += layer.on_datagram(&datagram, from, now).len();
        layer.poll(now);
    }
    delivered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seeds() -> Vec<Vec<u8>> {
        ["INVITE", "ACK", "BYE", "CANCEL", "OPTIONS", "REGISTER"]
            .iter()
            .map(|method| {
                format!(
                    "{method} sip:7200@172.31.99.14:5080 SIP/2.0\r\n\
Via: SIP/2.0/UDP 172.31.99.80:5060;branch=z9hG4bK-seed;rport\r\n\
From: <sip:tester@172.31.99.80>;tag=caller\r\n\
To: <sip:7200@172.31.99.14>\r\n\
Call-ID: seed-call\r\n\
CSeq: 1 {method}\r\n\
Max-Forwards: 70\r\n\
Contact: <sip:tester@172.31.99.80:5060>\r\n\
Session-Expires: 1800;refresher=uac\r\n\
Content-Length: 0\r\n\r\n"
                )
                .into_bytes()
            })
            .collect()
    }

    #[test]
    fn twenty_five_thousand_mutated_datagrams_never_panic_the_front_door() {
        let from: SocketAddr = "172.31.99.80:5060".parse().expect("a peer");
        sweep(&seeds(), 25_000, from);
    }

    #[test]
    fn the_sweep_is_deterministic_so_a_failure_can_be_reproduced() {
        let from: SocketAddr = "172.31.99.80:5060".parse().expect("a peer");
        assert_eq!(sweep(&seeds(), 5_000, from), sweep(&seeds(), 5_000, from));
    }

    #[test]
    fn a_mutation_that_stays_valid_is_still_understood() {
        let from: SocketAddr = "172.31.99.80:5060".parse().expect("a peer");
        let mut layer = ServerTransactions::new(Timings::default(), Reliability::Unreliable);
        let actions = layer.on_datagram(&seeds()[0], from, Duration::ZERO);
        assert!(
            !actions.is_empty(),
            "the corpus must contain at least one datagram the layer acts on, \
             or the sweep proves nothing"
        );
    }

    #[test]
    fn the_mutator_actually_changes_its_input() {
        let mut mutator = Mutator::seeded(7);
        let seed = seeds()[0].clone();
        let changed = (0..64).filter(|_| mutator.mutate(&seed) != seed).count();
        assert!(changed > 60, "the mutator produced {changed}/64 changes");
    }
}
