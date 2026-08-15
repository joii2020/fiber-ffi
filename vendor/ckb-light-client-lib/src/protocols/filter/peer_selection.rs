use std::cmp;

use ckb_network::{BoxedCKBProtocolContext, PeerIndex};
use ckb_types::core::BlockNumber;
use dashmap::DashMap;
use log::{debug, warn};
use rand::Rng as _;

use crate::types::{Duration, Instant};

/// Controls adaptive peer selection for block-filter synchronization.
///
/// A preferred peer is a CKB network whitelist peer. All selected peers must
/// still be members of the Light Client's proved candidate set.
#[derive(Clone, Copy, Debug)]
pub struct FilterPeerSelectionConfig {
    pub preferred_peer_chance_percent: u8,
    pub request_timeout: Duration,
    pub consecutive_failures_before_cooldown: u32,
    pub failure_cooldown: Duration,
}

impl Default for FilterPeerSelectionConfig {
    fn default() -> Self {
        Self {
            preferred_peer_chance_percent: 90,
            request_timeout: Duration::from_secs(6),
            consecutive_failures_before_cooldown: 2,
            failure_cooldown: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct PeerHealth {
    latency_ewma_ms: Option<u64>,
    successes: u64,
    timeouts: u64,
    consecutive_failures: u32,
    cooldown_until: Option<Instant>,
}

pub(crate) struct FilterPeerSelector {
    config: FilterPeerSelectionConfig,
    health: DashMap<PeerIndex, PeerHealth>,
    in_flight: DashMap<(BlockNumber, PeerIndex), Instant>,
}

impl FilterPeerSelector {
    pub(crate) fn new(config: FilterPeerSelectionConfig) -> Self {
        Self {
            config,
            health: DashMap::new(),
            in_flight: DashMap::new(),
        }
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.config.request_timeout
    }

    pub(crate) fn has_live_request(&self) -> bool {
        self.expire_stale_requests();
        !self.in_flight.is_empty()
    }

    pub(crate) fn record_request(&self, start_number: BlockNumber, peer: PeerIndex) {
        self.expire_stale_requests();
        self.in_flight.insert((start_number, peer), Instant::now());
    }

    pub(crate) fn record_send_failure(&self, start_number: BlockNumber, peer: PeerIndex) {
        self.in_flight.remove(&(start_number, peer));
        self.record_failure(peer, "send failed");
    }

    pub(crate) fn record_valid_response(
        &self,
        start_number: BlockNumber,
        peer: PeerIndex,
        blocks_count: usize,
    ) {
        let elapsed_ms = self
            .in_flight
            .remove(&(start_number, peer))
            .map(|(_, sent_at)| duration_millis(sent_at.elapsed()));

        // A retry can leave a second request for the same range in flight. The
        // first valid response wins; a later response is safely ignored by the
        // existing continuity check.
        let duplicate_keys = self
            .in_flight
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.0 == start_number).then_some(key)
            })
            .collect::<Vec<_>>();
        for key in duplicate_keys {
            self.in_flight.remove(&key);
        }

        let mut health = self.health.entry(peer).or_default();
        if let Some(sample_ms) = elapsed_ms {
            health.latency_ewma_ms = Some(match health.latency_ewma_ms {
                // alpha = 0.2, expressed with integer arithmetic.
                Some(previous_ms) => (previous_ms.saturating_mul(4) + sample_ms) / 5,
                None => sample_ms,
            });
        }
        health.successes = health.successes.saturating_add(1);
        health.consecutive_failures = 0;
        health.cooldown_until = None;

        debug!(
            "filter peer response peer={} start={} blocks={} latency_ms={:?} ewma_ms={:?} successes={} timeouts={}",
            peer,
            start_number,
            blocks_count,
            elapsed_ms,
            health.latency_ewma_ms,
            health.successes,
            health.timeouts,
        );
    }

    pub(crate) fn choose(
        &self,
        nc: &BoxedCKBProtocolContext,
        candidates: Vec<PeerIndex>,
    ) -> Option<PeerIndex> {
        self.expire_stale_requests();
        if candidates.is_empty() {
            return None;
        }

        let now = Instant::now();
        let connected = candidates
            .iter()
            .copied()
            .filter(|peer| nc.get_peer(*peer).is_some())
            .collect::<Vec<_>>();
        let connected_or_all = if connected.is_empty() {
            candidates
        } else {
            connected
        };
        let ready = connected_or_all
            .iter()
            .copied()
            .filter(|peer| !self.in_cooldown(*peer, now))
            .collect::<Vec<_>>();
        let ready_or_all = if ready.is_empty() {
            connected_or_all
        } else {
            ready
        };

        let preferred = ready_or_all
            .iter()
            .copied()
            .filter(|peer| nc.get_peer(*peer).is_some_and(|info| info.is_whitelist))
            .collect::<Vec<_>>();
        let public = ready_or_all
            .iter()
            .copied()
            .filter(|peer| !nc.get_peer(*peer).is_some_and(|info| info.is_whitelist))
            .collect::<Vec<_>>();

        let prefer =
            rand::thread_rng().gen_range(0..100) < self.config.preferred_peer_chance_percent;
        let pool = if prefer && !preferred.is_empty() {
            &preferred
        } else if !prefer && !public.is_empty() {
            &public
        } else if !preferred.is_empty() {
            &preferred
        } else {
            &ready_or_all
        };

        let selected = pool.iter().copied().min_by_key(|peer| self.rank(nc, *peer));
        if let Some(peer) = selected {
            let network_peer = nc.get_peer(peer);
            let health = self.health.get(&peer);
            debug!(
                "selected filter peer={} address={:?} preferred={} ping_ms={:?} ewma_ms={:?} consecutive_failures={} candidate_count={}",
                peer,
                network_peer.as_ref().map(|info| &info.connected_addr),
                network_peer.as_ref().is_some_and(|info| info.is_whitelist),
                network_peer
                    .as_ref()
                    .and_then(|info| info.ping_rtt)
                    .map(duration_millis),
                health.as_ref().and_then(|item| item.latency_ewma_ms),
                health
                    .as_ref()
                    .map(|item| item.consecutive_failures)
                    .unwrap_or_default(),
                ready_or_all.len(),
            );
        }
        selected
    }

    fn rank(&self, nc: &BoxedCKBProtocolContext, peer: PeerIndex) -> (u32, u64) {
        let health = self.health.get(&peer);
        let failures = health
            .as_ref()
            .map(|item| item.consecutive_failures)
            .unwrap_or_default();
        let measured_latency = health
            .as_ref()
            .and_then(|item| item.latency_ewma_ms)
            .or_else(|| {
                nc.get_peer(peer)
                    .and_then(|info| info.ping_rtt)
                    .map(duration_millis)
            })
            .unwrap_or(10_000);
        (failures, measured_latency)
    }

    fn in_cooldown(&self, peer: PeerIndex, now: Instant) -> bool {
        self.health
            .get(&peer)
            .and_then(|item| item.cooldown_until)
            .is_some_and(|until| until > now)
    }

    fn expire_stale_requests(&self) {
        let stale = self
            .in_flight
            .iter()
            .filter_map(|entry| {
                (entry.value().elapsed() >= self.config.request_timeout).then_some(*entry.key())
            })
            .collect::<Vec<_>>();
        for (start_number, peer) in stale {
            if self.in_flight.remove(&(start_number, peer)).is_some() {
                self.record_failure(peer, "request timeout");
            }
        }
    }

    fn record_failure(&self, peer: PeerIndex, reason: &str) {
        let mut health = self.health.entry(peer).or_default();
        health.timeouts = health.timeouts.saturating_add(1);
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        if health.consecutive_failures >= self.config.consecutive_failures_before_cooldown {
            health.cooldown_until = Some(Instant::now() + self.config.failure_cooldown);
        }
        warn!(
            "filter peer failure peer={} reason={} consecutive_failures={} timeouts={} cooldown_ms={}",
            peer,
            reason,
            health.consecutive_failures,
            health.timeouts,
            health
                .cooldown_until
                .map(|until| duration_millis(until.saturating_duration_since(Instant::now())))
                .unwrap_or_default(),
        );
    }
}

fn duration_millis(duration: Duration) -> u64 {
    cmp::min(duration.as_millis(), u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_response_completes_every_request_for_the_same_range() {
        let selector = FilterPeerSelector::new(FilterPeerSelectionConfig::default());
        let fast_peer = PeerIndex::new(1);
        let backup_peer = PeerIndex::new(2);

        selector.record_request(100, fast_peer);
        selector.record_request(100, backup_peer);
        assert_eq!(selector.in_flight.len(), 2);

        selector.record_valid_response(100, fast_peer, 1_000);
        assert!(selector.in_flight.is_empty());
        let health = selector.health.get(&fast_peer).unwrap();
        assert_eq!(health.successes, 1);
        assert_eq!(health.consecutive_failures, 0);
        assert!(health.latency_ewma_ms.is_some());
    }

    #[test]
    fn repeated_timeouts_put_a_peer_in_cooldown() {
        let selector = FilterPeerSelector::new(FilterPeerSelectionConfig {
            request_timeout: Duration::ZERO,
            consecutive_failures_before_cooldown: 2,
            ..FilterPeerSelectionConfig::default()
        });
        let peer = PeerIndex::new(1);

        selector.record_request(100, peer);
        assert!(!selector.has_live_request());
        assert!(selector.health.get(&peer).unwrap().cooldown_until.is_none());

        selector.record_request(101, peer);
        assert!(!selector.has_live_request());
        let health = selector.health.get(&peer).unwrap();
        assert_eq!(health.consecutive_failures, 2);
        assert!(health.cooldown_until.is_some());
    }
}
