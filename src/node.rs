use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub id: u64,
    #[cfg(feature = "volo-adapter")]
    pub address: volo::net::Address,
    #[cfg(not(feature = "volo-adapter"))]
    pub address: String,
}

#[derive(Debug)]
pub struct Node {
    pub endpoint: Endpoint,
    pub weight: u32,
    pub in_flight: AtomicUsize,
    pub success: AtomicU64,
    pub fail: AtomicU64,
    pub last_rtt_ns: AtomicU64,
}

impl Node {
    pub fn new(endpoint: Endpoint, weight: u32) -> Self {
        Self {
            endpoint,
            weight,
            in_flight: AtomicUsize::new(0),
            success: AtomicU64::new(0),
            fail: AtomicU64::new(0),
            last_rtt_ns: AtomicU64::new(0),
        }
    }

    pub fn clone_with_metadata(&self, endpoint: Endpoint, weight: u32) -> Self {
        let node = Self::new(endpoint, weight);
        let in_flight = self.in_flight.load(Ordering::Relaxed);
        let success = self.success.load(Ordering::Relaxed);
        let fail = self.fail.load(Ordering::Relaxed);
        let last_rtt = self.last_rtt_ns.load(Ordering::Relaxed);

        let cloned = node;
        cloned.in_flight.store(in_flight, Ordering::Relaxed);
        cloned.success.store(success, Ordering::Relaxed);
        cloned.fail.store(fail, Ordering::Relaxed);
        cloned.last_rtt_ns.store(last_rtt, Ordering::Relaxed);

        cloned
    }
}
