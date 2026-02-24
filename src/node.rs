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

    /// Create a new node with the given endpoint and weight, preserving
    /// runtime metrics (in-flight, success, fail, RTT) from `self`.
    ///
    /// Loads use `Acquire` ordering to ensure we see the most recent writes
    /// from other threads. Stores use `Relaxed` because the newly created
    /// node is not yet shared.
    pub fn clone_with_metadata(&self, endpoint: Endpoint, weight: u32) -> Self {
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let success = self.success.load(Ordering::Acquire);
        let fail = self.fail.load(Ordering::Acquire);
        let last_rtt = self.last_rtt_ns.load(Ordering::Acquire);

        Self {
            endpoint,
            weight,
            in_flight: AtomicUsize::new(in_flight),
            success: AtomicU64::new(success),
            fail: AtomicU64::new(fail),
            last_rtt_ns: AtomicU64::new(last_rtt),
        }
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        self.clone_with_metadata(self.endpoint.clone(), self.weight)
    }
}
