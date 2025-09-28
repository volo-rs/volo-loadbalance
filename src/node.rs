use std::sync::atomic::{AtomicU64, AtomicUsize};

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
} 
