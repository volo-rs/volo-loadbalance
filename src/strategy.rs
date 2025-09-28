use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ahash::AHasher;
use parking_lot::RwLock;
use rand::distributions::{Distribution, WeightedIndex};
use rand::Rng;

use crate::error::LoadBalanceError;
use crate::node::Node;

#[derive(Clone, Debug, Default)]
pub struct RequestMetadata {
	pub hash_key: Option<u64>,
}

pub trait Picker: Send + Sync {
	fn pick(&self, req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError>;
}

pub trait BalanceStrategy: Send + Sync {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker>;
}

#[derive(Clone)]
pub struct BaseBalancer<S: BalanceStrategy> {
	strategy: S,
	nodes: Arc<RwLock<Vec<Arc<Node>>>>,
}

impl<S: BalanceStrategy> BaseBalancer<S> {
	pub fn new(strategy: S) -> Self {
		Self { strategy, nodes: Arc::new(RwLock::new(Vec::new())) }
	}
	pub fn update_nodes(&self, nodes: Vec<Arc<Node>>) {
		*self.nodes.write() = nodes;
	}
	pub fn picker(&self) -> Arc<dyn Picker> {
		let nodes = Arc::new(self.nodes.read().clone());
		self.strategy.build_picker(nodes)
	}
}

// Round Robin
pub struct RoundRobin;

impl BalanceStrategy for RoundRobin {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		Arc::new(RoundRobinPicker { nodes, idx: parking_lot::Mutex::new(0usize) })
	}
}

struct RoundRobinPicker {
	nodes: Arc<Vec<Arc<Node>>>,
	idx: parking_lot::Mutex<usize>,
}

impl Picker for RoundRobinPicker {
	fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len();
		if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		let mut g = self.idx.lock();
		let i = *g % len;
		*g = g.wrapping_add(1);
		Ok(self.nodes[i].clone())
	}
}

// Weighted Round Robin (smooth)
pub struct WeightedRoundRobin;

impl BalanceStrategy for WeightedRoundRobin {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		Arc::new(WRRPicker::new(nodes))
	}
}

struct WRRPicker {
	nodes: Arc<Vec<Arc<Node>>>,
	cw: parking_lot::Mutex<i32>,
	idx: parking_lot::Mutex<usize>,
	max_w: i32,
	gcd_w: i32,
	weights: Vec<i32>,
}

impl WRRPicker {
	fn gcd(a: i32, b: i32) -> i32 { if b == 0 { a } else { Self::gcd(b, a % b) } }
	fn new(nodes: Arc<Vec<Arc<Node>>>) -> Self {
		let mut max_w = 0i32;
		let mut gcd_w = 0i32;
		let mut weights = Vec::new();
		for n in nodes.iter() {
			let w = n.weight as i32;
			if w > 0 {
				max_w = max_w.max(w);
				gcd_w = if gcd_w == 0 { w } else { Self::gcd(gcd_w, w) };
			}
			weights.push(w);
		}
		Self { nodes, cw: parking_lot::Mutex::new(0), idx: parking_lot::Mutex::new(usize::MAX), max_w, gcd_w: gcd_w.max(1), weights }
	}
}

impl Picker for WRRPicker {
	fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len();
		if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		let mut i = self.idx.lock();
		let mut cw = self.cw.lock();
		loop {
			*i = if *i == usize::MAX { 0 } else { (*i + 1) % len };
			if *i == 0 { *cw = (*cw - self.gcd_w).max(0); if *cw == 0 { *cw = self.max_w; } }
			if self.weights[*i] >= *cw { return Ok(self.nodes[*i].clone()); }
		}
	}
}

// P2C (Power of Two Choices)
pub struct PowerOfTwoChoices;

impl BalanceStrategy for PowerOfTwoChoices {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		Arc::new(P2CPicker { nodes })
	}
}

struct P2CPicker { nodes: Arc<Vec<Arc<Node>>> }

impl Picker for P2CPicker {
	fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len();
		if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		if len == 1 { return Ok(self.nodes[0].clone()); }

		let mut rng = rand::thread_rng();
		let a = rng.gen_range(0..len);
		
		let b = loop { let x = rng.gen_range(0..len); if x != a { break x } };
		let na = self.nodes[a].in_flight.load(std::sync::atomic::Ordering::Relaxed);
		let nb = self.nodes[b].in_flight.load(std::sync::atomic::Ordering::Relaxed);
		Ok(if na <= nb { self.nodes[a].clone() } else { self.nodes[b].clone() })
	}
}

// Weighted Random
pub struct WeightedRandom;

impl BalanceStrategy for WeightedRandom {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		let weights: Vec<i32> = nodes.iter().map(|n| n.weight as i32).collect();
		let dist = WeightedIndex::new(weights.iter().map(|w| (*w as f64).max(0.0))).ok();
		Arc::new(WeightedRandomPicker { nodes, dist })
	}
}

struct WeightedRandomPicker {
	nodes: Arc<Vec<Arc<Node>>>,
	dist: Option<WeightedIndex<f64>>,
}

impl Picker for WeightedRandomPicker {
	fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len();
		if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		if let Some(dist) = &self.dist { Ok(self.nodes[dist.sample(&mut rand::thread_rng())].clone()) } else { Ok(self.nodes[0].clone()) }
	}
}

// Least Connection
pub struct LeastConnection;

impl BalanceStrategy for LeastConnection {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		Arc::new(LeastConnPicker { nodes })
	}
}

struct LeastConnPicker { nodes: Arc<Vec<Arc<Node>>> }

impl Picker for LeastConnPicker {
	fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len(); if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		let mut best = &self.nodes[0]; let mut best_load = best.in_flight.load(std::sync::atomic::Ordering::Relaxed);
		for n in self.nodes.iter().skip(1) { let load = n.in_flight.load(std::sync::atomic::Ordering::Relaxed); if load < best_load { best = n; best_load = load; } }
		Ok(best.clone())
	}
}

// Response Time Weighted (use last_rtt_ns inversely as weight hint)
pub struct ResponseTimeWeighted;

impl BalanceStrategy for ResponseTimeWeighted {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		Arc::new(RTWeightedPicker { nodes })
	}
}

struct RTWeightedPicker { nodes: Arc<Vec<Arc<Node>>> }

impl Picker for RTWeightedPicker {
	fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len(); if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		let mut best = &self.nodes[0]; let mut best_score = score(best);
		for n in self.nodes.iter().skip(1) { let s = score(n); if s > best_score { best = n; best_score = s; } }
		Ok(best.clone())
	}
}

fn score(n: &Arc<Node>) -> f64 {
	let rtt = n.last_rtt_ns.load(std::sync::atomic::Ordering::Relaxed);
	let inflight = n.in_flight.load(std::sync::atomic::Ordering::Relaxed) as u64;
	let rtt = if rtt == 0 { 1 } else { rtt };
	(1_000_000_000u64 / rtt) as f64 / (1.0 + inflight as f64)
}

// Consistent Hash
pub struct ConsistentHash;

impl BalanceStrategy for ConsistentHash {
	fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
		Arc::new(ConsistentHashPicker::new(nodes))
	}
}

struct ConsistentHashPicker {
	nodes: Arc<Vec<Arc<Node>>>,
}

impl ConsistentHashPicker {
	fn new(nodes: Arc<Vec<Arc<Node>>>) -> Self { Self { nodes } }
}

impl Picker for ConsistentHashPicker {
	fn pick(&self, req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
		let len = self.nodes.len(); if len == 0 { return Err(LoadBalanceError::NoAvailableNodes); }
		let key = req.hash_key.ok_or(LoadBalanceError::MissingHashKey)?;
		let idx = (hash64(key) % (len as u64)) as usize;
		Ok(self.nodes[idx].clone())
	}
}

fn hash64(v: u64) -> u64 {
	let mut h = AHasher::default();
	v.hash(&mut h);
	h.finish()
} 