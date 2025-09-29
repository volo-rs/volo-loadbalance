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
        Self {
            strategy,
            nodes: Arc::new(RwLock::new(Vec::new())),
        }
    }
    pub fn update_nodes(&self, nodes: Vec<Arc<Node>>) {
        *self.nodes.write() = nodes;
    }
    pub fn picker(&self) -> Arc<dyn Picker> {
        // Use cloning to get the node list, avoiding holding the read lock for a long time
        let nodes = Arc::new(self.nodes.read().clone());
        self.strategy.build_picker(nodes)
    }
}

// Round Robin
pub struct RoundRobin;

impl BalanceStrategy for RoundRobin {
    fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
        Arc::new(RoundRobinPicker {
            nodes,
            idx: parking_lot::Mutex::new(0usize),
        })
    }
}

struct RoundRobinPicker {
    nodes: Arc<Vec<Arc<Node>>>,
    idx: parking_lot::Mutex<usize>,
}

impl Picker for RoundRobinPicker {
    fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
        let len = self.nodes.len();
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }

        let mut g = self.idx.lock();
        let i = *g % len;

        // Handle possible overflow, reset to 0 when approaching usize::MAX
        if *g == usize::MAX {
            *g = 0;
        } else {
            *g += 1;
        }

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
    fn gcd(a: i32, b: i32) -> i32 {
        if b == 0 {
            a
        } else {
            Self::gcd(b, a % b)
        }
    }
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
        Self {
            nodes,
            cw: parking_lot::Mutex::new(0),
            idx: parking_lot::Mutex::new(usize::MAX),
            max_w,
            gcd_w: gcd_w.max(1),
            weights,
        }
    }
}

impl Picker for WRRPicker {
    fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
        let len = self.nodes.len();
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }

        // Check if all node weights are 0
        if self.max_w <= 0 {
            // If all weights are 0, degrade to simple polling
            let mut i = self.idx.lock();
            *i = if *i == usize::MAX { 0 } else { (*i + 1) % len };
            return Ok(self.nodes[*i].clone());
        }

        let mut i = self.idx.lock();
        let mut cw = self.cw.lock();

        // Prevent infinite loops, loop at most len*2 times
        let mut attempts = 0;
        let max_attempts = len * 2;

        loop {
            *i = if *i == usize::MAX { 0 } else { (*i + 1) % len };
            if *i == 0 {
                *cw = (*cw - self.gcd_w).max(0);
                if *cw == 0 {
                    *cw = self.max_w;
                }
            }

            // If a suitable node is found or too many attempts, return
            if self.weights[*i] >= *cw || attempts >= max_attempts {
                return Ok(self.nodes[*i].clone());
            }

            attempts += 1;
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

struct P2CPicker {
    nodes: Arc<Vec<Arc<Node>>>,
}

impl Picker for P2CPicker {
    fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
        let len = self.nodes.len();
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }
        if len == 1 {
            return Ok(self.nodes[0].clone());
        }

        let mut rng = rand::thread_rng();
        let a = rng.gen_range(0..len);

        let b = loop {
            let x = rng.gen_range(0..len);
            if x != a {
                break x;
            }
        };
        let na = self.nodes[a]
            .in_flight
            .load(std::sync::atomic::Ordering::Acquire);
        let nb = self.nodes[b]
            .in_flight
            .load(std::sync::atomic::Ordering::Acquire);
        Ok(if na <= nb {
            self.nodes[a].clone()
        } else {
            self.nodes[b].clone()
        })
    }
}

/// Weighted Random Load Balancing Strategy
///
/// Features:
/// - Random selection based on node weights
/// - Higher weight means higher probability of being selected
/// - Performance optimizations:
///   - Uses thread-local random number generator
///   - Handles cases where all weights are 0
#[derive(Clone, Debug)]
pub struct WeightedRandom;

impl BalanceStrategy for WeightedRandom {
    fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
        // Check if all node weights are 0
        let all_zero = nodes.iter().all(|n| n.weight == 0);

        // If all weights are 0, use equal weights
        let weights: Vec<f64> = if all_zero {
            nodes.iter().map(|_| 1.0).collect()
        } else {
            nodes.iter().map(|n| (n.weight as f64).max(0.0)).collect()
        };

        let dist = WeightedIndex::new(&weights).ok();
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
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }

        // If there is only one node, return directly
        if len == 1 {
            return Ok(self.nodes[0].clone());
        }

        // Use weighted distribution to select nodes
        if let Some(dist) = &self.dist {
            // Use thread-local random number generator to avoid creating a new generator each time
            let mut rng = rand::thread_rng();
            let idx = dist.sample(&mut rng);
            Ok(self.nodes[idx].clone())
        } else {
            // If there is no weight distribution, degrade to polling
            let mut rng = rand::thread_rng();
            let idx = rng.gen_range(0..len);
            Ok(self.nodes[idx].clone())
        }
    }
}

// Least Connection
pub struct LeastConnection;

impl BalanceStrategy for LeastConnection {
    fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
        Arc::new(LeastConnPicker { nodes })
    }
}

struct LeastConnPicker {
    nodes: Arc<Vec<Arc<Node>>>,
}

impl Picker for LeastConnPicker {
    fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
        let len = self.nodes.len();
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }
        let mut best = &self.nodes[0];
        let mut best_load = best.in_flight.load(std::sync::atomic::Ordering::Acquire);
        for n in self.nodes.iter().skip(1) {
            let load = n.in_flight.load(std::sync::atomic::Ordering::Acquire);
            if load < best_load {
                best = n;
                best_load = load;
            }
        }
        Ok(best.clone())
    }
}

/// Response Time Weighted Load Balancing Strategy
///
/// Features:
/// - Weighted selection based on node's recent response time (RTT)
/// - Smaller RTT means higher weight
/// - Also considers current load (in_flight)
/// - Performance optimization: pre-calculates all node scores and sorts them
#[derive(Clone, Debug)]
pub struct ResponseTimeWeighted;

impl BalanceStrategy for ResponseTimeWeighted {
    fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
        Arc::new(RTWeightedPicker { nodes })
    }
}

struct RTWeightedPicker {
    nodes: Arc<Vec<Arc<Node>>>,
}

impl Picker for RTWeightedPicker {
    fn pick(&self, _req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
        let len = self.nodes.len();
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }
        if len == 1 {
            return Ok(self.nodes[0].clone());
        }

        // Pre-calculate scores for all nodes
        let mut scores: Vec<(f64, Arc<Node>)> =
            self.nodes.iter().map(|n| (score(n), n.clone())).collect();

        // Sort by score in descending order
        scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Select the node with the highest score
        Ok(scores[0].1.clone())
    }
}

fn score(n: &Arc<Node>) -> f64 {
    // Use atomic operations to get the latest values
    let rtt = n.last_rtt_ns.load(std::sync::atomic::Ordering::Acquire);
    let inflight = n.in_flight.load(std::sync::atomic::Ordering::Acquire) as u64;

    // Handle the case where rtt is 0
    let rtt = if rtt == 0 { 1 } else { rtt };

    // Calculate response time score
    let rtt_score = (1_000_000_000u64 / rtt) as f64;

    // Calculate load factor
    let load_factor = 1.0 + inflight as f64;

    // Comprehensive score
    rtt_score / load_factor
}

// Consistent Hash
pub struct ConsistentHash {
    // Virtual node multiplier, number of virtual nodes corresponding to each real node
    pub virtual_factor: usize,
}

impl Default for ConsistentHash {
    fn default() -> Self {
        Self { virtual_factor: 10 }
    }
}

impl BalanceStrategy for ConsistentHash {
    fn build_picker(&self, nodes: Arc<Vec<Arc<Node>>>) -> Arc<dyn Picker> {
        Arc::new(ConsistentHashPicker::new(nodes, self.virtual_factor))
    }
}

struct ConsistentHashPicker {
    nodes: Arc<Vec<Arc<Node>>>,
    // Hash ring: (hash value, node index)
    ring: Vec<(u64, usize)>,
}

impl ConsistentHashPicker {
    fn new(nodes: Arc<Vec<Arc<Node>>>, virtual_factor: usize) -> Self {
        let mut ring = Vec::new();

        // Create virtual nodes for each node
        for (i, node) in nodes.iter().enumerate() {
            let weight = node.weight.max(1) as usize; // Ensure weight is at least 1
            let vnode_count = weight * virtual_factor;

            for j in 0..vnode_count {
                // Generate hash value using node address and virtual node index
                let key = format!("{node:p}:{j}");
                let hash = hash_str(&key);
                ring.push((hash, i));
            }
        }

        // Sort by hash value
        ring.sort_by_key(|&(hash, _)| hash);

        Self { nodes, ring }
    }
}

impl Picker for ConsistentHashPicker {
    fn pick(&self, req: &RequestMetadata) -> Result<Arc<Node>, LoadBalanceError> {
        let len = self.nodes.len();
        if len == 0 {
            return Err(LoadBalanceError::NoAvailableNodes);
        }

        // If there are no virtual nodes, degrade to simple hashing
        if self.ring.is_empty() {
            let key = req.hash_key.ok_or(LoadBalanceError::MissingHashKey)?;
            let idx = (hash64(key) % (len as u64)) as usize;
            return Ok(self.nodes[idx].clone());
        }

        let key = req.hash_key.ok_or(LoadBalanceError::MissingHashKey)?;
        let hash = hash64(key);

        // Binary search to find the first position greater than or equal to hash
        match self.ring.binary_search_by(|&(h, _)| h.cmp(&hash)) {
            Ok(idx) => {
                // Found exact match
                let (_, node_idx) = self.ring[idx];
                Ok(self.nodes[node_idx].clone())
            }
            Err(idx) => {
                // No exact match found, take the next node (ring)
                let idx = if idx >= self.ring.len() { 0 } else { idx };
                let (_, node_idx) = self.ring[idx];
                Ok(self.nodes[node_idx].clone())
            }
        }
    }
}

// Hash a string
fn hash_str(s: &str) -> u64 {
    let mut h = AHasher::default();
    s.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Endpoint;
    use std::net::SocketAddr;

    fn create_test_node(weight: i32, _in_flight: u64, _rtt: u64) -> Arc<Node> {
        Arc::new(Node::new(
            Endpoint {
                id: 1,
                #[cfg(feature = "volo-adapter")]
                address: volo::net::Address::from(SocketAddr::from(([127, 0, 0, 1], 8080))),
                #[cfg(not(feature = "volo-adapter"))]
                address: "127.0.0.1:8080".to_string(),
            },
            weight as u32,
        ))
    }

    #[test]
    fn test_round_robin() {
        let nodes = vec![create_test_node(1, 0, 0), create_test_node(1, 0, 0)];
        let balancer = BaseBalancer::new(RoundRobin);
        balancer.update_nodes(nodes.clone());

        let picker = balancer.picker();
        assert_eq!(picker.pick(&RequestMetadata::default()).unwrap().weight, 1);
        assert_eq!(picker.pick(&RequestMetadata::default()).unwrap().weight, 1);
    }

    #[test]
    fn test_weighted_random() {
        let nodes = vec![create_test_node(2, 0, 0), create_test_node(1, 0, 0)];
        let balancer = BaseBalancer::new(WeightedRandom);
        balancer.update_nodes(nodes.clone());

        let picker = balancer.picker();
        let mut counts = [0; 2];
        for _ in 0..1000 {
            let node = picker.pick(&RequestMetadata::default()).unwrap();
            let idx = nodes.iter().position(|n| Arc::ptr_eq(n, &node)).unwrap();
            counts[idx] += 1;
        }

        // The node with weight 2 should be selected with a probability of approximately 2/3
        assert!(counts[0] > (counts[1] as f64 * 1.5) as usize);
    }
}

fn hash64(v: u64) -> u64 {
    let mut h = AHasher::default();
    v.hash(&mut h);
    h.finish()
}
