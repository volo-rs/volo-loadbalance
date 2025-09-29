//! Volo LoadBalance demo

use std::sync::Arc;
use volo_loadbalance::{
    node::{Endpoint, Node},
    strategy::{
        ConsistentHash, PowerOfTwoChoices, RequestMetadata, RoundRobin, WeightedRoundRobin,
    },
    BaseBalancer,
};

#[cfg(feature = "volo-adapter")]
use volo::net::Address;
#[cfg(not(feature = "volo-adapter"))]
type Address = String;

#[cfg(feature = "volo-adapter")]
fn create_address(addr: &str) -> Address {
    use std::net::SocketAddr;
    let socket_addr: SocketAddr = addr.parse().unwrap();
    Address::from(socket_addr)
}

#[cfg(not(feature = "volo-adapter"))]
fn create_address(addr: &str) -> Address {
    addr.to_string()
}

// Simple hash function example
fn hash_str(s: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[tokio::main]
async fn main() {
    println!("=== Volo LoadBalance Basic Example ===\n");

    // Create test nodes
    let nodes = vec![
        Arc::new(Node::new(
            Endpoint {
                id: 1,
                address: create_address("node1"),
            },
            1,
        )),
        Arc::new(Node::new(
            Endpoint {
                id: 2,
                address: create_address("node2"),
            },
            1,
        )),
        Arc::new(Node::new(
            Endpoint {
                id: 3,
                address: create_address("node3"),
            },
            1,
        )),
    ];

    // 1. Round Robin Strategy Example
    println!("1. Round Robin Strategy:");
    let round_robin = BaseBalancer::new(RoundRobin); // Round Robin Strategy
    round_robin.update_nodes(nodes.clone());
    let picker = round_robin.picker();

    for i in 0..5 {
        let req = RequestMetadata {
            hash_key: Some(i as u64),
        };
        if let Ok(node) = picker.pick(&req) {
            println!("   Request {} -> {}", i, node.endpoint.address);
        }
    }

    // 2. Weighted Round Robin Strategy Example
    println!("\n2. Weighted Round Robin Strategy:");
    let weighted_nodes = vec![
        Arc::new(Node::new(
            Endpoint {
                id: 1,
                address: create_address("node1"),
            },
            3,
        )), // Weight 3
        Arc::new(Node::new(
            Endpoint {
                id: 2,
                address: create_address("node2"),
            },
            2,
        )), // Weight 2
        Arc::new(Node::new(
            Endpoint {
                id: 3,
                address: create_address("node3"),
            },
            1,
        )), // Weight 1
    ];
    let weighted_rr = BaseBalancer::new(WeightedRoundRobin); // Weighted Round Robin Strategy
    weighted_rr.update_nodes(weighted_nodes);
    let weighted_picker = weighted_rr.picker();

    for i in 0..6 {
        let req = RequestMetadata {
            hash_key: Some(i as u64),
        };
        if let Ok(node) = weighted_picker.pick(&req) {
            println!("   Request {} -> {}", i, node.endpoint.address);
        }
    }

    // 3. Power of Two Choices Strategy Example
    println!("\n3. Power of Two Choices Strategy:");
    let p2c = BaseBalancer::new(PowerOfTwoChoices); // Power of Two Choices Strategy
    p2c.update_nodes(nodes.clone());
    let p2c_picker = p2c.picker();

    for i in 0..5 {
        let req = RequestMetadata {
            hash_key: Some(i as u64),
        };
        if let Ok(node) = p2c_picker.pick(&req) {
            println!("   Request {} -> {}", i, node.endpoint.address);
        }
    }

    // 4. Consistent Hash Strategy Example (Session Affinity)
    println!("\n4. Consistent Hash Strategy (Session Affinity):");
    let consistent_hash = BaseBalancer::new(ConsistentHash {
        virtual_factor: 160,
    }); // Consistent Hash Strategy
    consistent_hash.update_nodes(nodes.clone());

    let session_ids = vec!["session-123", "session-456", "session-789"];
    for session_id in session_ids {
        let req = RequestMetadata {
            hash_key: Some(hash_str(session_id)),
        };
        if let Ok(node) = consistent_hash.picker().pick(&req) {
            println!("   Session {} -> {}", session_id, node.endpoint.address);
        }
    }

    println!("\n=== Example Completed ===");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_str() {
        let hash1 = hash_str("test");
        let hash2 = hash_str("test");
        assert_eq!(hash1, hash2);
    }
}
