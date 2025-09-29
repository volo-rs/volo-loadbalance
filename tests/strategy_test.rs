use std::collections::HashMap;
use std::sync::Arc;

use volo_loadbalance::{
    error::LoadBalanceError,
    node::Node,
    strategy::{
        BalanceStrategy, BaseBalancer, ConsistentHash, LeastConnection, PowerOfTwoChoices,
        RequestMetadata, ResponseTimeWeighted, RoundRobin, WeightedRandom, WeightedRoundRobin,
    },
};

#[cfg(test)]
mod tests {
    use super::*;
    use volo_loadbalance::node::Endpoint;

    // Create test nodes
    fn create_test_nodes(count: usize, base_weight: u32) -> Vec<Arc<Node>> {
        (0..count)
            .map(|i| {
                let endpoint = Endpoint {
                    id: i as u64,
                    #[cfg(feature = "volo-adapter")]
                    address: format!("127.0.0.1:{}", 8080 + i)
                        .parse::<std::net::SocketAddr>()
                        .unwrap()
                        .into(),
                    #[cfg(not(feature = "volo-adapter"))]
                    address: format!("127.0.0.1:{}", 8080 + i),
                };
                Arc::new(Node::new(endpoint, base_weight + i as u32))
            })
            .collect()
    }

    // Create weighted test nodes
    fn create_weighted_test_nodes() -> Vec<Arc<Node>> {
        vec![
            Arc::new(Node::new(
                Endpoint {
                    id: 1,
                    #[cfg(feature = "volo-adapter")]
                    address: "127.0.0.1:8081"
                        .parse::<std::net::SocketAddr>()
                        .unwrap()
                        .into(),
                    #[cfg(not(feature = "volo-adapter"))]
                    address: "127.0.0.1:8081".to_string(),
                },
                10, // weight 10
            )),
            Arc::new(Node::new(
                Endpoint {
                    id: 2,
                    #[cfg(feature = "volo-adapter")]
                    address: "127.0.0.1:8082"
                        .parse::<std::net::SocketAddr>()
                        .unwrap()
                        .into(),
                    #[cfg(not(feature = "volo-adapter"))]
                    address: "127.0.0.1:8082".to_string(),
                },
                20, // weight 20
            )),
            Arc::new(Node::new(
                Endpoint {
                    id: 3,
                    #[cfg(feature = "volo-adapter")]
                    address: "127.0.0.1:8083"
                        .parse::<std::net::SocketAddr>()
                        .unwrap()
                        .into(),
                    #[cfg(not(feature = "volo-adapter"))]
                    address: "127.0.0.1:8083".to_string(),
                },
                30, // weight 30
            )),
        ]
    }

    #[test]
    fn test_round_robin_basic() {
        let nodes = create_test_nodes(3, 1);
        let strategy = RoundRobin;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        // Test round-robin selection
        let req = RequestMetadata { hash_key: None };
        let node1 = picker.pick(&req).unwrap();
        let node2 = picker.pick(&req).unwrap();
        let node3 = picker.pick(&req).unwrap();
        let node4 = picker.pick(&req).unwrap();

        assert_eq!(node1.endpoint.id, 0);
        assert_eq!(node2.endpoint.id, 1);
        assert_eq!(node3.endpoint.id, 2);
        assert_eq!(node4.endpoint.id, 0); // Back to the first node
    }

    #[test]
    fn test_round_robin_empty_nodes() {
        let strategy = RoundRobin;
        let picker = strategy.build_picker(Arc::new(Vec::new()));

        let req = RequestMetadata { hash_key: None };
        let result = picker.pick(&req);

        assert!(matches!(result, Err(LoadBalanceError::NoAvailableNodes)));
    }

    #[test]
    fn test_weighted_round_robin_distribution() {
        let nodes = create_weighted_test_nodes();
        let strategy = WeightedRoundRobin;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        let req = RequestMetadata { hash_key: None };
        let mut selection_count = HashMap::new();

        // Select enough times to verify the distribution
        for _ in 0..600 {
            let node = picker.pick(&req).unwrap();
            *selection_count.entry(node.endpoint.id).or_insert(0) += 1;
        }

        // Verify the weight distribution is roughly correct
        let count1 = selection_count.get(&1).unwrap_or(&0);
        let count2 = selection_count.get(&2).unwrap_or(&0);
        let count3 = selection_count.get(&3).unwrap_or(&0);

        // Weight ratio is 10:20:30 = 1:2:3
        // Total selection count is 600, expected distribution is 100:200:300
        assert!(*count1 > 80 && *count1 < 120); // Node 1 selected ~100 times
        assert!(*count2 > 180 && *count2 < 220); // Node 2 selected ~200 times
        assert!(*count3 > 280 && *count3 < 320); // Node 3 selected ~300 times
    }

    #[test]
    fn test_power_of_two_choices() {
        let nodes = create_test_nodes(4, 1);
        let strategy = PowerOfTwoChoices;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        let req = RequestMetadata { hash_key: None };

        // Verify the algorithm works by multiple selections
        for _ in 0..10 {
            let node = picker.pick(&req).unwrap();
            assert!(node.endpoint.id < 4);
        }
    }

    #[test]
    fn test_power_of_two_choices_single_node() {
        let nodes = create_test_nodes(1, 1);
        let strategy = PowerOfTwoChoices;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        let req = RequestMetadata { hash_key: None };
        let node = picker.pick(&req).unwrap();

        assert_eq!(node.endpoint.id, 0);
    }

    #[test]
    fn test_weighted_random_distribution() {
        let nodes = create_weighted_test_nodes();
        let strategy = WeightedRandom;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        let req = RequestMetadata { hash_key: None };
        let mut selection_count = HashMap::new();

        // Select enough times to verify the distribution
        for _ in 0..6000 {
            let node = picker.pick(&req).unwrap();
            *selection_count.entry(node.endpoint.id).or_insert(0) += 1;
        }

        let count1 = selection_count.get(&1).unwrap_or(&0);
        let count2 = selection_count.get(&2).unwrap_or(&0);
        let count3 = selection_count.get(&3).unwrap_or(&0);

        // Weight ratio is 10:20:30 = 1:2:3
        // Total weight is 60, expected distribution is 10/60, 20/60, 30/60
        let total = count1 + count2 + count3;
        let ratio1 = *count1 as f64 / total as f64;
        let ratio2 = *count2 as f64 / total as f64;
        let ratio3 = *count3 as f64 / total as f64;

        assert!((ratio1 - 1.0 / 6.0).abs() < 0.05); // Node 1 is approximately 16.7%
        assert!((ratio2 - 2.0 / 6.0).abs() < 0.05); // Node 2 is approximately 33.3%
        assert!((ratio3 - 3.0 / 6.0).abs() < 0.05); // Node 3 is approximately 50%
    }

    #[test]
    fn test_least_connection() {
        let nodes = create_test_nodes(3, 1);
        let strategy = LeastConnection;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        let req = RequestMetadata { hash_key: None };

        // Initially, all nodes have 0 connections, so the first node should be selected
        let node1 = picker.pick(&req).unwrap();
        assert_eq!(node1.endpoint.id, 0);

        // Increase the connection count of node 2
        nodes[1]
            .in_flight
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);

        // Now select the node with the least connections (node 0 or node 2)
        let node2 = picker.pick(&req).unwrap();
        assert!(node2.endpoint.id == 0 || node2.endpoint.id == 2);

        // Increase the connection count of all nodes, but node 0 has the least
        nodes[0]
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        nodes[2]
            .in_flight
            .fetch_add(3, std::sync::atomic::Ordering::Relaxed);

        let node3 = picker.pick(&req).unwrap();
        assert_eq!(node3.endpoint.id, 0); // Node 0 has the least connections (1 < 5 and 3)
    }

    #[test]
    fn test_response_time_weighted() {
        let nodes = create_test_nodes(3, 1);
        let strategy = ResponseTimeWeighted;
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        let req = RequestMetadata { hash_key: None };

        // Set different response times
        nodes[0]
            .last_rtt_ns
            .store(100_000_000, std::sync::atomic::Ordering::Relaxed); // 100ms
        nodes[1]
            .last_rtt_ns
            .store(50_000_000, std::sync::atomic::Ordering::Relaxed); // 50ms
        nodes[2]
            .last_rtt_ns
            .store(10_000_000, std::sync::atomic::Ordering::Relaxed); // 10ms

        // The node with the shortest response time should be prioritized
        let node = picker.pick(&req).unwrap();
        assert_eq!(node.endpoint.id, 2); // Node 2 has the shortest response time
    }

    #[test]
    fn test_consistent_hash_basic() {
        let nodes = create_test_nodes(3, 1);
        let strategy = ConsistentHash {
            virtual_factor: 160,
        };
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        // Test valid hash key
        let req = RequestMetadata {
            hash_key: Some(12345),
        };
        let node = picker.pick(&req).unwrap();

        // The same hash key should return the same node
        let node2 = picker.pick(&req).unwrap();
        assert_eq!(node.endpoint.id, node2.endpoint.id);

        // Different hash keys may return different nodes
        let req3 = RequestMetadata {
            hash_key: Some(67890),
        };
        let _node3 = picker.pick(&req3).unwrap();
        // Note: Different hash keys may return the same node, which is normal
    }

    #[test]
    fn test_consistent_hash_missing_key() {
        let nodes = create_test_nodes(3, 1);
        let strategy = ConsistentHash {
            virtual_factor: 160,
        };
        let picker = strategy.build_picker(Arc::new(nodes.clone()));

        // Test missing hash key scenario
        let req = RequestMetadata { hash_key: None };
        let result = picker.pick(&req);

        assert!(matches!(result, Err(LoadBalanceError::MissingHashKey)));
    }

    #[test]
    fn test_base_balancer_integration() {
        let nodes = create_test_nodes(3, 1);
        let balancer = BaseBalancer::new(RoundRobin);

        // Update the node list
        balancer.update_nodes(nodes.clone());

        // Get the picker and test selection
        let picker = balancer.picker();
        let req = RequestMetadata { hash_key: None };

        let node1 = picker.pick(&req).unwrap();
        let node2 = picker.pick(&req).unwrap();
        let node3 = picker.pick(&req).unwrap();

        assert_eq!(node1.endpoint.id, 0);
        assert_eq!(node2.endpoint.id, 1);
        assert_eq!(node3.endpoint.id, 2);
    }

    #[test]
    fn test_base_balancer_empty_nodes() {
        let balancer = BaseBalancer::new(RoundRobin);

        // Initialize with an empty node list
        balancer.update_nodes(Vec::new());

        let picker = balancer.picker();
        let req = RequestMetadata { hash_key: None };
        let result = picker.pick(&req);

        assert!(matches!(result, Err(LoadBalanceError::NoAvailableNodes)));
    }

    #[test]
    fn test_request_metadata() {
        let metadata = RequestMetadata { hash_key: Some(42) };
        assert_eq!(metadata.hash_key, Some(42));

        let metadata2 = RequestMetadata { hash_key: None };
        assert_eq!(metadata2.hash_key, None);

        // Test cloning
        let cloned = metadata.clone();
        assert_eq!(cloned.hash_key, Some(42));
    }
}
