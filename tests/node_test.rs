use std::sync::Arc;
use volo_loadbalance::node::{Endpoint, Node};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let endpoint = Endpoint {
            id: 1,
            #[cfg(feature = "volo-adapter")]
            address: "127.0.0.1:8080"
                .parse::<std::net::SocketAddr>()
                .unwrap()
                .into(),
            #[cfg(not(feature = "volo-adapter"))]
            address: "127.0.0.1:8080".to_string(),
        };
        let node = Node::new(endpoint, 10);

        assert_eq!(node.weight, 10);
        assert_eq!(node.in_flight.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(node.success.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(node.fail.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(
            node.last_rtt_ns.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_node_atomic_operations() {
        let endpoint = Endpoint {
            id: 2,
            #[cfg(feature = "volo-adapter")]
            address: "127.0.0.1:8081"
                .parse::<std::net::SocketAddr>()
                .unwrap()
                .into(),
            #[cfg(not(feature = "volo-adapter"))]
            address: "127.0.0.1:8081".to_string(),
        };
        let node = Arc::new(Node::new(endpoint, 5));

        // Test atomic increment operations
        node.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        node.success
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        node.fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        node.last_rtt_ns
            .store(1000, std::sync::atomic::Ordering::Relaxed);

        assert_eq!(node.in_flight.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(node.success.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(node.fail.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(
            node.last_rtt_ns.load(std::sync::atomic::Ordering::Relaxed),
            1000
        );
    }

    #[test]
    fn test_node_clone() {
        let endpoint = Endpoint {
            id: 3,
            #[cfg(feature = "volo-adapter")]
            address: "127.0.0.1:8082"
                .parse::<std::net::SocketAddr>()
                .unwrap()
                .into(),
            #[cfg(not(feature = "volo-adapter"))]
            address: "127.0.0.1:8082".to_string(),
        };
        let node = Node::new(endpoint, 8);

        // Test node can be safely cloned and shared
        let node_arc = Arc::new(node);
        let cloned_node = node_arc.clone();

        assert_eq!(node_arc.weight, cloned_node.weight);
        assert_eq!(node_arc.endpoint.id, cloned_node.endpoint.id);
    }
}
