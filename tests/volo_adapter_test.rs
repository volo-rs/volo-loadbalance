#[cfg(feature = "volo-adapter")]
mod volo_adapter_tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use volo::context::Endpoint;
    use volo::discovery::{Change, Discover, Instance};
    use volo::loadbalance::LoadBalance;
    use volo::net::Address;
    use volo_loadbalance::adapter::volo_adapter::*;
    // use volo_loadbalance::strategy::RoundRobin;

    // Mock service discoverer
    struct MockDiscover {
        instances: Vec<Arc<Instance>>,
    }

    impl MockDiscover {
        fn new(instances: Vec<Arc<Instance>>) -> Self {
            Self { instances }
        }
    }

    impl Discover for MockDiscover {
        type Key = String;
        type Error = Box<dyn std::error::Error + Send + Sync>;

        fn key(&self, _endpoint: &Endpoint) -> Self::Key {
            "test_key".to_string()
        }

        async fn discover(&self, _endpoint: &Endpoint) -> Result<Vec<Arc<Instance>>, Self::Error> {
            Ok(self.instances.clone())
        }

        fn watch(
            &self,
            _keys: Option<&[Self::Key]>,
        ) -> Option<async_broadcast::Receiver<Change<Self::Key>>> {
            None
        }
    }

    #[tokio::test]
    async fn test_volo_loadbalancer_creation() {
        let lb = round_robin();
        assert!(std::mem::size_of_val(&lb) > 0);
    }

    #[tokio::test]
    async fn test_volo_loadbalancer_with_instances() {
        let lb = round_robin();
        let discover = MockDiscover::new(vec![
            Arc::new(Instance {
                address: "127.0.0.1:8080"
                    .parse::<std::net::SocketAddr>()
                    .unwrap()
                    .into(),
                weight: 10,
                tags: Default::default(),
            }),
            Arc::new(Instance {
                address: "127.0.0.1:8081"
                    .parse::<std::net::SocketAddr>()
                    .unwrap()
                    .into(),
                weight: 20,
                tags: Default::default(),
            }),
        ]);

        let endpoint = Endpoint {
            service_name: "test_service".to_string().into(),
            address: Some(Address::from(
                "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            )),
            tags: Default::default(),
            faststr_tags: Default::default(),
        };

        let result = lb
            .get_picker(
                &endpoint,
                &volo::discovery::StaticDiscover::new(discover.instances.clone()),
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_volo_loadbalancer_empty_instances() {
        let lb = round_robin();
        let discover = MockDiscover::new(vec![]);
        let endpoint = Endpoint {
            service_name: "test_service".to_string().into(),
            address: Some(Address::from(
                "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            )),
            tags: Default::default(),
            faststr_tags: Default::default(),
        };

        let result = lb
            .get_picker(
                &endpoint,
                &volo::discovery::StaticDiscover::new(discover.instances.clone()),
            )
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_volo_instance_iter() {
        // Build a RoundRobin picker with 3 nodes and verify the iterator
        // returns different addresses in round-robin order.
        use volo_loadbalance::strategy::{BalanceStrategy, RoundRobin};
        use volo_loadbalance::node::{Endpoint as InternalEndpoint, Node};

        let nodes: Vec<Arc<Node>> = (0..3)
            .map(|i| {
                Arc::new(Node::new(
                    InternalEndpoint {
                        id: i as u64,
                        address: format!("127.0.0.1:{}", 7000 + i)
                            .parse::<SocketAddr>()
                            .unwrap()
                            .into(),
                    },
                    1,
                ))
            })
            .collect();

        let picker = RoundRobin.build_picker(Arc::new(nodes));
        let mut iter = VoloInstanceIter::new(picker);

        let a1 = iter.next().unwrap();
        let a2 = iter.next().unwrap();
        let a3 = iter.next().unwrap();
        // RR should cycle through distinct addresses.
        assert_ne!(a1, a2);
        assert_ne!(a2, a3);
        // Fourth pick wraps back to the first address.
        let a4 = iter.next().unwrap();
        assert_eq!(a1, a4);
    }

    #[test]
    fn test_volo_instance_iter_with_hash_key() {
        use volo_loadbalance::strategy::{BalanceStrategy, ConsistentHash};
        use volo_loadbalance::node::{Endpoint as InternalEndpoint, Node};

        let nodes: Vec<Arc<Node>> = (0..3)
            .map(|i| {
                Arc::new(Node::new(
                    InternalEndpoint {
                        id: i as u64,
                        address: format!("127.0.0.1:{}", 7100 + i)
                            .parse::<SocketAddr>()
                            .unwrap()
                            .into(),
                    },
                    1,
                ))
            })
            .collect();

        let strategy = ConsistentHash { virtual_factor: 100 };
        let picker = strategy.build_picker(Arc::new(nodes));

        // Without hash key the iterator should return None (MissingHashKey error).
        let mut iter_no_key = VoloInstanceIter::new(picker.clone());
        assert!(iter_no_key.next().is_none());

        // Use with_hash_key builder to set the key.
        let iter_with_key = VoloInstanceIter::new(picker.clone())
            .with_hash_key(42);

        // The iterator should now return addresses deterministically.
        let mut iter = iter_with_key;
        let addr1 = iter.next().unwrap();
        // Rebuild with same key — must get the same address.
        let mut iter2 = VoloInstanceIter::new(picker).with_hash_key(42);
        let addr2 = iter2.next().unwrap();
        assert_eq!(addr1, addr2);
    }

    #[test]
    fn test_volo_instance_iter_set_hash_key() {
        use volo_loadbalance::strategy::{BalanceStrategy, ConsistentHash};
        use volo_loadbalance::node::{Endpoint as InternalEndpoint, Node};

        let nodes: Vec<Arc<Node>> = (0..3)
            .map(|i| {
                Arc::new(Node::new(
                    InternalEndpoint {
                        id: i as u64,
                        address: format!("127.0.0.1:{}", 7200 + i)
                            .parse::<SocketAddr>()
                            .unwrap()
                            .into(),
                    },
                    1,
                ))
            })
            .collect();

        let strategy = ConsistentHash { virtual_factor: 100 };
        let picker = strategy.build_picker(Arc::new(nodes));

        let mut iter = VoloInstanceIter::new(picker);

        // Initially None — consistent hash should fail.
        assert!(iter.next().is_none());

        // Set a hash key via set_hash_key.
        iter.set_hash_key(Some(12345));
        assert!(iter.next().is_some());

        // Clear the hash key.
        iter.set_hash_key(None);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_convenience_constructors() {
        // Test all convenience constructors work correctly
        let _rr = round_robin();
        let _wrr = weighted_round_robin();
        let _p2c = power_of_two_choices();
        let _wr = weighted_random();
        let _lc = least_connection();
        let _rtw = response_time_weighted();
        let _ch = consistent_hash();

        assert!(true);
    }
}

#[cfg(not(feature = "volo-adapter"))]
mod volo_adapter_tests {
    #[test]
    fn test_volo_adapter_disabled() {
        assert!(true);
    }
}
