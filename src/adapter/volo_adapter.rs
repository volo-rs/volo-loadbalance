#[cfg(feature = "volo-adapter")]
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use volo::discovery::{Discover, Instance, Change};
use volo::net::Address;
use volo::context::Endpoint;
use volo::loadbalance::error::LoadBalanceError;

use crate::strategy::{BalanceStrategy, RequestMetadata};
use crate::node::Node as InternalNode;
use crate::error::LoadBalanceError as OurLoadBalanceError;

/// Volo LoadBalancer Adapter
pub struct VoloLoadBalancer<S: BalanceStrategy> {
    strategy: S,
    nodes_cache: Arc<parking_lot::RwLock<HashMap<String, Vec<Arc<InternalNode>>>>>,
    picker_cache: Arc<parking_lot::RwLock<HashMap<String, Arc<dyn crate::strategy::Picker>>>>,
}

impl<S: BalanceStrategy> VoloLoadBalancer<S> {
    pub fn new(strategy: S) -> Self {
        Self {
            strategy,
            nodes_cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            picker_cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    fn convert_instances_to_nodes(&self, instances: &[Arc<Instance>]) -> Vec<Arc<InternalNode>> {
        instances.iter().map(|instance| {
            let endpoint = crate::node::Endpoint {
                id: 0, // Use the hash of the address as the ID
                address: instance.address.clone(),
            };
            let weight = instance.weight;
            Arc::new(InternalNode::new(endpoint, weight))
        }).collect()
    }

    fn get_cache_key(&self, endpoint: &Endpoint) -> String {
        format!("{}", endpoint.service_name)
    }
}

impl<S: BalanceStrategy> volo::LoadBalance<volo::discovery::StaticDiscover> for VoloLoadBalancer<S> {
    type InstanceIter = VoloInstanceIter;

    async fn get_picker(
        &self,
        endpoint: &Endpoint,
        discover: &volo::discovery::StaticDiscover,
    ) -> Result<Self::InstanceIter, LoadBalanceError> {
        let key = self.get_cache_key(endpoint);
        
        // Check cache
        {
            let cache = self.picker_cache.read();
            if let Some(picker) = cache.get(&key) {
                return Ok(VoloInstanceIter {
                    picker: picker.clone(),
                    current_idx: Arc::new(AtomicUsize::new(0)),
                });
            }
        }

        // Get instances from service discovery
        let instances = discover.discover(endpoint).await
            .map_err(|e| LoadBalanceError::NoAvailableNodes)?;
        
        if instances.is_empty() {
            return Err(LoadBalanceError::NoAvailableNodes);
        }

        // Convert to internal node format
        let nodes = self.convert_instances_to_nodes(&instances);
        let nodes_arc = Arc::new(nodes);
        
        // Create picker
        let picker = self.strategy.build_picker(nodes_arc);
        
        // Update cache
        {
            let mut cache = self.picker_cache.write();
            cache.insert(key, picker.clone());
        }

        Ok(VoloInstanceIter {
            picker,
            current_idx: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn rebalance(&self, changes: Change<volo::discovery::StaticDiscover::Key>) {
        // Clear related cache
        let mut cache = self.picker_cache.write();
        cache.clear();
    }
}

/// Volo Instance Iterator
pub struct VoloInstanceIter {
    picker: Arc<dyn crate::strategy::Picker>,
    current_idx: Arc<AtomicUsize>,
}

impl Iterator for VoloInstanceIter {
    type Item = Address;

    fn next(&mut self) -> Option<Self::Item> {
        let req = RequestMetadata { hash_key: None };
        match self.picker.pick(&req) {
            Ok(node) => Some(node.endpoint.address.clone()),
            Err(_) => None,
        }
    }
}

// Convenience constructors for various strategies
pub fn round_robin() -> VoloLoadBalancer<crate::strategy::RoundRobin> {
    VoloLoadBalancer::new(crate::strategy::RoundRobin)
}

pub fn weighted_round_robin() -> VoloLoadBalancer<crate::strategy::WeightedRoundRobin> {
    VoloLoadBalancer::new(crate::strategy::WeightedRoundRobin)
}

pub fn power_of_two_choices() -> VoloLoadBalancer<crate::strategy::PowerOfTwoChoices> {
    VoloLoadBalancer::new(crate::strategy::PowerOfTwoChoices)
}

pub fn weighted_random() -> VoloLoadBalancer<crate::strategy::WeightedRandom> {
    VoloLoadBalancer::new(crate::strategy::WeightedRandom)
}

pub fn least_connection() -> VoloLoadBalancer<crate::strategy::LeastConnection> {
    VoloLoadBalancer::new(crate::strategy::LeastConnection)
}

pub fn response_time_weighted() -> VoloLoadBalancer<crate::strategy::ResponseTimeWeighted> {
    VoloLoadBalancer::new(crate::strategy::ResponseTimeWeighted)
}

pub fn consistent_hash() -> VoloLoadBalancer<crate::strategy::ConsistentHash> {
    VoloLoadBalancer::new(crate::strategy::ConsistentHash)
}