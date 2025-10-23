use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ahash::AHasher;
use volo::discovery::{Change, Discover, Instance};
use volo::net::Address;

use volo::loadbalance::error::LoadBalanceError;
use volo::loadbalance::LoadBalance;

use crate::node::Node as InternalNode;
use crate::strategy::{BalanceStrategy, RequestMetadata};

type DiscoverKey = <volo::discovery::StaticDiscover as Discover>::Key;

struct PickerCacheEntry {
    picker: Arc<dyn crate::strategy::Picker>,
}

/// Volo LoadBalancer Adapter
pub struct VoloLoadBalancer<S: BalanceStrategy> {
    strategy: S,
    picker_cache: Arc<parking_lot::RwLock<HashMap<String, PickerCacheEntry>>>,
    node_cache: Arc<parking_lot::RwLock<HashMap<String, HashMap<u64, Arc<InternalNode>>>>>,
    key_index: Arc<parking_lot::RwLock<HashMap<DiscoverKey, HashSet<String>>>>,
}

impl<S: BalanceStrategy> VoloLoadBalancer<S> {
    pub fn new(strategy: S) -> Self {
        Self {
            strategy,
            picker_cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            node_cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            key_index: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    fn convert_instances_to_nodes(
        &self,
        cache_key: &str,
        instances: &[Arc<Instance>],
    ) -> Vec<Arc<InternalNode>> {
        self.sync_instances(cache_key, instances)
    }

    fn sync_instances(
        &self,
        cache_key: &str,
        instances: &[Arc<Instance>],
    ) -> Vec<Arc<InternalNode>> {
        let cache_key_owned = cache_key.to_owned();
        let mut state_guard = self.node_cache.write();
        let mut seen = HashSet::with_capacity(instances.len());
        let mut nodes = Vec::with_capacity(instances.len());

        let should_remove = {
            let nodes_map = state_guard
                .entry(cache_key_owned.clone())
                .or_insert_with(HashMap::new);

            for instance in instances {
                let node_id = Self::compute_instance_id(instance);
                let endpoint = crate::node::Endpoint {
                    id: node_id,
                    address: instance.address.clone(),
                };
                let weight = instance.weight;

                let node = match nodes_map.get(&node_id) {
                    Some(existing)
                        if existing.weight == weight
                            && existing.endpoint.address == endpoint.address =>
                    {
                        existing.clone()
                    }
                    Some(existing) => {
                        let rebuilt = Arc::new(existing.clone_with_metadata(endpoint, weight));
                        nodes_map.insert(node_id, rebuilt.clone());
                        rebuilt
                    }
                    None => {
                        let node = Arc::new(InternalNode::new(endpoint, weight));
                        nodes_map.insert(node_id, node.clone());
                        node
                    }
                };

                nodes.push(node);
                seen.insert(node_id);
            }

            nodes_map.retain(|id, _| seen.contains(id));
            nodes_map.is_empty()
        };

        if should_remove {
            state_guard.remove(&cache_key_owned);
        }

        nodes
    }

    fn compute_instance_id(instance: &Instance) -> u64 {
        let mut hasher = AHasher::default();
        instance.address.hash(&mut hasher);

        if !instance.tags.is_empty() {
            let mut tags: Vec<_> = instance.tags.iter().collect();
            tags.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
            for (k, v) in tags {
                k.hash(&mut hasher);
                v.hash(&mut hasher);
            }
        }

        hasher.finish()
    }

    fn get_cache_key(
        &self,
        endpoint: &volo::context::Endpoint,
        discover_key: &DiscoverKey,
    ) -> String {
        let mut hasher = AHasher::default();
        endpoint.service_name.hash(&mut hasher);
        if let Some(addr) = &endpoint.address {
            addr.hash(&mut hasher);
        }

        let mut fast_entries: Vec<_> = endpoint
            .faststr_tags
            .iter()
            .map(|(type_id, value)| {
                let mut type_hasher = AHasher::default();
                type_id.hash(&mut type_hasher);
                let type_hash = type_hasher.finish();

                let mut value_hasher = AHasher::default();
                value.hash(&mut value_hasher);
                let value_hash = value_hasher.finish();

                (type_hash, value_hash)
            })
            .collect();
        fast_entries.sort_by_key(|(type_hash, _)| *type_hash);
        for (type_hash, value_hash) in fast_entries {
            hasher.write_u64(type_hash);
            hasher.write_u64(value_hash);
        }

        discover_key.hash(&mut hasher);

        format!("{}:{:016x}", endpoint.service_name, hasher.finish())
    }

    fn update_key_index(&self, discover_key: DiscoverKey, cache_key: String) {
        let mut index = self.key_index.write();
        index
            .entry(discover_key)
            .or_insert_with(HashSet::new)
            .insert(cache_key);
    }

    fn handle_rebalance(&self, changes: Change<DiscoverKey>) {
        let cache_keys = {
            let index = self.key_index.read();
            index
                .get(&changes.key)
                .map(|set| set.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        };

        if cache_keys.is_empty() {
            return;
        }

        for cache_key in &cache_keys {
            self.sync_instances(cache_key, &changes.all);
        }

        {
            let mut cache = self.picker_cache.write();
            for cache_key in &cache_keys {
                cache.remove(cache_key);
            }
        }

        let mut index = self.key_index.write();
        if let Some(set) = index.get_mut(&changes.key) {
            for cache_key in &cache_keys {
                set.remove(cache_key);
            }
            if set.is_empty() {
                index.remove(&changes.key);
            }
        }
    }
}

impl<S: BalanceStrategy + 'static> LoadBalance<volo::discovery::StaticDiscover>
    for VoloLoadBalancer<S>
{
    type InstanceIter = VoloInstanceIter;

    async fn get_picker(
        &self,
        endpoint: &volo::context::Endpoint,
        discover: &volo::discovery::StaticDiscover,
    ) -> Result<Self::InstanceIter, LoadBalanceError> {
        let discover_key = discover.key(endpoint);
        let cache_key = self.get_cache_key(endpoint, &discover_key);

        // Check cache
        {
            let cache = self.picker_cache.read();
            if let Some(entry) = cache.get(&cache_key) {
                return Ok(VoloInstanceIter {
                    picker: entry.picker.clone(),
                });
            }
        }

        // Get instances from service discovery
        let instances = discover
            .discover(endpoint)
            .await
            .map_err(|e| LoadBalanceError::Discover(Box::new(e)))?;

        if instances.is_empty() {
            // When no available instances are found, return a custom error
            return Err(LoadBalanceError::from(Box::<
                dyn std::error::Error + Send + Sync,
            >::from(
                "No available instances found"
            )));
        }

        // Convert to internal node format
        let nodes = self.convert_instances_to_nodes(&cache_key, &instances);
        let nodes_arc = Arc::new(nodes);

        // Create picker
        let picker = self.strategy.build_picker(nodes_arc);

        // Update cache
        {
            let mut cache = self.picker_cache.write();
            cache.insert(
                cache_key.clone(),
                PickerCacheEntry {
                    picker: picker.clone(),
                },
            );
        }

        self.update_key_index(discover_key, cache_key);

        Ok(VoloInstanceIter { picker })
    }

    fn rebalance(&self, changes: Change<<volo::discovery::StaticDiscover as Discover>::Key>) {
        self.handle_rebalance(changes);
    }
}

/// Volo Instance Iterator
pub struct VoloInstanceIter {
    picker: Arc<dyn crate::strategy::Picker>,
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
    VoloLoadBalancer::new(crate::strategy::ConsistentHash::default())
}
