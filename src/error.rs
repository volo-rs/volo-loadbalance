use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadBalanceError {
	#[error("no available nodes")] 
	NoAvailableNodes,
	#[error("hash key missing")] 
	MissingHashKey,
} 