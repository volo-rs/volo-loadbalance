#[derive(Clone, Debug, Default)]
pub struct NodeMeta {
	pub weight: u32,
}

#[derive(Clone, Debug)]
pub struct BalanceConfig {
	pub default_weight: u32,
}

impl Default for BalanceConfig {
	fn default() -> Self {
		Self { default_weight: 100 }
	}
} 