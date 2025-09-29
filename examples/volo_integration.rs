use std::sync::Arc;
#[cfg(feature = "volo-adapter")]
use volo::discovery::{Discover, StaticDiscover};
#[cfg(feature = "volo-adapter")]
use volo::loadbalance::LoadBalance;
#[cfg(feature = "volo-adapter")]
use volo::net::Address;
#[cfg(feature = "volo-adapter")]
use volo_loadbalance::adapter::volo_adapter;

/// Create static service discovery
#[cfg(feature = "volo-adapter")]
fn create_static_discover(addrs: Vec<&str>) -> StaticDiscover {
    let instances = addrs
        .into_iter()
        .map(|addr| {
            Arc::new(volo::discovery::Instance {
                address: Address::Ip(addr.parse().unwrap()),
                weight: 1,
                tags: Default::default(),
            })
        })
        .collect();

    StaticDiscover::new(instances)
}

#[tokio::main]
async fn main() {
    #[cfg(not(feature = "volo-adapter"))]
    {
        println!("=== Volo LoadBalance Integration Example ===\n");
        println!("Please run this example with --features volo-adapter");
        println!("=== Example Skipped ===");
        return;
    }

    #[cfg(feature = "volo-adapter")]
    {
        println!("=== Volo LoadBalance Integration Example ===\n");

        // Create static service discovery
        let discover = create_static_discover(vec![
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "127.0.0.1:8082",
            "127.0.0.1:8083",
        ]);

        // Create load balancer
        let load_balancer = volo_adapter::round_robin();

        // Create endpoint
        let endpoint = volo::context::Endpoint {
            service_name: "test-service".into(),
            ..Default::default()
        };

        println!("Service Discovery Instances:");
        let instances = discover.discover(&endpoint).await.unwrap();
        println!("  {:?}", instances);

        println!("\nLoadBalance Selection:");

        // Perform multiple load balancing selections
        for i in 0..5 {
            match load_balancer.get_picker(&endpoint, &discover).await {
                Ok(mut picker) => {
                    if let Some(instance) = picker.next() {
                        println!("  Request {} -> {:?}", i, instance);
                    }
                }
                Err(e) => {
                    println!("  Request {} -> Error: {}", i, e);
                }
            }
        }

        println!("\n=== Integration Example Completed ===");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_discover() {
        let discover = create_static_discover(vec!["127.0.0.1:8080"]);
        let endpoint = volo::context::Endpoint {
            service_name: "test".to_string(),
            ..Default::default()
        };
        let instances = discover.discover(&endpoint).await.unwrap();
        assert_eq!(instances.len(), 1);
    }
}
