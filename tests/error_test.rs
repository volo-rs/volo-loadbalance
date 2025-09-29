use volo_loadbalance::error::LoadBalanceError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_variants() {
        // Test NoAvailableNodes error
        let no_nodes_error = LoadBalanceError::NoAvailableNodes;
        assert_eq!(format!("{}", no_nodes_error), "no available nodes");

        // Test MissingHashKey error
        let missing_key_error = LoadBalanceError::MissingHashKey;
        assert_eq!(format!("{}", missing_key_error), "hash key missing");
    }

    #[test]
    fn test_error_debug() {
        let no_nodes_error = LoadBalanceError::NoAvailableNodes;
        let debug_output = format!("{:?}", no_nodes_error);

        // Ensure Debug trait works correctly
        assert!(debug_output.contains("NoAvailableNodes"));

        let missing_key_error = LoadBalanceError::MissingHashKey;
        let debug_output2 = format!("{:?}", missing_key_error);
        assert!(debug_output2.contains("MissingHashKey"));
    }

    #[test]
    fn test_error_send_sync() {
        // 验证错误类型实现了 Send 和 Sync trait
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LoadBalanceError>();
    }
}
