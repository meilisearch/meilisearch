pub mod network {
    use milli::sharding::Shards;

    use crate::network::Network;

    impl Network {
        pub fn shards(&self) -> Option<Shards> {
            None
        }

        pub fn sharding(&self) -> bool {
            // always false in CE
            false
        }
    }
}
