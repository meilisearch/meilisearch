pub mod network {
    use milli::update::new::indexer::current_edition::sharding::Shards;

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
