pub mod sharding {
    pub struct Shards;

    impl Shards {
        pub fn must_process(&self, _docid: &str) -> bool {
            true
        }
    }
}
