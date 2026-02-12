use super::{Shard, Shards};

impl Shards {
    pub fn processing_shard<'a>(&'a self, _docid: &str) -> Option<&'a Shard> {
        None
    }
}
