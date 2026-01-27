use super::{Shards, Shard};

impl Shards {
    pub fn processing_shard<'a>(&'a self, _docid: &str) -> Option<&'a Shard> {
        None
    }
}
