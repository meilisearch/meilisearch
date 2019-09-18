mod cf_tree;
mod database;
mod document_attr_key;
mod indexer;
mod number;
mod ranked_map;
mod serde;

pub use self::cf_tree::{CfTree, CfIter};
pub use self::database::{Database, Index, CustomSettingsIndex, RankingOrdering, StopWords, RankingOrder, DistinctField, RankingRules};
pub use self::number::Number;
pub use self::ranked_map::RankedMap;
pub use self::serde::{compute_document_id, extract_document_id, value_to_string};

pub type RocksDbResult<T> = Result<T, rocksdb::Error>;
