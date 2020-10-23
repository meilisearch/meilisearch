mod available_documents_ids;
mod criterion;
mod fields_ids_map;
mod index;
mod indexing;
mod mdfs;
mod query_tokens;
mod search;
mod update_store;
pub mod heed_codec;
pub mod proximity;
pub mod subcommand;
pub mod tokenizer;

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use fxhash::{FxHasher32, FxHasher64};

pub use self::available_documents_ids::AvailableDocumentsIds;
pub use self::criterion::{Criterion, default_criteria};
pub use self::fields_ids_map::FieldsIdsMap;
pub use self::index::Index;
pub use self::search::{Search, SearchResult};
pub use self::update_store::UpdateStore;
pub use self::heed_codec::{
    RoaringBitmapCodec, BEU32StrCodec, StrStrU8Codec,
    ObkvCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec,
};

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type Attribute = u32;
pub type Position = u32;
