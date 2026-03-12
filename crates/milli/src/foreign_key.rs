use deserr::Deserr;
use heed::{
    types::{SerdeJson, Str},
    RoTxn, RwTxn,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{index::main_key, Index};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
#[derive(Default)]
pub struct ForeignKey {
    // Index uid of the foreign index
    pub foreign_index_uid: String,
    // Field name of the current index documents containing document ids of the foreign index
    pub field_name: String,
}

impl Index {
    /* foreign keys */
    pub(crate) fn put_foreign_keys(
        &self,
        wtxn: &mut RwTxn<'_>,
        keys: &[ForeignKey],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(wtxn, main_key::FOREIGN_KEYS_KEY, &keys)
    }

    pub(crate) fn delete_foreign_keys(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::FOREIGN_KEYS_KEY)
    }

    pub fn foreign_keys(&self, rtxn: &RoTxn<'_>) -> heed::Result<Vec<ForeignKey>> {
        self.main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::FOREIGN_KEYS_KEY)
            .map(|keys| keys.unwrap_or_default())
    }
}
