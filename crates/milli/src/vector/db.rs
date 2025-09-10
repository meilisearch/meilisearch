//! Module containing types and methods to store meta-information about the embedders and fragments

use std::borrow::Cow;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use heed::types::{SerdeJson, Str, U8};
use heed::{BytesEncode, Database, RoTxn, RwTxn, Unspecified};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::vector::settings::RemoveFragments;
use crate::vector::EmbeddingConfig;
use crate::{CboRoaringBitmapCodec, DocumentId, UserError};

/// DB representation of an embedder configuration.
///
/// # Warning
///
/// This type is serialized in and deserialized from the DB, any modification should either go
/// through dumpless upgrade or be backward-compatible
#[derive(Debug, Deserialize, Serialize)]
pub struct IndexEmbeddingConfig {
    pub name: String,
    pub config: EmbeddingConfig,
    #[serde(default)]
    pub fragments: FragmentConfigs,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FragmentConfigs(Vec<FragmentConfig>);

impl FragmentConfigs {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn as_slice(&self) -> &[FragmentConfig] {
        self.0.as_slice()
    }

    pub fn into_inner(self) -> Vec<FragmentConfig> {
        self.0
    }

    pub fn remove_fragments<'a>(
        &mut self,
        fragments: impl IntoIterator<Item = &'a str>,
    ) -> Option<RemoveFragments> {
        let mut remove_fragments = Vec::new();
        for fragment in fragments {
            let Ok(index_to_remove) = self.0.binary_search_by_key(&fragment, |f| &f.name) else {
                continue;
            };
            let fragment = self.0.swap_remove(index_to_remove);
            remove_fragments.push(fragment.id);
        }
        (!remove_fragments.is_empty()).then_some(RemoveFragments { fragment_ids: remove_fragments })
    }

    pub fn add_new_fragments(
        &mut self,
        new_fragments: impl IntoIterator<Item = String>,
    ) -> crate::Result<()> {
        let mut free_indices: [bool; u8::MAX as usize] = [true; u8::MAX as usize];

        for FragmentConfig { id, name: _ } in self.0.iter() {
            free_indices[*id as usize] = false;
        }
        let mut free_indices = free_indices.iter_mut().enumerate();
        let mut find_free_index =
            move || free_indices.find(|(_, free)| **free).map(|(index, _)| index as u8);

        let mut new_fragments = new_fragments.into_iter();

        for name in &mut new_fragments {
            let id = match find_free_index() {
                Some(id) => id,
                None => {
                    let more = (&mut new_fragments).count();
                    return Err(UserError::TooManyFragments(u8::MAX as usize + more + 1).into());
                }
            };
            self.0.push(FragmentConfig { id, name });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FragmentConfig {
    pub id: u8,
    pub name: String,
}

pub struct IndexEmbeddingConfigs {
    main: Database<Unspecified, Unspecified>,
    embedder_info: Database<Str, EmbedderInfoCodec>,
}

pub struct EmbedderInfo {
    pub embedder_id: u8,
    pub embedding_status: EmbeddingStatus,
}

impl EmbedderInfo {
    pub fn to_bytes(&self) -> Result<Cow<'_, [u8]>, heed::BoxedError> {
        EmbedderInfoCodec::bytes_encode(self)
    }
}

/// Optimized struct to hold the list of documents that are `user_provided` and `must_regenerate`.
///
/// Because most documents have the same value for `user_provided` and `must_regenerate`, we store only
/// the `user_provided` and a list of the documents for which `must_regenerate` assumes the other value
/// than `user_provided`.
#[derive(Default)]
pub struct EmbeddingStatus {
    user_provided: RoaringBitmap,
    skip_regenerate_different_from_user_provided: RoaringBitmap,
}

impl EmbeddingStatus {
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a new `EmbeddingStatus` that assumes that any `user_provided` docid is also skipping regenerate.
    ///
    /// Used for migration from v1.15 and earlier DBs.
    pub(crate) fn from_user_provided(user_provided: RoaringBitmap) -> Self {
        Self { user_provided, skip_regenerate_different_from_user_provided: Default::default() }
    }

    /// Whether the document contains user-provided vectors for that embedder.
    pub fn is_user_provided(&self, docid: DocumentId) -> bool {
        self.user_provided.contains(docid)
    }

    /// Whether vectors should be regenerated for that document and that embedder.
    pub fn must_regenerate(&self, docid: DocumentId) -> bool {
        let invert = self.skip_regenerate_different_from_user_provided.contains(docid);
        let user_provided = self.user_provided.contains(docid);
        !(user_provided ^ invert)
    }

    pub fn is_user_provided_must_regenerate(&self, docid: DocumentId) -> (bool, bool) {
        let invert = self.skip_regenerate_different_from_user_provided.contains(docid);
        let user_provided = self.user_provided.contains(docid);
        (user_provided, !(user_provided ^ invert))
    }

    pub fn user_provided_docids(&self) -> &RoaringBitmap {
        &self.user_provided
    }

    pub fn skip_regenerate_docids(&self) -> RoaringBitmap {
        &self.user_provided ^ &self.skip_regenerate_different_from_user_provided
    }

    pub(crate) fn into_user_provided(self) -> RoaringBitmap {
        self.user_provided
    }
}

#[derive(Default)]
pub struct EmbeddingStatusDelta {
    del_status: EmbeddingStatus,
    add_status: EmbeddingStatus,
}

impl EmbeddingStatusDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn needs_change(
        old_is_user_provided: bool,
        old_must_regenerate: bool,
        new_is_user_provided: bool,
        new_must_regenerate: bool,
    ) -> bool {
        let old_skip_regenerate_different_user_provided =
            old_is_user_provided == old_must_regenerate;
        let new_skip_regenerate_different_user_provided =
            new_is_user_provided == new_must_regenerate;

        old_is_user_provided != new_is_user_provided
            || old_skip_regenerate_different_user_provided
                != new_skip_regenerate_different_user_provided
    }

    pub fn needs_clear(is_user_provided: bool, must_regenerate: bool) -> bool {
        Self::needs_change(is_user_provided, must_regenerate, false, true)
    }

    pub fn clear_docid(
        &mut self,
        docid: DocumentId,
        is_user_provided: bool,
        must_regenerate: bool,
    ) {
        self.push_delta(docid, is_user_provided, must_regenerate, false, true);
    }

    pub fn push_delta(
        &mut self,
        docid: DocumentId,
        old_is_user_provided: bool,
        old_must_regenerate: bool,
        new_is_user_provided: bool,
        new_must_regenerate: bool,
    ) {
        // must_regenerate == !skip_regenerate
        let old_skip_regenerate_different_user_provided =
            old_is_user_provided == old_must_regenerate;
        let new_skip_regenerate_different_user_provided =
            new_is_user_provided == new_must_regenerate;

        match (old_is_user_provided, new_is_user_provided) {
            (true, true) | (false, false) => { /* no change */ }
            (true, false) => {
                self.del_status.user_provided.insert(docid);
            }
            (false, true) => {
                self.add_status.user_provided.insert(docid);
            }
        }

        match (
            old_skip_regenerate_different_user_provided,
            new_skip_regenerate_different_user_provided,
        ) {
            (true, true) | (false, false) => { /* no change */ }
            (true, false) => {
                self.del_status.skip_regenerate_different_from_user_provided.insert(docid);
            }
            (false, true) => {
                self.add_status.skip_regenerate_different_from_user_provided.insert(docid);
            }
        }
    }

    pub fn push_new(&mut self, docid: DocumentId, is_user_provided: bool, must_regenerate: bool) {
        self.push_delta(
            docid,
            !is_user_provided,
            !must_regenerate,
            is_user_provided,
            must_regenerate,
        );
    }

    pub fn apply_to(&self, status: &mut EmbeddingStatus) {
        status.user_provided -= &self.del_status.user_provided;
        status.user_provided |= &self.add_status.user_provided;

        status.skip_regenerate_different_from_user_provided -=
            &self.del_status.skip_regenerate_different_from_user_provided;
        status.skip_regenerate_different_from_user_provided |=
            &self.add_status.skip_regenerate_different_from_user_provided;
    }
}

struct EmbedderInfoCodec;

impl<'a> heed::BytesDecode<'a> for EmbedderInfoCodec {
    type DItem = EmbedderInfo;

    fn bytes_decode(mut bytes: &'a [u8]) -> Result<Self::DItem, heed::BoxedError> {
        let embedder_id = bytes.read_u8()?;
        // Support all version that didn't store the embedding status
        if bytes.is_empty() {
            return Ok(EmbedderInfo { embedder_id, embedding_status: EmbeddingStatus::new() });
        }
        let first_bitmap_size = bytes.read_u32::<BigEndian>()?;
        let first_bitmap_bytes = &bytes[..first_bitmap_size as usize];
        let user_provided = CboRoaringBitmapCodec::bytes_decode(first_bitmap_bytes)?;
        let skip_regenerate_different_from_user_provided =
            CboRoaringBitmapCodec::bytes_decode(&bytes[first_bitmap_size as usize..])?;
        Ok(EmbedderInfo {
            embedder_id,
            embedding_status: EmbeddingStatus {
                user_provided,
                skip_regenerate_different_from_user_provided,
            },
        })
    }
}

impl<'a> heed::BytesEncode<'a> for EmbedderInfoCodec {
    type EItem = EmbedderInfo;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, heed::BoxedError> {
        let first_bitmap_size =
            CboRoaringBitmapCodec::serialized_size(&item.embedding_status.user_provided);
        let second_bitmap_size = CboRoaringBitmapCodec::serialized_size(
            &item.embedding_status.skip_regenerate_different_from_user_provided,
        );

        let mut bytes = Vec::with_capacity(1 + 4 + first_bitmap_size + second_bitmap_size);
        bytes.write_u8(item.embedder_id)?;
        bytes.write_u32::<BigEndian>(first_bitmap_size.try_into()?)?;
        CboRoaringBitmapCodec::serialize_into_writer(
            &item.embedding_status.user_provided,
            &mut bytes,
        )?;
        CboRoaringBitmapCodec::serialize_into_writer(
            &item.embedding_status.skip_regenerate_different_from_user_provided,
            &mut bytes,
        )?;
        Ok(bytes.into())
    }
}

impl IndexEmbeddingConfigs {
    pub(crate) fn new(
        main: Database<Unspecified, Unspecified>,
        embedder_info: Database<Unspecified, Unspecified>,
    ) -> Self {
        Self { main, embedder_info: embedder_info.remap_types() }
    }

    pub(crate) fn put_embedding_configs(
        &self,
        wtxn: &mut RwTxn<'_>,
        configs: Vec<IndexEmbeddingConfig>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<Vec<IndexEmbeddingConfig>>>().put(
            wtxn,
            crate::index::main_key::EMBEDDING_CONFIGS,
            &configs,
        )
    }

    pub(crate) fn delete_embedding_configs(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, crate::index::main_key::EMBEDDING_CONFIGS)
    }

    pub fn embedding_configs(&self, rtxn: &RoTxn<'_>) -> heed::Result<Vec<IndexEmbeddingConfig>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<Vec<IndexEmbeddingConfig>>>()
            .get(rtxn, crate::index::main_key::EMBEDDING_CONFIGS)?
            .unwrap_or_default())
    }

    pub fn embedder_id(&self, rtxn: &RoTxn<'_>, name: &str) -> heed::Result<Option<u8>> {
        self.embedder_info.remap_data_type::<U8>().get(rtxn, name)
    }

    pub fn put_fresh_embedder_id(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        embedder_id: u8,
    ) -> heed::Result<()> {
        let info = EmbedderInfo { embedder_id, embedding_status: EmbeddingStatus::new() };
        self.put_embedder_info(wtxn, name, &info)
    }

    /// Iterate through the passed list of embedder names, associating a fresh embedder id to any new names.
    ///
    /// Passing the name of a currently existing embedder is not an error, and will not modify its embedder id,
    /// so it is not necessary to differentiate between new and existing embedders before calling this function.
    pub fn add_new_embedders<'a>(
        &self,
        wtxn: &mut RwTxn<'_>,
        embedder_names: impl IntoIterator<Item = &'a str>,
        total_embedder_count: usize,
    ) -> crate::Result<()> {
        let mut free_indices: [bool; u8::MAX as usize] = [true; u8::MAX as usize];

        for res in self.embedder_info.iter(wtxn)? {
            let (_name, EmbedderInfo { embedder_id, embedding_status: _ }) = res?;
            free_indices[embedder_id as usize] = false;
        }

        let mut free_indices = free_indices.iter_mut().enumerate();
        let mut find_free_index =
            move || free_indices.find(|(_, free)| **free).map(|(index, _)| index as u8);

        for embedder_name in embedder_names {
            if self.embedder_id(wtxn, embedder_name)?.is_some() {
                continue;
            }
            let embedder_id = find_free_index()
                .ok_or(crate::UserError::TooManyEmbedders(total_embedder_count))?;
            tracing::debug!(
                embedder = embedder_name,
                embedder_id,
                "assigning free id to new embedder"
            );
            self.put_fresh_embedder_id(wtxn, embedder_name, embedder_id)?;
        }
        Ok(())
    }

    pub fn embedder_info(
        &self,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> heed::Result<Option<EmbedderInfo>> {
        self.embedder_info.get(rtxn, name)
    }

    /// Clear the list of docids that are `user_provided` or `must_regenerate` across all embedders.
    pub fn clear_embedder_info_docids(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<()> {
        let mut it = self.embedder_info.iter_mut(wtxn)?;
        while let Some(res) = it.next() {
            let (embedder_name, info) = res?;
            let embedder_name = embedder_name.to_owned();
            // SAFETY: we copied the `embedder_name` so are not using the reference while using put
            unsafe {
                it.put_current(
                    &embedder_name,
                    &EmbedderInfo {
                        embedder_id: info.embedder_id,
                        embedding_status: EmbeddingStatus::new(),
                    },
                )?;
            }
        }
        Ok(())
    }

    pub fn iter_embedder_info<'a>(
        &self,
        rtxn: &'a RoTxn<'_>,
    ) -> heed::Result<impl Iterator<Item = heed::Result<(&'a str, EmbedderInfo)>>> {
        self.embedder_info.iter(rtxn)
    }

    pub fn iter_embedder_id<'a>(
        &self,
        rtxn: &'a RoTxn<'_>,
    ) -> heed::Result<impl Iterator<Item = heed::Result<(&'a str, u8)>>> {
        self.embedder_info.remap_data_type::<U8>().iter(rtxn)
    }

    pub fn remove_embedder(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
    ) -> heed::Result<Option<EmbedderInfo>> {
        let info = self.embedder_info.get(wtxn, name)?;
        self.embedder_info.delete(wtxn, name)?;
        Ok(info)
    }

    pub fn put_embedder_info(
        &self,
        wtxn: &mut RwTxn<'_>,
        name: &str,
        info: &EmbedderInfo,
    ) -> heed::Result<()> {
        self.embedder_info.put(wtxn, name, info)
    }
}
