use heed::types::Bytes;
use heed::{Database, RoTxn};
use obkv::KvReaderU16;
use roaring::RoaringBitmap;

use crate::{all_obkv_to_json, DocumentId, FieldsIdsMap, Object, ObkvCodec, Result, BEU32};

pub struct ImmutableObkvs<'t> {
    ids: RoaringBitmap,
    fields_ids_map: FieldsIdsMap,
    slices: Vec<&'t [u8]>,
}

impl<'t> ImmutableObkvs<'t> {
    /// Creates the structure by fetching all the OBKVs
    /// and keeping the transaction making the pointers valid.
    pub fn new(
        rtxn: &'t RoTxn,
        documents_database: Database<BEU32, ObkvCodec>,
        fields_ids_map: FieldsIdsMap,
        subset: RoaringBitmap,
    ) -> heed::Result<Self> {
        let mut slices = Vec::new();
        let documents_database = documents_database.remap_data_type::<Bytes>();
        for docid in &subset {
            let slice = documents_database.get(rtxn, &docid)?.unwrap();
            slices.push(slice);
        }

        Ok(ImmutableObkvs { ids: subset, fields_ids_map, slices })
    }

    /// Returns the OBKVs identified by the given ID.
    pub fn obkv(&self, docid: DocumentId) -> heed::Result<Option<KvReaderU16<'t>>> {
        match self
            .ids
            .rank(docid)
            .checked_sub(1)
            .and_then(|offset| self.slices.get(offset as usize))
        {
            Some(bytes) => Ok(Some(KvReaderU16::new(bytes))),
            None => Ok(None),
        }
    }

    /// Returns the owned rhai::Map identified by the given ID.
    pub fn rhai_map(&self, docid: DocumentId) -> Result<Option<rhai::Map>> {
        let obkv = match self.obkv(docid) {
            Ok(Some(obkv)) => obkv,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let all_keys = obkv.iter().map(|(k, _v)| k).collect::<Vec<_>>();
        let map: Result<rhai::Map> = all_keys
            .iter()
            .copied()
            .flat_map(|id| obkv.get(id).map(|value| (id, value)))
            .map(|(id, value)| {
                let name = self.fields_ids_map.name(id).ok_or(
                    crate::error::FieldIdMapMissingEntry::FieldId {
                        field_id: id,
                        process: "allobkv_to_rhaimap",
                    },
                )?;
                let value = serde_json::from_slice(value)
                    .map_err(crate::error::InternalError::SerdeJson)?;
                Ok((name.into(), value))
            })
            .collect();

        map.map(Some)
    }

    pub fn json_map(&self, docid: DocumentId) -> Result<Option<Object>> {
        let obkv = match self.obkv(docid) {
            Ok(Some(obkv)) => obkv,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        all_obkv_to_json(obkv, &self.fields_ids_map).map(Some)
    }
}

unsafe impl Sync for ImmutableObkvs<'_> {}
