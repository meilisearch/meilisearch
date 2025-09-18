use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::RwLock;

use bumpalo::Bump;
use bumparaw_collections::RawMap;
use heed::{RoTxn, WithoutTls};
use rustc_hash::FxBuildHasher;
use serde_json::value::RawValue;
use serde_json::{from_str, Value};

use super::vector_document::VectorDocument;
use super::{KvReaderFieldId, KvWriterFieldId};
use crate::constants::{RESERVED_GEO_FIELD_NAME, RESERVED_VECTORS_FIELD_NAME};
use crate::documents::FieldIdMapper;
use crate::update::del_add::KvReaderDelAdd;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::update::new::vector_document::VectorDocumentFromDb;
use crate::vector::settings::EmbedderAction;
use crate::{
    DocumentId, FieldIdMapWithMetadata, FieldsIdsMap, GlobalFieldsIdsMap, Index, InternalError,
    Result, UserError,
};

/// A view into a document that can represent either the current version from the DB,
/// the update data from payload or other means, or the merged updated version.
///
/// The 'doc lifetime is meant to live sufficiently for the document to be handled by the extractors.
pub trait Document<'doc> {
    /// Iterate over all **top-level** fields of the document, returning their name and raw JSON value.
    ///
    /// - The returned values *may* contain nested fields.
    /// - The `_vectors` and `_geo` fields are **ignored** by this method, meaning they are **not returned** by this method.
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>>;

    /// Iterate over all fields of the document, returning their `JSON path` and raw JSON value.
    ///
    /// - The returned values *will* contain nested fields.
    /// - The `_vectors` and `_geo` fields are **ignored** by this method, meaning they are **not returned** by this method.
    fn iter_all_fields(&self) -> impl Iterator<Item = Result<(String, Box<RawValue>)>>;

    /// Number of top level fields, **excluding** `_vectors` and `_geo`
    fn top_level_fields_count(&self) -> usize;

    /// Get the **top-level** with the specified name, if exists.
    ///
    /// - The `_vectors` and `_geo` fields are **ignored** by this method, meaning e.g. `top_level_field("_vectors")` will return `Ok(None)`
    fn top_level_field(&self, k: &str) -> Result<Option<&'doc RawValue>>;

    /// Returns the unparsed value of the `_vectors` field from the document data.
    ///
    /// This field alone is insufficient to retrieve vectors, as they may be stored in a dedicated location in the database.
    /// Use a [`super::vector_document::VectorDocument`] to access the vector.
    ///
    /// This method is meant as a convenience for implementors of [`super::vector_document::VectorDocument`].
    fn vectors_field(&self) -> Result<Option<&'doc RawValue>>;

    /// Returns the unparsed value of the `_geo` field from the document data.
    ///
    /// This field alone is insufficient to retrieve geo data, as they may be stored in a dedicated location in the database.
    /// Use a [`super::geo_document::GeoDocument`] to access the vector.
    ///
    /// This method is meant as a convenience for implementors of [`super::geo_document::GeoDocument`].
    fn geo_field(&self) -> Result<Option<&'doc RawValue>>;
}

#[derive(Debug)]
pub struct DocumentFromDb<'t, Mapper: FieldIdMapper>
where
    Mapper: FieldIdMapper,
{
    fields_ids_map: &'t Mapper,
    content: &'t KvReaderFieldId,
}

impl<Mapper: FieldIdMapper> Clone for DocumentFromDb<'_, Mapper> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<Mapper: FieldIdMapper> Copy for DocumentFromDb<'_, Mapper> {}

impl<'t, Mapper: FieldIdMapper> Document<'t> for DocumentFromDb<'t, Mapper> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'t str, &'t RawValue)>> {
        let mut it = self.content.iter();

        std::iter::from_fn(move || loop {
            let (fid, value) = it.next()?;
            let name = match self.fields_ids_map.name(fid).ok_or(
                InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                    field_id: fid,
                    process: "getting current document",
                }),
            ) {
                Ok(name) => name,
                Err(error) => return Some(Err(error.into())),
            };

            if name == RESERVED_VECTORS_FIELD_NAME || name == RESERVED_GEO_FIELD_NAME {
                continue;
            }

            let res = (|| {
                let value =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                Ok((name, value))
            })();

            return Some(res);
        })
    }

    fn iter_all_fields(&self) -> impl Iterator<Item = Result<(String, Box<RawValue>)>> {
        let mut it = self.content.iter();
        let mut stack = VecDeque::new();

        std::iter::from_fn(move || loop {
            if let Some(next) = it.next() {
                let (fid, value) = next;
                let name = match self.fields_ids_map.name(fid).ok_or(
                    InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                        field_id: fid,
                        process: "getting current document",
                    }),
                ) {
                    Ok(name) => name,
                    Err(error) => return Some(Err(error.into())),
                };

                if name == RESERVED_VECTORS_FIELD_NAME || name == RESERVED_GEO_FIELD_NAME {
                    continue;
                }

                let res: Result<(String, Box<RawValue>)> = (|| {
                    let value =
                        serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                    Ok((name.to_string(), value))
                })();

                if let Ok((name, value)) = &res {
                    stack.push_back((value.clone(), name.clone(), true));
                } else {
                    return Some(Err(res.err().unwrap()));
                }

                return Some(res);
            } else {
                while let Some((raw, path, is_first_level)) = stack.pop_back() {
                    if let Ok(val) = from_str::<Value>(raw.get()) {
                        match val {
                            Value::Object(map) => {
                                for (child_key, child_val) in map.iter() {
                                    let child_path = format!("{}.{}", path, child_key);
                                    if let Ok(child_raw) =
                                        RawValue::from_string(child_val.to_string())
                                    {
                                        stack.push_back((child_raw, child_path, false));
                                    }
                                }
                                continue;
                            }
                            Value::Array(array) => {
                                let mut is_object = false;
                                for ele in array {
                                    if ele.is_object() {
                                        is_object = true;
                                        // We only have to traverse deeper if the array element is of object typeh
                                        if let Ok(child_raw) =
                                            RawValue::from_string(ele.to_string())
                                        {
                                            stack.push_back((child_raw, path.clone(), false));
                                        }
                                    }
                                }
                                if !is_object && !is_first_level {
                                    return Some(Ok((path, raw)));
                                }
                                continue;
                            }
                            _ => {
                                if !is_first_level {
                                    return Some(Ok((path, raw)));
                                }
                                continue;
                            }
                        }
                    } else {
                        return Some(Ok((path, raw)));
                    }
                }
                return None;
            }
        })
    }

    fn vectors_field(&self) -> Result<Option<&'t RawValue>> {
        self.field(RESERVED_VECTORS_FIELD_NAME)
    }

    fn geo_field(&self) -> Result<Option<&'t RawValue>> {
        self.field(RESERVED_GEO_FIELD_NAME)
    }

    fn top_level_fields_count(&self) -> usize {
        let has_vectors_field = self.vectors_field().unwrap_or(None).is_some();
        let has_geo_field = self.geo_field().unwrap_or(None).is_some();
        let count = self.content.iter().count();
        match (has_vectors_field, has_geo_field) {
            (true, true) => count - 2,
            (true, false) | (false, true) => count - 1,
            (false, false) => count,
        }
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'t RawValue>> {
        if k == RESERVED_VECTORS_FIELD_NAME || k == RESERVED_GEO_FIELD_NAME {
            return Ok(None);
        }
        self.field(k)
    }
}

impl<'t, Mapper: FieldIdMapper> DocumentFromDb<'t, Mapper> {
    pub fn new(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
    ) -> Result<Option<Self>> {
        index.documents.get(rtxn, &docid).map_err(crate::Error::from).map(|reader| {
            reader.map(|reader| Self { fields_ids_map: db_fields_ids_map, content: reader })
        })
    }

    pub fn field(&self, name: &str) -> Result<Option<&'t RawValue>> {
        let Some(fid) = self.fields_ids_map.id(name) else {
            return Ok(None);
        };
        let Some(value) = self.content.get(fid) else { return Ok(None) };
        Ok(Some(serde_json::from_slice(value).map_err(InternalError::SerdeJson)?))
    }
}

#[derive(Debug)]
pub struct DocumentFromVersions<'a, 'doc> {
    versions: &'a Versions<'doc>,
}

impl<'a, 'doc> DocumentFromVersions<'a, 'doc> {
    pub fn new(versions: &'a Versions<'doc>) -> Self {
        Self { versions }
    }
}

impl<'doc> Document<'doc> for DocumentFromVersions<'_, 'doc> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        self.versions.iter_top_level_fields().map(Ok)
    }

    fn iter_all_fields(&self) -> impl Iterator<Item = Result<(String, Box<RawValue>)>> {
        self.versions.iter_all_fields()
    }

    fn vectors_field(&self) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.vectors_field())
    }

    fn geo_field(&self) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.geo_field())
    }

    fn top_level_fields_count(&self) -> usize {
        let has_vectors_field = self.vectors_field().unwrap_or(None).is_some();
        let has_geo_field = self.geo_field().unwrap_or(None).is_some();
        let count = self.versions.len();
        match (has_vectors_field, has_geo_field) {
            (true, true) => count - 2,
            (true, false) | (false, true) => count - 1,
            (false, false) => count,
        }
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'doc RawValue>> {
        Ok(self.versions.top_level_field(k))
    }
}

#[derive(Debug)]
pub struct MergedDocument<'a, 'doc, 't, Mapper: FieldIdMapper> {
    new_doc: DocumentFromVersions<'a, 'doc>,
    db: Option<DocumentFromDb<'t, Mapper>>,
}

impl<'a, 'doc, 't, Mapper: FieldIdMapper> MergedDocument<'a, 'doc, 't, Mapper> {
    pub fn with_db(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
        new_doc: DocumentFromVersions<'a, 'doc>,
    ) -> Result<Self> {
        let db = DocumentFromDb::new(docid, rtxn, index, db_fields_ids_map)?;
        Ok(Self { new_doc, db })
    }

    pub fn without_db(new_doc: DocumentFromVersions<'a, 'doc>) -> Self {
        Self { new_doc, db: None }
    }
}

impl<'d, 'doc: 'd, 't: 'd, Mapper: FieldIdMapper> Document<'d>
    for MergedDocument<'d, 'doc, 't, Mapper>
{
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'d str, &'d RawValue)>> {
        let mut new_doc_it = self.new_doc.iter_top_level_fields();
        let mut db_it = self.db.iter().flat_map(|db| db.iter_top_level_fields());
        let mut seen_fields = BTreeSet::new();

        std::iter::from_fn(move || {
            if let Some(next) = new_doc_it.next() {
                if let Ok((name, _)) = next {
                    seen_fields.insert(name);
                }
                return Some(next);
            }
            loop {
                match db_it.next()? {
                    Ok((name, value)) => {
                        if seen_fields.contains(name) {
                            continue;
                        }
                        return Some(Ok((name, value)));
                    }
                    Err(err) => return Some(Err(err)),
                }
            }
        })
    }

    fn iter_all_fields(&self) -> impl Iterator<Item = Result<(String, Box<RawValue>)>> {
        let mut seen_fields = BTreeSet::new();
        let mut new_doc_it = self.new_doc.iter_all_fields();
        let mut db_it = self.db.iter().flat_map(|db| db.iter_all_fields());

        std::iter::from_fn(move || {
            if let Some(next) = new_doc_it.next() {
                if let Ok((name, _)) = &next {
                    seen_fields.insert(name.clone());
                }
                return Some(next);
            }
            loop {
                match db_it.next()? {
                    Ok((name, value)) => {
                        if seen_fields.contains(&name) {
                            continue;
                        }
                        return Some(Ok((name, value)));
                    }
                    Err(err) => return Some(Err(err)),
                }
            }
        })
    }

    fn vectors_field(&self) -> Result<Option<&'d RawValue>> {
        if let Some(vectors) = self.new_doc.vectors_field()? {
            return Ok(Some(vectors));
        }

        let Some(db) = self.db else { return Ok(None) };

        db.vectors_field()
    }

    fn geo_field(&self) -> Result<Option<&'d RawValue>> {
        if let Some(geo) = self.new_doc.geo_field()? {
            return Ok(Some(geo));
        }

        let Some(db) = self.db else { return Ok(None) };

        db.geo_field()
    }

    fn top_level_fields_count(&self) -> usize {
        self.iter_top_level_fields().count()
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'d RawValue>> {
        if let Some(f) = self.new_doc.top_level_field(k)? {
            return Ok(Some(f));
        }
        if let Some(db) = self.db {
            return db.field(k);
        }
        Ok(None)
    }
}

impl<'doc, D> Document<'doc> for &D
where
    D: Document<'doc>,
{
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        D::iter_top_level_fields(self)
    }

    fn iter_all_fields(&self) -> impl Iterator<Item = Result<(String, Box<RawValue>)>> {
        D::iter_all_fields(self)
    }

    fn vectors_field(&self) -> Result<Option<&'doc RawValue>> {
        D::vectors_field(self)
    }

    fn geo_field(&self) -> Result<Option<&'doc RawValue>> {
        D::geo_field(self)
    }

    fn top_level_fields_count(&self) -> usize {
        D::top_level_fields_count(self)
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'doc RawValue>> {
        D::top_level_field(self, k)
    }
}

/// Turn this document into an obkv, whose fields are indexed by the provided `FieldIdMapper`.
///
/// The produced obkv is suitable for storing into the documents DB, meaning:
///
/// - It contains the contains of `_vectors` that are not configured as an embedder
/// - It contains all the top-level fields of the document, with their raw JSON value as value.
///
/// # Panics
///
/// - If the document contains a top-level field that is not present in `fields_ids_map`.
///
pub fn write_to_obkv<'s, 'a, 'map, 'buffer>(
    document: &'s impl Document<'s>,
    vector_document: Option<&'s impl VectorDocument<'s>>,
    embedder_actions: &'a BTreeMap<String, EmbedderAction>,
    fields_ids_map: &'a mut GlobalFieldsIdsMap<'map>,
    mut document_buffer: &'a mut bumpalo::collections::Vec<'buffer, u8>,
) -> Result<&'a KvReaderFieldId>
where
    's: 'a,
{
    // will be used in 'inject_vectors
    let vectors_value: Box<RawValue>;

    document_buffer.clear();
    let mut unordered_field_buffer = Vec::new();
    unordered_field_buffer.clear();

    let mut writer = KvWriterFieldId::new(&mut document_buffer);

    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;
        let field_id =
            fields_ids_map.id_or_insert(field_name).ok_or(UserError::AttributeLimitReached)?;
        unordered_field_buffer.push((field_id, value));
    }

    'inject_vectors: {
        let Some(vector_document) = vector_document else { break 'inject_vectors };

        let mut vectors = BTreeMap::new();
        for res in vector_document.iter_vectors() {
            let (name, entry) = res?;
            if entry.has_configured_embedder {
                if let Some(action) = embedder_actions.get(name) {
                    if action.write_back().is_some() && !entry.regenerate {
                        vectors.insert(
                            name,
                            serde_json::json!({
                                "regenerate": entry.regenerate,
                                // TODO: consider optimizing the shape of embedders here to store an array of f32 rather than a JSON object
                                "embeddings": entry.embeddings,
                            }),
                        );
                    }
                }
            } else {
                match embedder_actions.get(name) {
                    Some(action) if action.write_back().is_none() => {
                        continue;
                    }
                    _ => {
                        vectors.insert(
                            name,
                            if entry.implicit {
                                serde_json::json!(entry.embeddings)
                            } else {
                                serde_json::json!({
                                    "regenerate": entry.regenerate,
                                    // TODO: consider optimizing the shape of embedders here to store an array of f32 rather than a JSON object
                                    "embeddings": entry.embeddings,
                                })
                            },
                        );
                    }
                }
            }
        }

        if vectors.is_empty() {
            break 'inject_vectors;
        }

        let vectors_fid = fields_ids_map
            .id_or_insert(RESERVED_VECTORS_FIELD_NAME)
            .ok_or(UserError::AttributeLimitReached)?;

        vectors_value = serde_json::value::to_raw_value(&vectors).unwrap();
        unordered_field_buffer.push((vectors_fid, &vectors_value));
    }

    if let Some(geo_value) = document.geo_field()? {
        let fid = fields_ids_map
            .id_or_insert(RESERVED_GEO_FIELD_NAME)
            .ok_or(UserError::AttributeLimitReached)?;
        fields_ids_map.id_or_insert("_geo.lat").ok_or(UserError::AttributeLimitReached)?;
        fields_ids_map.id_or_insert("_geo.lng").ok_or(UserError::AttributeLimitReached)?;
        unordered_field_buffer.push((fid, geo_value));
    }

    unordered_field_buffer.sort_by_key(|(fid, _)| *fid);
    for (fid, value) in unordered_field_buffer.iter() {
        writer.insert(*fid, value.get().as_bytes()).unwrap();
    }

    writer.finish().unwrap();
    Ok(KvReaderFieldId::from_slice(document_buffer))
}

pub type Entry<'doc> = (&'doc str, &'doc RawValue);

#[derive(Debug)]
pub struct Versions<'doc> {
    data: RawMap<'doc, FxBuildHasher>,
}

impl<'doc> Versions<'doc> {
    pub fn multiple(
        mut versions: impl Iterator<Item = Result<RawMap<'doc, FxBuildHasher>>>,
    ) -> Result<Option<Self>> {
        let Some(data) = versions.next() else { return Ok(None) };
        let mut data = data?;
        for future_version in versions {
            let future_version = future_version?;
            for (field, value) in future_version {
                data.insert(field, value);
            }
        }
        Ok(Some(Self::single(data)))
    }

    pub fn single(version: RawMap<'doc, FxBuildHasher>) -> Self {
        Self { data: version }
    }

    pub fn iter_top_level_fields(&self) -> impl Iterator<Item = (&'doc str, &'doc RawValue)> + '_ {
        self.data
            .iter()
            .filter(|(k, _)| *k != RESERVED_VECTORS_FIELD_NAME && *k != RESERVED_GEO_FIELD_NAME)
    }

    pub fn iter_all_fields(
        &'doc self,
    ) -> impl Iterator<Item = Result<(String, Box<RawValue>)>> + 'doc {
        let mut it = self.data.iter();
        let mut stack = VecDeque::new();

        std::iter::from_fn(move || loop {
            if let Some((k, v)) = it.next() {
                if k != RESERVED_VECTORS_FIELD_NAME && k != RESERVED_GEO_FIELD_NAME {
                    let res: Result<(String, Box<RawValue>)> = (|| {
                        let value = serde_json::from_slice(v.get().as_bytes())
                            .map_err(crate::InternalError::SerdeJson)?;

                        Ok((k.to_string(), value))
                    })();
                    if let Ok((name, value)) = &res {
                        stack.push_back((value.clone(), name.clone(), true));
                    } else {
                        return Some(Err(res.err().unwrap()));
                    }

                    return Some(res);
                }
            } else if let Some((raw, path, is_first_level)) = stack.pop_back() {
                if let Ok(val) = from_str::<Value>(raw.get()) {
                    match val {
                        Value::Object(map) => {
                            for (child_key, child_val) in map.iter() {
                                let child_path = format!("{}.{}", path, child_key);
                                if let Ok(child_raw) = RawValue::from_string(child_val.to_string())
                                {
                                    stack.push_back((child_raw, child_path, false));
                                }
                            }
                            continue;
                        }
                        Value::Array(array) => {
                            let mut is_object = false;
                            for ele in array {
                                if ele.is_object() {
                                    is_object = true;
                                    if let Ok(child_raw) = RawValue::from_string(ele.to_string()) {
                                        stack.push_back((child_raw, path.clone(), false));
                                    }
                                }
                            }
                            if !is_object && !is_first_level {
                                return Some(Ok((path, raw)));
                            }
                            continue;
                        }
                        _ => {
                            if !is_first_level {
                                return Some(Ok((path, raw)));
                            }
                        }
                    }
                } else {
                    return Some(Ok((path, raw)));
                }
            } else {
                return None;
            }
        })
    }

    pub fn vectors_field(&self) -> Option<&'doc RawValue> {
        self.data.get(RESERVED_VECTORS_FIELD_NAME)
    }

    pub fn geo_field(&self) -> Option<&'doc RawValue> {
        self.data.get(RESERVED_GEO_FIELD_NAME)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn top_level_field(&self, k: &str) -> Option<&'doc RawValue> {
        if k == RESERVED_VECTORS_FIELD_NAME || k == RESERVED_GEO_FIELD_NAME {
            return None;
        }
        self.data.get(k)
    }
}

#[derive(Debug)]
pub struct KvDelAddDocument<'a, Mapper: FieldIdMapper> {
    document: &'a obkv::KvReaderU16,
    side: crate::update::del_add::DelAdd,
    fields_ids_map: &'a Mapper,
}

impl<'a, Mapper: FieldIdMapper> KvDelAddDocument<'a, Mapper> {
    pub fn new(
        document: &'a obkv::KvReaderU16,
        side: crate::update::del_add::DelAdd,
        fields_ids_map: &'a Mapper,
    ) -> Self {
        Self { document, side, fields_ids_map }
    }

    fn get(&self, k: &str) -> Result<Option<&'a RawValue>> {
        let Some(id) = self.fields_ids_map.id(k) else { return Ok(None) };
        let Some(value) = self.document.get(id) else { return Ok(None) };
        let Some(value) = KvReaderDelAdd::from_slice(value).get(self.side) else { return Ok(None) };

        let value = serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

        Ok(Some(value))
    }
}

impl<'a, Mapper: FieldIdMapper> Document<'a> for KvDelAddDocument<'a, Mapper> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'a str, &'a RawValue)>> {
        let mut it = self.document.iter();

        std::iter::from_fn(move || loop {
            let (fid, value) = it.next()?;
            let Some(value) = KvReaderDelAdd::from_slice(value).get(self.side) else {
                continue;
            };
            let name = match self.fields_ids_map.name(fid).ok_or(
                InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                    field_id: fid,
                    process: "getting current document",
                }),
            ) {
                Ok(name) => name,
                Err(error) => return Some(Err(error.into())),
            };

            if name == RESERVED_VECTORS_FIELD_NAME || name == RESERVED_GEO_FIELD_NAME {
                continue;
            }

            let res = (|| {
                let value =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                Ok((name, value))
            })();

            return Some(res);
        })
    }

    fn iter_all_fields(&self) -> impl Iterator<Item = Result<(String, Box<RawValue>)>> {
        let mut it = self.document.iter();
        let mut stack = VecDeque::new();

        std::iter::from_fn(move || loop {
            if let Some(next) = it.next() {
                let (fid, value) = next;
                let Some(value) = KvReaderDelAdd::from_slice(value).get(self.side) else {
                    continue;
                };
                let name = match self.fields_ids_map.name(fid).ok_or(
                    InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                        field_id: fid,
                        process: "getting current document",
                    }),
                ) {
                    Ok(name) => name,
                    Err(error) => return Some(Err(error.into())),
                };

                if name == RESERVED_VECTORS_FIELD_NAME || name == RESERVED_GEO_FIELD_NAME {
                    continue;
                }

                let res: Result<(String, Box<RawValue>)> = (|| {
                    let value =
                        serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                    Ok((name.to_string(), value))
                })();

                if let Ok((name, value)) = &res {
                    stack.push_back((value.clone(), name.clone(), true));
                }

                return Some(res);
            } else if let Some((raw, path, is_first_level)) = stack.pop_back() {
                if let Ok(val) = from_str::<Value>(raw.get()) {
                    match val {
                        Value::Object(map) => {
                            for (child_key, child_val) in map.iter() {
                                let child_path = format!("{}.{}", path, child_key);
                                if let Ok(child_raw) = RawValue::from_string(child_val.to_string())
                                {
                                    stack.push_back((child_raw, child_path, false));
                                }
                            }
                            continue;
                        }
                        Value::Array(array) => {
                            let mut is_object = false;
                            for ele in array {
                                if ele.is_object() {
                                    is_object = true;
                                    if let Ok(child_raw) = RawValue::from_string(ele.to_string()) {
                                        stack.push_back((child_raw, path.clone(), false));
                                    }
                                }
                            }
                            if !is_object && !is_first_level {
                                return Some(Ok((path, raw)));
                            }
                            continue;
                        }
                        _ => {
                            if !is_first_level {
                                return Some(Ok((path, raw)));
                            }
                        }
                    }
                } else {
                    return Some(Ok((path, raw)));
                }
            } else {
                return None;
            }
        })
    }

    fn top_level_fields_count(&self) -> usize {
        let mut it = self.document.iter();

        std::iter::from_fn(move || loop {
            let (fid, value) = it.next()?;
            let Some(_) = KvReaderDelAdd::from_slice(value).get(self.side) else {
                continue;
            };
            let name = match self.fields_ids_map.name(fid).ok_or(
                InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                    field_id: fid,
                    process: "getting current document",
                }),
            ) {
                Ok(name) => name,
                Err(_) => return Some(()),
            };

            if name == RESERVED_VECTORS_FIELD_NAME || name == RESERVED_GEO_FIELD_NAME {
                continue;
            }

            return Some(());
        })
        .count()
    }

    fn top_level_field(&self, k: &str) -> Result<Option<&'a RawValue>> {
        if k == RESERVED_VECTORS_FIELD_NAME || k == RESERVED_GEO_FIELD_NAME {
            return Ok(None);
        }
        self.get(k)
    }

    fn vectors_field(&self) -> Result<Option<&'a RawValue>> {
        self.get(RESERVED_VECTORS_FIELD_NAME)
    }

    fn geo_field(&self) -> Result<Option<&'a RawValue>> {
        self.get(RESERVED_GEO_FIELD_NAME)
    }
}

pub struct DocumentIdentifiers<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
}

impl<'doc> DocumentIdentifiers<'doc> {
    pub fn create(docid: DocumentId, external_document_id: &'doc str) -> Self {
        Self { docid, external_document_id }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }

    pub fn current<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        mapper: &'a Mapper,
    ) -> Result<DocumentFromDb<'a, Mapper>> {
        Ok(DocumentFromDb::new(self.docid, rtxn, index, mapper)?.ok_or(
            crate::error::UserError::UnknownInternalDocumentId { document_id: self.docid },
        )?)
    }

    pub fn current_vectors<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        mapper: &'a Mapper,
        doc_alloc: &'a Bump,
    ) -> Result<VectorDocumentFromDb<'a>> {
        Ok(VectorDocumentFromDb::new(self.docid, index, rtxn, mapper, doc_alloc)?.ok_or(
            crate::error::UserError::UnknownInternalDocumentId { document_id: self.docid },
        )?)
    }
}

pub struct DocumentContext<
    'doc,             // covariant lifetime of a single `process` call
    'extractor: 'doc, // invariant lifetime of the extractor_allocs
    'fid: 'doc,       // invariant lifetime of the new_fields_ids_map
    'indexer: 'doc,   // covariant lifetime of objects that outlive a single `process` call
    T: MostlySend,
> {
    /// The index we're indexing in
    pub index: &'indexer Index,
    /// The fields ids map as it was at the start of this indexing process. Contains at least all top-level fields from documents
    /// inside of the DB.
    pub db_fields_ids_map: &'indexer FieldsIdsMap,
    /// A transaction providing data from the DB before all indexing operations
    pub rtxn: RoTxn<'indexer, WithoutTls>,

    /// Global field id map that is up to date with the current state of the indexing process.
    ///
    /// - Inserting a field will take a lock
    /// - Retrieving a field may take a lock as well
    pub new_fields_ids_map: &'doc std::cell::RefCell<GlobalFieldsIdsMap<'fid>>,

    /// Data allocated in this allocator is cleared between each call to `process`.
    pub doc_alloc: Bump,

    /// Data allocated in this allocator is not cleared between each call to `process`, unless the data spills.
    pub extractor_alloc: &'extractor Bump,

    /// Pool of doc allocators, used to retrieve the doc allocator we provided for the documents
    pub doc_allocs: &'doc ThreadLocal<FullySend<Cell<Bump>>>,

    /// Extractor-specific data
    pub data: &'doc T,
}

impl<
        'doc,             // covariant lifetime of a single `process` call
        'data: 'doc,      // invariant on T lifetime of the datastore
        'extractor: 'doc, // invariant lifetime of extractor_allocs
        'fid: 'doc,       // invariant lifetime of fields ids map
        'indexer: 'doc,   // covariant lifetime of objects that survive a `process` call
        T: MostlySend,
    > DocumentContext<'doc, 'extractor, 'fid, 'indexer, T>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new<F>(
        index: &'indexer Index,
        db_fields_ids_map: &'indexer FieldsIdsMap,
        new_fields_ids_map: &'fid RwLock<FieldIdMapWithMetadata>,
        extractor_allocs: &'extractor ThreadLocal<FullySend<Bump>>,
        doc_allocs: &'doc ThreadLocal<FullySend<Cell<Bump>>>,
        datastore: &'data ThreadLocal<T>,
        fields_ids_map_store: &'doc ThreadLocal<FullySend<RefCell<GlobalFieldsIdsMap<'fid>>>>,
        init_data: F,
    ) -> Result<Self>
    where
        F: FnOnce(&'extractor Bump) -> Result<T>,
    {
        let doc_alloc =
            doc_allocs.get_or(|| FullySend(Cell::new(Bump::with_capacity(1024 * 1024))));
        let doc_alloc = doc_alloc.0.take();
        let fields_ids_map = fields_ids_map_store
            .get_or(|| RefCell::new(GlobalFieldsIdsMap::new(new_fields_ids_map)).into());

        let fields_ids_map = &fields_ids_map.0;
        let extractor_alloc = extractor_allocs.get_or_default();

        let data = datastore.get_or_try(move || init_data(&extractor_alloc.0))?;

        let txn = index.read_txn()?;
        Ok(DocumentContext {
            index,
            rtxn: txn,
            db_fields_ids_map,
            new_fields_ids_map: fields_ids_map,
            doc_alloc,
            extractor_alloc: &extractor_alloc.0,
            data,
            doc_allocs,
        })
    }
}
