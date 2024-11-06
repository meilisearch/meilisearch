use std::collections::BTreeSet;

use heed::RoTxn;
use serde_json::value::RawValue;

use super::document_change::{Entry, Versions};
use super::{KvReaderFieldId, KvWriterFieldId};
use crate::documents::FieldIdMapper;
use crate::vector::parsed_vectors::RESERVED_VECTORS_FIELD_NAME;
use crate::{DocumentId, Index, InternalError, Result};

/// A view into a document that can represent either the current version from the DB,
/// the update data from payload or other means, or the merged updated version.
///
/// The 'doc lifetime is meant to live sufficiently for the document to be handled by the extractors.
pub trait Document<'doc> {
    /// Iterate over all **top-level** fields of the document, returning their name and raw JSON value.
    ///
    /// - The returned values *may* contain nested fields.
    /// - The `_vectors` field is **ignored** by this method, meaning it is **not returned** by this method.
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>>;
}

#[derive(Clone, Copy)]
pub struct DocumentFromDb<'t, Mapper: FieldIdMapper>
where
    Mapper: FieldIdMapper,
{
    fields_ids_map: &'t Mapper,
    content: &'t KvReaderFieldId,
}

impl<'t, Mapper: FieldIdMapper> Document<'t> for DocumentFromDb<'t, Mapper> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'t str, &'t RawValue)>> {
        let mut it = self.content.iter();

        std::iter::from_fn(move || {
            let (fid, value) = it.next()?;

            let res = (|| {
                let value =
                    serde_json::from_slice(value).map_err(crate::InternalError::SerdeJson)?;

                let name = self.fields_ids_map.name(fid).ok_or(
                    InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldId {
                        field_id: fid,
                        process: "getting current document",
                    }),
                )?;
                Ok((name, value))
            })();

            Some(res)
        })
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
}

#[derive(Clone, Copy)]
pub struct DocumentFromVersions<'doc> {
    versions: Versions<'doc>,
}

impl<'doc> DocumentFromVersions<'doc> {
    pub fn new(versions: Versions<'doc>) -> Self {
        Self { versions }
    }
}

impl<'doc> Document<'doc> for DocumentFromVersions<'doc> {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        match &self.versions {
            Versions::Single(version) => either::Either::Left(version.iter_top_level_fields()),
            Versions::Multiple(versions) => {
                let mut seen_fields = BTreeSet::new();
                let mut it = versions.iter().rev().flat_map(|version| version.iter()).copied();
                either::Either::Right(std::iter::from_fn(move || loop {
                    let (name, value) = it.next()?;

                    if seen_fields.contains(name) {
                        continue;
                    }
                    seen_fields.insert(name);
                    return Some(Ok((name, value)));
                }))
            }
        }
    }
}

// used in document from payload
impl<'doc> Document<'doc> for &'doc [Entry<'doc>] {
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<Entry<'doc>>> {
        self.iter().copied().map(|(k, v)| Ok((k, v)))
    }
}

pub struct MergedDocument<'doc, 't, Mapper: FieldIdMapper> {
    new_doc: DocumentFromVersions<'doc>,
    db: Option<DocumentFromDb<'t, Mapper>>,
}

impl<'doc, 't, Mapper: FieldIdMapper> MergedDocument<'doc, 't, Mapper> {
    pub fn new(
        new_doc: DocumentFromVersions<'doc>,
        db: Option<DocumentFromDb<'t, Mapper>>,
    ) -> Self {
        Self { new_doc, db }
    }

    pub fn with_db(
        docid: DocumentId,
        rtxn: &'t RoTxn,
        index: &'t Index,
        db_fields_ids_map: &'t Mapper,
        new_doc: DocumentFromVersions<'doc>,
    ) -> Result<Self> {
        let db = DocumentFromDb::new(docid, rtxn, index, db_fields_ids_map)?;
        Ok(Self { new_doc, db })
    }

    pub fn without_db(new_doc: DocumentFromVersions<'doc>) -> Self {
        Self { new_doc, db: None }
    }
}

impl<'d, 'doc: 'd, 't: 'd, Mapper: FieldIdMapper> Document<'d>
    for MergedDocument<'doc, 't, Mapper>
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
}

impl<'doc, D> Document<'doc> for &D
where
    D: Document<'doc>,
{
    fn iter_top_level_fields(&self) -> impl Iterator<Item = Result<(&'doc str, &'doc RawValue)>> {
        D::iter_top_level_fields(self)
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
pub fn write_to_obkv<'s, 'a, 'b>(
    document: &'s impl Document<'s>,
    vector_document: Option<()>,
    fields_ids_map: &'a impl FieldIdMapper,
    mut document_buffer: &'a mut Vec<u8>,
) -> Result<&'a KvReaderFieldId>
where
    's: 'a,
    's: 'b,
{
    // will be used in 'inject_vectors
    let vectors_value: Box<RawValue>;

    document_buffer.clear();
    let mut unordered_field_buffer = Vec::new();
    unordered_field_buffer.clear();

    let mut writer = KvWriterFieldId::new(&mut document_buffer);

    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;
        let field_id = fields_ids_map.id(field_name).unwrap();
        unordered_field_buffer.push((field_id, value));
    }

    'inject_vectors: {
        let Some(vector_document) = vector_document else { break 'inject_vectors };

        let Some(vectors_fid) = fields_ids_map.id(RESERVED_VECTORS_FIELD_NAME) else {
            break 'inject_vectors;
        };
        /*
        let mut vectors = BTreeMap::new();
        for (name, entry) in vector_document.iter_vectors() {
            if entry.has_configured_embedder {
                continue; // we don't write vectors with configured embedder in documents
            }
            vectors.insert(
                name,
                serde_json::json!({
                    "regenerate": entry.regenerate,
                    // TODO: consider optimizing the shape of embedders here to store an array of f32 rather than a JSON object
                    "embeddings": entry.embeddings,
                }),
            );
        }

        vectors_value = serde_json::value::to_raw_value(&vectors).unwrap();
        unordered_field_buffer.push((vectors_fid, &vectors_value));*/
    }

    unordered_field_buffer.sort_by_key(|(fid, _)| *fid);
    for (fid, value) in unordered_field_buffer.iter() {
        writer.insert(*fid, value.get().as_bytes()).unwrap();
    }

    writer.finish().unwrap();
    Ok(KvReaderFieldId::from_slice(document_buffer))
}
