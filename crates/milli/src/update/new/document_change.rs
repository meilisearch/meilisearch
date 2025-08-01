use bumpalo::Bump;
use heed::RoTxn;
use serde_json::Value;

use super::document::{
    Document as _, DocumentFromDb, DocumentFromVersions, MergedDocument, Versions,
};
use super::vector_document::{
    MergedVectorDocument, VectorDocumentFromDb, VectorDocumentFromVersions,
};
use crate::attribute_patterns::PatternMatch;
use crate::documents::FieldIdMapper;
use crate::update::new::document::DocumentIdentifiers;
use crate::vector::RuntimeEmbedders;
use crate::{DocumentId, Index, InternalError, Result};

pub enum DocumentChange<'doc> {
    Deletion(DocumentIdentifiers<'doc>),
    Update(Update<'doc>),
    Insertion(Insertion<'doc>),
}

pub struct Update<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
    new: Versions<'doc>,
    from_scratch: bool,
}

pub struct Insertion<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
    new: Versions<'doc>,
}

impl<'doc> DocumentChange<'doc> {
    pub fn docid(&self) -> DocumentId {
        match &self {
            Self::Deletion(inner) => inner.docid(),
            Self::Update(inner) => inner.docid(),
            Self::Insertion(inner) => inner.docid(),
        }
    }

    pub fn external_docid(&self) -> &'doc str {
        match self {
            DocumentChange::Deletion(deletion) => deletion.external_document_id(),
            DocumentChange::Update(update) => update.external_document_id(),
            DocumentChange::Insertion(insertion) => insertion.external_document_id(),
        }
    }
}

impl<'doc> Insertion<'doc> {
    pub fn create(docid: DocumentId, external_document_id: &'doc str, new: Versions<'doc>) -> Self {
        Insertion { docid, external_document_id, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }
    pub fn inserted(&self) -> DocumentFromVersions<'_, 'doc> {
        DocumentFromVersions::new(&self.new)
    }

    pub fn inserted_vectors(
        &self,
        doc_alloc: &'doc Bump,
        embedders: &'doc RuntimeEmbedders,
    ) -> Result<Option<VectorDocumentFromVersions<'doc>>> {
        VectorDocumentFromVersions::new(self.external_document_id, &self.new, doc_alloc, embedders)
    }
}

impl<'doc> Update<'doc> {
    pub fn create(
        docid: DocumentId,
        external_document_id: &'doc str,
        new: Versions<'doc>,
        from_scratch: bool,
    ) -> Self {
        Update { docid, new, external_document_id, from_scratch }
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

    pub fn only_changed_fields(&self) -> DocumentFromVersions<'_, 'doc> {
        DocumentFromVersions::new(&self.new)
    }

    pub fn merged<'t, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'t RoTxn,
        index: &'t Index,
        mapper: &'t Mapper,
    ) -> Result<MergedDocument<'_, 'doc, 't, Mapper>> {
        if self.from_scratch {
            Ok(MergedDocument::without_db(DocumentFromVersions::new(&self.new)))
        } else {
            MergedDocument::with_db(
                self.docid,
                rtxn,
                index,
                mapper,
                DocumentFromVersions::new(&self.new),
            )
        }
    }

    /// Returns whether the updated version of the document is different from the current version for the subset of fields selected by `selector`.
    ///
    /// `true` if at least one top-level-field that is exactly a selected field or a parent of a selected field changed.
    /// Otherwise `false`.
    ///
    /// - Note: `_geo` and `_vectors` are not taken into account by this function.
    pub fn has_changed_for_fields<'t, Mapper: FieldIdMapper>(
        &self,
        selector: &mut impl FnMut(&str) -> PatternMatch,
        rtxn: &'t RoTxn,
        index: &'t Index,
        mapper: &'t Mapper,
    ) -> Result<bool> {
        let mut changed = false;
        let mut cached_current = None;
        let mut updated_selected_field_count = 0;

        for entry in self.only_changed_fields().iter_top_level_fields() {
            let (key, updated_value) = entry?;

            if selector(key) == PatternMatch::NoMatch {
                continue;
            }

            updated_selected_field_count += 1;
            let current = match cached_current {
                Some(current) => current,
                None => self.current(rtxn, index, mapper)?,
            };
            let current_value = current.top_level_field(key)?;
            let Some(current_value) = current_value else {
                changed = true;
                break;
            };

            if current_value.get() != updated_value.get() {
                changed = true;
                break;
            }
            cached_current = Some(current);
        }

        if !self.from_scratch {
            // no field deletion or update, so fields that don't appear in `updated` cannot have changed
            return Ok(changed);
        }

        if changed {
            return Ok(true);
        }

        // we saw all updated fields, and set `changed` if any field wasn't in `current`.
        // so if there are as many fields in `current` as in `updated`, then nothing changed.
        // If there is any more fields in `current`, then they are missing in `updated`.
        let has_deleted_fields = {
            let current = match cached_current {
                Some(current) => current,
                None => self.current(rtxn, index, mapper)?,
            };

            let mut current_selected_field_count = 0;
            for entry in current.iter_top_level_fields() {
                let (key, _) = entry?;

                if selector(key) == PatternMatch::NoMatch {
                    continue;
                }
                current_selected_field_count += 1;
            }

            current_selected_field_count != updated_selected_field_count
        };

        Ok(has_deleted_fields)
    }

    /// Returns `true` if the geo fields have changed.
    pub fn has_changed_for_geo_fields<'t, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'t RoTxn,
        index: &'t Index,
        mapper: &'t Mapper,
    ) -> Result<bool> {
        let current = self.current(rtxn, index, mapper)?;
        let current_geo = current.geo_field()?;
        let updated_geo = self.only_changed_fields().geo_field()?;
        match (current_geo, updated_geo) {
            (Some(current_geo), Some(updated_geo)) => {
                let current: Value =
                    serde_json::from_str(current_geo.get()).map_err(InternalError::SerdeJson)?;
                let updated: Value =
                    serde_json::from_str(updated_geo.get()).map_err(InternalError::SerdeJson)?;
                Ok(current != updated)
            }
            (None, None) => Ok(false),
            _ => Ok(true),
        }
    }

    pub fn only_changed_vectors(
        &self,
        doc_alloc: &'doc Bump,
        embedders: &'doc RuntimeEmbedders,
    ) -> Result<Option<VectorDocumentFromVersions<'doc>>> {
        VectorDocumentFromVersions::new(self.external_document_id, &self.new, doc_alloc, embedders)
    }

    pub fn merged_vectors<Mapper: FieldIdMapper>(
        &self,
        rtxn: &'doc RoTxn,
        index: &'doc Index,
        mapper: &'doc Mapper,
        doc_alloc: &'doc Bump,
        embedders: &'doc RuntimeEmbedders,
    ) -> Result<Option<MergedVectorDocument<'doc>>> {
        if self.from_scratch {
            MergedVectorDocument::without_db(
                self.external_document_id,
                &self.new,
                doc_alloc,
                embedders,
            )
        } else {
            MergedVectorDocument::with_db(
                self.docid,
                self.external_document_id,
                index,
                rtxn,
                mapper,
                &self.new,
                doc_alloc,
                embedders,
            )
        }
    }
}
