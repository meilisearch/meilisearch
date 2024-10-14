use heed::RoTxn;
use serde_json::value::RawValue;

use super::document::{DocumentFromDb, DocumentFromVersions, MergedDocument};
use crate::documents::FieldIdMapper;
use crate::{DocumentId, Index, Result};

pub enum DocumentChange<'doc> {
    Deletion(Deletion<'doc>),
    Update(Update<'doc>),
    Insertion(Insertion<'doc>),
}

pub struct Deletion<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
}

pub struct Update<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
    new: DocumentFromVersions<'doc>,
    has_deletion: bool,
}

pub struct Insertion<'doc> {
    docid: DocumentId,
    external_document_id: &'doc str,
    new: DocumentFromVersions<'doc>,
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

impl<'doc> Deletion<'doc> {
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
}

impl<'doc> Insertion<'doc> {
    pub fn create(
        docid: DocumentId,
        external_document_id: &'doc str,
        new: DocumentFromVersions<'doc>,
    ) -> Self {
        Insertion { docid, external_document_id, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }
    pub fn new(&self) -> DocumentFromVersions<'doc> {
        self.new
    }
}

impl<'doc> Update<'doc> {
    pub fn create(
        docid: DocumentId,
        external_document_id: &'doc str,
        new: DocumentFromVersions<'doc>,
        has_deletion: bool,
    ) -> Self {
        Update { docid, new, external_document_id, has_deletion }
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

    pub fn updated(&self) -> DocumentFromVersions<'doc> {
        self.new
    }

    pub fn new<'a, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
        mapper: &'a Mapper,
    ) -> Result<MergedDocument<'doc, 'a, Mapper>> {
        if self.has_deletion {
            Ok(MergedDocument::without_db(self.new))
        } else {
            MergedDocument::with_db(self.docid, rtxn, index, mapper, self.new)
        }
    }
}

pub type Entry<'doc> = (&'doc str, &'doc RawValue);

#[derive(Clone, Copy)]
pub enum Versions<'doc> {
    Single(&'doc [Entry<'doc>]),
    Multiple(&'doc [&'doc [Entry<'doc>]]),
}
