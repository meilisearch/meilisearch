use bumpalo::Bump;
use heed::RoTxn;

use super::document::{DocumentFromDb, DocumentFromVersions, MergedDocument, Versions};
use super::vector_document::{VectorDocumentFromDb, VectorDocumentFromVersions};
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
    new: Versions<'doc>,
    has_deletion: bool,
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
    pub fn create(docid: DocumentId, external_document_id: &'doc str, new: Versions<'doc>) -> Self {
        Insertion { docid, external_document_id, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &'doc str {
        self.external_document_id
    }
    pub fn new(&self) -> DocumentFromVersions<'_, 'doc> {
        DocumentFromVersions::new(&self.new)
    }

    pub fn inserted_vectors(
        &self,
        doc_alloc: &'doc Bump,
    ) -> Result<Option<VectorDocumentFromVersions<'doc>>> {
        VectorDocumentFromVersions::new(&self.new, doc_alloc)
    }
}

impl<'doc> Update<'doc> {
    pub fn create(
        docid: DocumentId,
        external_document_id: &'doc str,
        new: Versions<'doc>,
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

    pub fn updated(&self) -> DocumentFromVersions<'_, 'doc> {
        DocumentFromVersions::new(&self.new)
    }

    pub fn new<'t, Mapper: FieldIdMapper>(
        &self,
        rtxn: &'t RoTxn,
        index: &'t Index,
        mapper: &'t Mapper,
    ) -> Result<MergedDocument<'_, 'doc, 't, Mapper>> {
        if self.has_deletion {
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

    pub fn updated_vectors(
        &self,
        doc_alloc: &'doc Bump,
    ) -> Result<Option<VectorDocumentFromVersions<'doc>>> {
        VectorDocumentFromVersions::new(&self.new, doc_alloc)
    }
}
