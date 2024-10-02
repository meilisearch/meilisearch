use heed::RoTxn;
use obkv::KvReader;

use crate::update::new::KvReaderFieldId;
use crate::{DocumentId, FieldId, Index, Result};

pub enum DocumentChange {
    Deletion(Deletion),
    Update(Update),
    Insertion(Insertion),
}

pub struct Deletion {
    pub docid: DocumentId,
    pub external_document_id: String,
    current: Box<KvReaderFieldId>,
}

pub struct Update {
    pub docid: DocumentId,
    pub external_document_id: String,
    current: Box<KvReaderFieldId>,
    pub new: Box<KvReaderFieldId>,
}

pub struct Insertion {
    pub docid: DocumentId,
    pub external_document_id: String,
    pub new: Box<KvReaderFieldId>,
}

impl DocumentChange {
    pub fn docid(&self) -> DocumentId {
        match &self {
            Self::Deletion(inner) => inner.docid(),
            Self::Update(inner) => inner.docid(),
            Self::Insertion(inner) => inner.docid(),
        }
    }
}

impl Deletion {
    pub fn create(
        docid: DocumentId,
        external_document_id: String,
        current: Box<KvReaderFieldId>,
    ) -> Self {
        Self { docid, external_document_id, current }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &str {
        &self.external_document_id
    }

    // TODO shouldn't we use the one in self?
    pub fn current<'a>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<&'a KvReader<FieldId>>> {
        index.documents.get(rtxn, &self.docid).map_err(crate::Error::from)
    }
}

impl Insertion {
    pub fn create(
        docid: DocumentId,
        external_document_id: String,
        new: Box<KvReaderFieldId>,
    ) -> Self {
        Insertion { docid, external_document_id, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &str {
        &self.external_document_id
    }

    pub fn new(&self) -> &KvReader<FieldId> {
        self.new.as_ref()
    }
}

impl Update {
    pub fn create(
        docid: DocumentId,
        external_document_id: String,
        current: Box<KvReaderFieldId>,
        new: Box<KvReaderFieldId>,
    ) -> Self {
        Update { docid, external_document_id, current, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn external_document_id(&self) -> &str {
        &self.external_document_id
    }

    pub fn current<'a>(
        &self,
        rtxn: &'a RoTxn,
        index: &'a Index,
    ) -> Result<Option<&'a KvReader<FieldId>>> {
        index.documents.get(rtxn, &self.docid).map_err(crate::Error::from)
    }

    pub fn new(&self) -> &KvReader<FieldId> {
        self.new.as_ref()
    }
}
