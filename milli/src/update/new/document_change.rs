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
    docid: DocumentId,
    external_docid: String,        // ?
    current: Box<KvReaderFieldId>, // ?
}

pub struct Update {
    docid: DocumentId,
    external_docid: String,        // ?
    current: Box<KvReaderFieldId>, // ?
    new: Box<KvReaderFieldId>,
}

pub struct Insertion {
    docid: DocumentId,
    external_docid: String, // ?
    new: Box<KvReaderFieldId>,
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
        external_docid: String,
        current: Box<KvReaderFieldId>,
    ) -> Self {
        Self { docid, external_docid, current }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
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
    pub fn create(docid: DocumentId, external_docid: String, new: Box<KvReaderFieldId>) -> Self {
        Insertion { docid, external_docid, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
    }

    pub fn new(&self) -> &KvReader<FieldId> {
        self.new.as_ref()
    }
}

impl Update {
    pub fn create(
        docid: DocumentId,
        external_docid: String,
        current: Box<KvReaderFieldId>,
        new: Box<KvReaderFieldId>,
    ) -> Self {
        Update { docid, external_docid, current, new }
    }

    pub fn docid(&self) -> DocumentId {
        self.docid
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
