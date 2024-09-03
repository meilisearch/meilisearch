use heed::RoTxn;
use obkv::KvReader;

use crate::update::new::KvReaderFieldId;
use crate::{DocumentId, FieldId, Index};

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

    pub fn current(&self, rtxn: &RoTxn, index: &Index) -> &KvReader<FieldId> {
        unimplemented!()
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
        unimplemented!()
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

    pub fn current(&self, rtxn: &RoTxn, index: &Index) -> &KvReader<FieldId> {
        unimplemented!()
    }

    pub fn new(&self) -> &KvReader<FieldId> {
        unimplemented!()
    }
}
