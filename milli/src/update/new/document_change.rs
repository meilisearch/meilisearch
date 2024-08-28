use heed::RoTxn;
use obkv2::KvReader;

use super::indexer::KvReaderFieldId;
use crate::{DocumentId, FieldId};

pub enum DocumentChange {
    Deletion(Deletion),
    Update(Update),
    Insertion(Insertion),
}

pub struct Deletion {
    docid: DocumentId,
    external_docid: String, // ?
    current: Box<KvReaderFieldId>,
}

pub struct Update {
    docid: DocumentId,
    external_docid: String, // ?
    current: Box<KvReaderFieldId>,
    new: Box<KvReaderFieldId>,
}

pub struct Insertion {
    docid: DocumentId,
    external_docid: String, // ?
    new: Box<KvReaderFieldId>,
}

impl DocumentChange {
    fn docid(&self) -> DocumentId {
        match &self {
            Self::Deletion(inner) => inner.docid(),
            Self::Update(inner) => inner.docid(),
            Self::Insertion(inner) => inner.docid(),
        }
    }
}

impl Deletion {
    pub fn new(docid: DocumentId, external_docid: String, current: Box<KvReaderFieldId>) -> Self {
        Self { docid, external_docid, current }
    }

    fn docid(&self) -> DocumentId {
        self.docid
    }

    fn current(&self, rtxn: &RoTxn) -> &KvReader<FieldId> {
        unimplemented!()
    }
}

impl Insertion {
    fn docid(&self) -> DocumentId {
        self.docid
    }

    fn new(&self) -> &KvReader<FieldId> {
        unimplemented!()
    }
}

impl Update {
    fn docid(&self) -> DocumentId {
        self.docid
    }

    fn current(&self, rtxn: &RoTxn) -> &KvReader<FieldId> {
        unimplemented!()
    }

    fn new(&self) -> &KvReader<FieldId> {
        unimplemented!()
    }
}
