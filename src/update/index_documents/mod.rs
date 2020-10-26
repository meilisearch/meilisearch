use crate::Index;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexDocumentsMethod {
    /// Replace the previous document with the new one,
    /// removing all the already known attributes.
    ReplaceDocuments,

    /// Merge the previous version of the document with the new version,
    /// replacing old attributes values with the new ones and add the new attributes.
    UpdateDocuments,
}

pub struct IndexDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'u>,
    index: &'i Index,
    update_method: IndexDocumentsMethod,
}

impl<'t, 'u, 'i> IndexDocuments<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'u>, index: &'i Index) -> IndexDocuments<'t, 'u, 'i> {
        IndexDocuments { wtxn, index, update_method: IndexDocumentsMethod::ReplaceDocuments }
    }

    pub fn index_documents_method(&mut self, method: IndexDocumentsMethod) -> &mut Self {
        self.update_method = method;
        self
    }

    pub fn execute(self) -> anyhow::Result<()> {
        todo!()
    }
}
