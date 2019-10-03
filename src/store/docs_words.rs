use crate::DocumentId;

#[derive(Copy, Clone)]
pub struct DocsWords {
    pub(crate) docs_words: rkv::SingleStore,
}

impl DocsWords {
    pub fn doc_words<T: rkv::Readable>(
        &self,
        reader: &T,
        document_id: DocumentId,
    ) -> Result<Option<fst::Set>, rkv::StoreError>
    {
        Ok(Some(fst::Set::default()))
    }

    pub fn put_doc_words(
        &self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
        words: &fst::Set,
    ) -> Result<(), rkv::StoreError>
    {
        unimplemented!()
    }

    pub fn del_doc_words(
        &self,
        writer: &mut rkv::Writer,
        document_id: DocumentId,
    ) -> Result<(), rkv::StoreError>
    {
        let document_id_bytes = document_id.0.to_be_bytes();
        self.docs_words.delete(writer, document_id_bytes)
    }
}
