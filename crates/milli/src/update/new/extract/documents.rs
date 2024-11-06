use std::cell::RefCell;

use bumpalo::Bump;

use super::DelAddRoaringBitmap;
use crate::update::new::channel::DocumentsSender;
use crate::update::new::document::write_to_obkv;
use crate::update::new::indexer::document_changes::{
    DocumentChangeContext, Extractor, FullySend, RefCellExt as _,
};
use crate::update::new::DocumentChange;
use crate::Result;

pub struct DocumentsExtractor<'a> {
    documents_sender: &'a DocumentsSender<'a>,
}

impl<'a> DocumentsExtractor<'a> {
    pub fn new(documents_sender: &'a DocumentsSender<'a>) -> Self {
        Self { documents_sender }
    }
}

impl<'a, 'extractor> Extractor<'extractor> for DocumentsExtractor<'a> {
    type Data = FullySend<RefCell<DelAddRoaringBitmap>>;

    fn init_data(&self, _extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(FullySend(RefCell::new(DelAddRoaringBitmap::empty())))
    }

    fn process(
        &self,
        change: DocumentChange,
        context: &DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        let mut document_buffer = Vec::new();
        let mut delta_documents_ids = context.data.0.borrow_mut_or_yield();

        let new_fields_ids_map = context.new_fields_ids_map.borrow_or_yield();
        let new_fields_ids_map = &*new_fields_ids_map;
        let new_fields_ids_map = new_fields_ids_map.local_map();

        let external_docid = change.external_docid().to_owned();

        // document but we need to create a function that collects and compresses documents.
        match change {
            DocumentChange::Deletion(deletion) => {
                let docid = deletion.docid();
                self.documents_sender.delete(docid, external_docid).unwrap();
                delta_documents_ids.insert_del_u32(docid);
            }
            /// TODO: change NONE by SOME(vector) when implemented
            DocumentChange::Update(update) => {
                let docid = update.docid();
                let content =
                    update.new(&context.txn, context.index, &context.db_fields_ids_map)?;
                let content =
                    write_to_obkv(&content, None, new_fields_ids_map, &mut document_buffer)?;
                self.documents_sender.uncompressed(docid, external_docid, content).unwrap();
            }
            DocumentChange::Insertion(insertion) => {
                let docid = insertion.docid();
                let content = insertion.new();
                let content =
                    write_to_obkv(&content, None, new_fields_ids_map, &mut document_buffer)?;
                self.documents_sender.uncompressed(docid, external_docid, content).unwrap();
                delta_documents_ids.insert_add_u32(docid);
                // extracted_dictionary_sender.send(self, dictionary: &[u8]);
            }
        }
        Ok(())
    }
}
