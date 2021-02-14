use roaring::RoaringBitmap;
use crate::{ExternalDocumentsIds, Index};

pub struct ClearDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    _update_id: u64,
}

impl<'t, 'u, 'i> ClearDocuments<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        update_id: u64
    ) -> ClearDocuments<'t, 'u, 'i> {

        ClearDocuments { wtxn, index, _update_id: update_id }
    }

    pub fn execute(self) -> anyhow::Result<usize> {
        let Index {
            env: _env,
            main: _main,
            word_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            facet_field_id_value_docids,
            field_id_docid_facet_values,
            documents,
        } = self.index;

        // We retrieve the number of documents ids that we are deleting.
        let number_of_documents = self.index.number_of_documents(self.wtxn)?;
        let faceted_fields = self.index.faceted_fields_ids(self.wtxn)?;

        // We clean some of the main engine datastructures.
        self.index.put_words_fst(self.wtxn, &fst::Set::default())?;
        self.index.put_external_documents_ids(self.wtxn, &ExternalDocumentsIds::default())?;
        self.index.put_documents_ids(self.wtxn, &RoaringBitmap::default())?;

        // We clean all the faceted documents ids.
        for (field_id, _) in faceted_fields {
            self.index.put_faceted_documents_ids(self.wtxn, field_id, &RoaringBitmap::default())?;
        }

        // Clear the other databases.
        word_docids.clear(self.wtxn)?;
        docid_word_positions.clear(self.wtxn)?;
        word_pair_proximity_docids.clear(self.wtxn)?;
        facet_field_id_value_docids.clear(self.wtxn)?;
        field_id_docid_facet_values.clear(self.wtxn)?;
        documents.clear(self.wtxn)?;

        Ok(number_of_documents)
    }
}
