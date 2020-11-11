use roaring::RoaringBitmap;
use crate::Index;

pub struct ClearDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
}

impl<'t, 'u, 'i> ClearDocuments<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> ClearDocuments<'t, 'u, 'i> {
        ClearDocuments { wtxn, index }
    }

    pub fn execute(self) -> anyhow::Result<usize> {
        let Index {
            env: _env,
            main: _main,
            word_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            facet_field_id_value_docids,
            documents,
        } = self.index;

        // We retrieve the number of documents ids that we are deleting.
        let number_of_documents = self.index.number_of_documents(self.wtxn)?;

        // We clean some of the main engine datastructures.
        self.index.put_words_fst(self.wtxn, &fst::Set::default())?;
        self.index.put_users_ids_documents_ids(self.wtxn, &fst::Map::default())?;
        self.index.put_documents_ids(self.wtxn, &RoaringBitmap::default())?;

        // Clear the other databases.
        word_docids.clear(self.wtxn)?;
        docid_word_positions.clear(self.wtxn)?;
        word_pair_proximity_docids.clear(self.wtxn)?;
        facet_field_id_value_docids.clear(self.wtxn)?;
        documents.clear(self.wtxn)?;

        Ok(number_of_documents)
    }
}
