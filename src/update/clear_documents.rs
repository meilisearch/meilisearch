use roaring::RoaringBitmap;
use crate::Index;

pub struct ClearDocuments<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'u>,
    index: &'i Index,
}

impl<'t, 'u, 'i> ClearDocuments<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'u>, index: &'i Index) -> ClearDocuments<'t, 'u, 'i> {
        ClearDocuments { wtxn, index }
    }

    pub fn execute(self) -> anyhow::Result<usize> {
        let Index {
            main: _main,
            word_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            documents,
        } = self.index;

        // We clear the word fst.
        self.index.put_words_fst(self.wtxn, &fst::Set::default())?;

        // We clear the users ids documents ids.
        self.index.put_users_ids_documents_ids(self.wtxn, &fst::Map::default())?;

        // We retrieve the documents ids.
        let documents_ids = self.index.documents_ids(self.wtxn)?;

        // We clear the internal documents ids.
        self.index.put_documents_ids(self.wtxn, &RoaringBitmap::default())?;

        // We clear the word docids.
        word_docids.clear(self.wtxn)?;

        // We clear the docid word positions.
        docid_word_positions.clear(self.wtxn)?;

        // We clear the word pair proximity docids.
        word_pair_proximity_docids.clear(self.wtxn)?;

        // We clear the documents themselves.
        documents.clear(self.wtxn)?;

        Ok(documents_ids.len() as usize)
    }
}
