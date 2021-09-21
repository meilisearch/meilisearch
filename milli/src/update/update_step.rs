use UpdateIndexingStep::*;

#[derive(Debug, Clone, Copy)]
pub enum UpdateIndexingStep {
    /// Remap document addition fields the one present in the database, adding new fields in to the
    /// schema on the go.
    RemapDocumentAddition { documents_seen: usize },

    /// This step check the external document id, computes the internal ids and merge
    /// the documents that are already present in the database.
    ComputeIdsAndMergeDocuments { documents_seen: usize, total_documents: usize },

    /// Extract the documents words using the tokenizer and compute the documents
    /// facets. Stores those words, facets and documents ids on disk.
    IndexDocuments { documents_seen: usize, total_documents: usize },

    /// Merge the previously extracted data (words and facets) into the final LMDB database.
    /// These extracted data are split into multiple databases.
    MergeDataIntoFinalDatabase { databases_seen: usize, total_databases: usize },
}

impl UpdateIndexingStep {
    pub const fn step(&self) -> usize {
        match self {
            RemapDocumentAddition { .. } => 0,
            ComputeIdsAndMergeDocuments { .. } => 1,
            IndexDocuments { .. } => 2,
            MergeDataIntoFinalDatabase { .. } => 3,
        }
    }

    pub const fn number_of_steps(&self) -> usize {
        4
    }
}
