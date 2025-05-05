use crate::make_enum_progress;

make_enum_progress! {
    pub enum IndexingStep {
        PreparingPayloads,
        ExtractingDocuments,
        ExtractingFacets,
        ExtractingWords,
        ExtractingWordProximity,
        ExtractingEmbeddings,
        MergingFacetCaches,
        MergingWordCaches,
        MergingWordProximity,
        WritingGeoPoints,
        WaitingForDatabaseWrites,
        WaitingForExtractors,
        WritingEmbeddingsToDatabase,
        PostProcessingFacets,
        PostProcessingWords,
        Finalizing,
    }
}

make_enum_progress! {
    pub enum PostProcessingFacets {
        StringsBulk,
        StringsIncremental,
        NumbersBulk,
        NumbersIncremental,
        FacetSearch,
    }
}

make_enum_progress! {
    pub enum PostProcessingWords {
        WordFst,
        WordPrefixDocids,
        ExactWordPrefixDocids,
        WordPrefixFieldIdDocids,
        WordPrefixPositionDocids,
    }
}
