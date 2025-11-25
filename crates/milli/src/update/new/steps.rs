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
        WritingGeoJson,
        WaitingForDatabaseWrites,
        WaitingForExtractors,
        WritingEmbeddingsToDatabase,
        PostProcessingFacets,
        PostProcessingWords,
        BuildingGeoJson,
        Finalizing,
    }
}

make_enum_progress! {
    pub enum SettingsIndexerStep {
        ChangingVectorStore,
        UsingStableIndexer,
        UsingExperimentalIndexer,
        DeletingOldWordFidDocids,
        DeletingOldFidWordCountDocids,
        DeletingOldWordPrefixFidDocids,
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
