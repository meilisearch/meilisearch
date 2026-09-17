use crate::make_enum_progress;

// Search steps

make_enum_progress! {
    pub enum RetrieveIndexDataStep {
        LoadFieldIdsMap,
        TokenizeQuery,
        EmbedQuery,
        EvaluateFilter,
        EvaluateQuery,
        KeywordRanking,
        PlaceholderRanking,
        SemanticRanking,
        Format,
        PinHits,
        FacetDistribution,
        Personalization,
    }
}

make_enum_progress! {
    pub enum FederatingResultsStep {
        PartitionQueries,
        StartRemoteSearch,
        ExecuteLocalSearch,
        WaitForRemoteResults,
        MergeResults,
        HydrateDocuments,
        MergeFacets,
    }
}

make_enum_progress! {
    pub enum TotalProcessingTimeStep {
        WaitInQueue,
        PreprocessFilters,
        Process,
        Hydrate,
        MergeFacets,
    }
}

make_enum_progress! {
    pub enum PerformRetrievalStep {
        Prepare,
        SendToRemote,
        ExecuteLocal,
        WaitForRemote,
        Merge,
        Personalize,
        Format,
        PinHits,
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct QueryStep {
    current: usize,
    total: usize,
}

impl QueryStep {
    pub fn new(current: usize, mut total: usize) -> Self {
        if current >= total {
            // warn because we don't want to break the search because of this
            tracing::warn!("current is greater than total");
            total = current + 1;
        }

        Self { current, total }
    }
}

impl crate::progress::Step for QueryStep {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        format!("query[{}]", self.current).into()
    }

    fn current(&self) -> u32 {
        self.current as u32
    }

    fn total(&self) -> u32 {
        self.total as u32
    }
}

// Indexing steps

make_enum_progress! {
    pub enum IndexingStep {
        PreparingPayloads,
        AssigningDocumentsIds,
        ReorderingPayloadOffsets,
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
        WritingEmbeddingsToDatabase,
        DeletingFromAllFilters,
        DeletingFromFacetsOnly,
        DeletingFromComparisonsOnly,
        DeletingFromGeoDatabases,
        WaitingForDatabaseWrites,
        WaitingForExtractors,
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
        ComputePrefixFst,
        ComputePrefixes,
        WordPrefixDocids,
        ExactWordPrefixDocids,
        WordPrefixFieldIdDocids,
        WordPrefixPositionDocids,
    }
}

make_enum_progress! {
    pub enum MergingWordCache {
        WordDocids,
        WordFieldIdDocids,
        ExactWordDocids,
        WordPositionDocids,
        FieldIdWordCountDocids,
    }
}
