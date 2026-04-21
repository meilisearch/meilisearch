use crate::make_enum_progress;

make_enum_progress! {
    pub enum SearchStep {
        TokenizeQuery,
        EmbedQuery,
        EvaluateFilter,
        EvaluateQuery,
        KeywordRanking,
        PlaceholderRanking,
        SemanticRanking,
        Format,
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
        Search,
        Similar,
    }
}
