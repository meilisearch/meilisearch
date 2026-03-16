use crate::make_enum_progress;

make_enum_progress! {
    pub enum SearchStep {
        Tokenize,
        Embed,
        Filter,
        ResolveUniverse,
        KeywordSearch,
        PlaceholderSearch,
        SemanticSearch,
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
        WaitForPermit,
        Search,
        Similar,
    }
}
