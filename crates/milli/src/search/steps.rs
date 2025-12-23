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
        Federation,
        Personalization,
    }
}

make_enum_progress! {
    pub enum ComputingBucketSortStep {
        MergeCandidates,
        Distinct,
        Words,
        Typo,
        Proximity,
        AttributePosition,
        WordPosition,
        Exactness,
        Sort,
        GeoSort,
        VectorSort,
        Asc,
        Desc,
    }
}

make_enum_progress! {
    pub enum RankingRuleStep {
        StartIteration,
        NextBucket,
        NonBlockingNextBucket,
    }
}

make_enum_progress! {
    pub enum FederatingResultsStep {
        WaitForRemoteResults,
        MergeFacets,
        MergeResults,
    }
}

make_enum_progress! {
    pub enum TotalProcessingTimeStep {
        WaitForPermit,
        Search,
        Similar,
    }
}
