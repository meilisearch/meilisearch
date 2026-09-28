use crate::make_enum_progress;

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
    pub enum FacetDistributionStep {
        ComputeFacetDistribution,
        ComputeFacetStats,
    }
}

make_enum_progress! {
    pub enum TotalProcessingTimeStep {
        WaitInQueue,
        PreprocessFilters,
        Process,
        Hydrate,
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
