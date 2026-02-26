use crate::make_enum_progress;
use crate::progress::Step;
use std::borrow::Cow;

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

#[derive(Debug, Clone, Copy)]
pub enum FstBuildingStep {
    Building { total: usize, built: usize },
}

impl Step for FstBuildingStep {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("fst building")
    }

    fn current(&self) -> u32 {
        match self {
            Self::Building { built, .. } => *built as u32,
        }
    }

    fn total(&self) -> u32 {
        match self {
            FstBuildingStep::Building { total, .. } => *total as u32,
        }
    }
}
