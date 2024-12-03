use enum_iterator::Sequence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Sequence)]
#[repr(u16)]
pub enum Step {
    PreparingPayloads,
    ExtractingDocuments,
    ExtractingFacets,
    ExtractingWords,
    ExtractingWordProximity,
    ExtractingEmbeddings,
    WritingGeoPoints,
    WritingToDatabase,
    WaitingForExtractors,
    WritingEmbeddingsToDatabase,
    PostProcessingFacets,
    PostProcessingWords,
    Finalizing,
}

impl Step {
    pub fn name(&self) -> &'static str {
        match self {
            Step::PreparingPayloads => "preparing update file",
            Step::ExtractingDocuments => "extracting documents",
            Step::ExtractingFacets => "extracting facets",
            Step::ExtractingWords => "extracting words",
            Step::ExtractingWordProximity => "extracting word proximity",
            Step::ExtractingEmbeddings => "extracting embeddings",
            Step::WritingGeoPoints => "writing geo points",
            Step::WritingToDatabase => "writing to database",
            Step::WaitingForExtractors => "waiting for extractors",
            Step::WritingEmbeddingsToDatabase => "writing embeddings to database",
            Step::PostProcessingFacets => "post-processing facets",
            Step::PostProcessingWords => "post-processing words",
            Step::Finalizing => "finalizing",
        }
    }

    pub fn finished_steps(self) -> u16 {
        self as u16
    }

    pub const fn total_steps() -> u16 {
        Self::CARDINALITY as u16
    }
}
