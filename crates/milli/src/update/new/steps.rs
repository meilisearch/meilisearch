use std::borrow::Cow;

use enum_iterator::Sequence;

use crate::progress::Step;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Sequence)]
#[repr(u8)]
pub enum IndexingStep {
    PreparingPayloads,
    ExtractingDocuments,
    ExtractingFacets,
    ExtractingWords,
    ExtractingWordProximity,
    ExtractingEmbeddings,
    WritingGeoPoints,
    WaitingForDatabaseWrites,
    WaitingForExtractors,
    WritingEmbeddingsToDatabase,
    PostProcessingFacets,
    PostProcessingWords,
    Finalizing,
}

impl Step for IndexingStep {
    fn name(&self) -> Cow<'static, str> {
        match self {
            IndexingStep::PreparingPayloads => "preparing update file",
            IndexingStep::ExtractingDocuments => "extracting documents",
            IndexingStep::ExtractingFacets => "extracting facets",
            IndexingStep::ExtractingWords => "extracting words",
            IndexingStep::ExtractingWordProximity => "extracting word proximity",
            IndexingStep::ExtractingEmbeddings => "extracting embeddings",
            IndexingStep::WritingGeoPoints => "writing geo points",
            IndexingStep::WaitingForDatabaseWrites => "waiting for database writes",
            IndexingStep::WaitingForExtractors => "waiting for extractors",
            IndexingStep::WritingEmbeddingsToDatabase => "writing embeddings to database",
            IndexingStep::PostProcessingFacets => "post-processing facets",
            IndexingStep::PostProcessingWords => "post-processing words",
            IndexingStep::Finalizing => "finalizing",
        }
        .into()
    }

    fn current(&self) -> u32 {
        *self as u32
    }

    fn total(&self) -> u32 {
        Self::CARDINALITY as u32
    }
}
