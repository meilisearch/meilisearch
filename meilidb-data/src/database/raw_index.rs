use super::{MainIndex, WordsIndex, DocsWordsIndex, DocumentsIndex, CustomSettings};

#[derive(Clone)]
pub struct RawIndex {
    pub main: MainIndex,
    pub words: WordsIndex,
    pub docs_words: DocsWordsIndex,
    pub documents: DocumentsIndex,
    pub custom: CustomSettings,
}
