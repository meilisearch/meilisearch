use super::{MainIndex, WordsIndex, DocsWordsIndex, DocumentsIndex};

#[derive(Clone)]
pub struct RawIndex {
    pub main: MainIndex,
    pub words: WordsIndex,
    pub docs_words: DocsWordsIndex,
    pub documents: DocumentsIndex,
}
