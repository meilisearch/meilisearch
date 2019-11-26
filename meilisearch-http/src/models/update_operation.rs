use std::fmt;

#[allow(dead_code)]
#[derive(Debug)]
pub enum UpdateOperation {
    ClearAllDocuments,
    DocumentsAddition,
    DocumentsDeletion,
    SynonymsAddition,
    SynonymsDeletion,
    StopWordsAddition,
    StopWordsDeletion,
    Schema,
    Config,
}

impl fmt::Display for UpdateOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        use UpdateOperation::*;

        match self {
            ClearAllDocuments => write!(f, "ClearAllDocuments"),
            DocumentsAddition => write!(f, "DocumentsAddition"),
            DocumentsDeletion => write!(f, "DocumentsDeletion"),
            SynonymsAddition => write!(f, "SynonymsAddition"),
            SynonymsDeletion => write!(f, "SynonymsDelettion"),
            StopWordsAddition => write!(f, "StopWordsAddition"),
            StopWordsDeletion => write!(f, "StopWordsDeletion"),
            Schema => write!(f, "Schema"),
            Config => write!(f, "Config"),
        }
    }
}
