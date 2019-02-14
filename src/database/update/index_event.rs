use sdset::{Set, SetBuf};
use serde_derive::{Serialize, Deserialize};

use crate::database::Index;
use crate::DocumentId;

#[derive(Serialize)]
pub enum WriteIndexEvent<'a> {
    RemovedDocuments(&'a Set<DocumentId>),
    UpdatedDocuments(&'a Index),
}

#[derive(Deserialize)]
pub enum ReadIndexEvent {
    RemovedDocuments(SetBuf<DocumentId>),
    UpdatedDocuments(Index),
}
