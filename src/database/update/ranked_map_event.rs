use sdset::{Set, SetBuf};
use serde_derive::{Serialize, Deserialize};

use crate::database::RankedMap;
use crate::DocumentId;

#[derive(Serialize)]
pub enum WriteRankedMapEvent<'a> {
    RemovedDocuments(&'a Set<DocumentId>),
    UpdatedDocuments(&'a RankedMap),
}

#[derive(Deserialize)]
pub enum ReadRankedMapEvent {
    RemovedDocuments(SetBuf<DocumentId>),
    UpdatedDocuments(RankedMap),
}
