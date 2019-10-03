mod documents_addition;
mod documents_deletion;

pub use self::documents_addition::{DocumentsAddition, apply_documents_addition};
pub use self::documents_deletion::{DocumentsDeletion, apply_documents_deletion};

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use crate::{store, DocumentId};
use super::Error;

#[derive(Serialize, Deserialize)]
pub enum Update {
    DocumentsAddition(Vec<rmpv::Value>),
    DocumentsDeletion(Vec<DocumentId>),
    SynonymsAddition(BTreeMap<String, Vec<String>>),
    SynonymsDeletion(BTreeMap<String, Option<Vec<String>>>),
}

pub fn push_documents_addition<D: serde::Serialize>(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    addition: Vec<D>,
) -> Result<u64, Error>
{
    let mut values = Vec::with_capacity(addition.len());
    for add in addition {
        let vec = rmp_serde::to_vec_named(&add)?;
        let add = rmp_serde::from_read(&vec[..])?;
        values.push(add);
    }

    let update = Update::DocumentsAddition(values);
    Ok(updates_store.push_back(writer, &update)?)
}

pub fn push_documents_deletion(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    deletion: Vec<DocumentId>,
) -> Result<u64, Error>
{
    let update = Update::DocumentsDeletion(deletion);
    Ok(updates_store.push_back(writer, &update)?)
}

pub fn push_synonyms_addition(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    addition: BTreeMap<String, Vec<String>>,
) -> Result<u64, Error>
{
    let update = Update::SynonymsAddition(addition);
    Ok(updates_store.push_back(writer, &update)?)
}

pub fn push_synonyms_deletion(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    deletion: BTreeMap<String, Option<Vec<String>>>,
) -> Result<u64, Error>
{
    let update = Update::SynonymsDeletion(deletion);
    Ok(updates_store.push_back(writer, &update)?)
}
