use std::collections::BTreeMap;
use std::iter::FromIterator;

use fst::{SetBuilder, set::OpBuilder};
use sdset::SetBuf;

use crate::automaton::normalize_str;
use crate::update::{Update, next_update_id};
use crate::{store, MResult};

pub struct SynonymsDeletion {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: crossbeam_channel::Sender<()>,
    synonyms: BTreeMap<String, Option<Vec<String>>>,
}

impl SynonymsDeletion {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: crossbeam_channel::Sender<()>,
    ) -> SynonymsDeletion
    {
        SynonymsDeletion {
            updates_store,
            updates_results_store,
            updates_notifier,
            synonyms: BTreeMap::new(),
        }
    }

    pub fn delete_all_alternatives_of<S: AsRef<str>>(&mut self, synonym: S) {
        let synonym = normalize_str(synonym.as_ref());
        self.synonyms.insert(synonym, None);
    }

    pub fn delete_specific_alternatives_of<S, T, I>(&mut self, synonym: S, alternatives: I)
    where S: AsRef<str>,
          T: AsRef<str>,
          I: Iterator<Item=T>,
    {
        let synonym = normalize_str(synonym.as_ref());
        let value = self.synonyms.entry(synonym).or_insert(None);
        let alternatives = alternatives.map(|s| s.as_ref().to_lowercase());
        match value {
            Some(v) => v.extend(alternatives),
            None => *value = Some(Vec::from_iter(alternatives)),
        }
    }

    pub fn finalize(self, writer: &mut zlmdb::RwTxn) -> MResult<u64> {
        let _ = self.updates_notifier.send(());
        let update_id = push_synonyms_deletion(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.synonyms,
        )?;
        Ok(update_id)
    }
}

pub fn push_synonyms_deletion(
    writer: &mut zlmdb::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    deletion: BTreeMap<String, Option<Vec<String>>>,
) -> MResult<u64>
{
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::SynonymsDeletion(deletion);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_synonyms_deletion(
    writer: &mut zlmdb::RwTxn,
    main_store: store::Main,
    synonyms_store: store::Synonyms,
    deletion: BTreeMap<String, Option<Vec<String>>>,
) -> MResult<()>
{
    let mut delete_whole_synonym_builder = SetBuilder::memory();

    for (synonym, alternatives) in deletion {
        match alternatives {
            Some(alternatives) => {
                let prev_alternatives = synonyms_store.synonyms(writer, synonym.as_bytes())?;
                let prev_alternatives = match prev_alternatives {
                    Some(alternatives) => alternatives,
                    None => continue,
                };

                let delta_alternatives = {
                    let alternatives = SetBuf::from_dirty(alternatives);
                    let mut builder = SetBuilder::memory();
                    builder.extend_iter(alternatives).unwrap();
                    builder.into_inner()
                        .and_then(fst::Set::from_bytes)
                        .unwrap()
                };

                let op = OpBuilder::new()
                    .add(prev_alternatives.stream())
                    .add(delta_alternatives.stream())
                    .difference();

                let (alternatives, empty_alternatives) = {
                    let mut builder = SetBuilder::memory();
                    let len = builder.get_ref().len();
                    builder.extend_stream(op).unwrap();
                    let is_empty = len == builder.get_ref().len();
                    let bytes = builder.into_inner().unwrap();
                    let alternatives = fst::Set::from_bytes(bytes).unwrap();

                    (alternatives, is_empty)
                };

                if empty_alternatives {
                    delete_whole_synonym_builder.insert(synonym.as_bytes())?;
                } else {
                    synonyms_store.put_synonyms(writer, synonym.as_bytes(), &alternatives)?;
                }
            },
            None => {
                delete_whole_synonym_builder.insert(&synonym).unwrap();
                synonyms_store.del_synonyms(writer, synonym.as_bytes())?;
            }
        }
    }

    let delta_synonyms = delete_whole_synonym_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    let synonyms = match main_store.synonyms_fst(writer)? {
        Some(synonyms) => {
            let op = OpBuilder::new()
                .add(synonyms.stream())
                .add(delta_synonyms.stream())
                .difference();

            let mut synonyms_builder = SetBuilder::memory();
            synonyms_builder.extend_stream(op).unwrap();
            synonyms_builder
                .into_inner()
                .and_then(fst::Set::from_bytes)
                .unwrap()
        },
        None => fst::Set::default(),
    };

    main_store.put_synonyms_fst(writer, &synonyms)?;

    Ok(())
}
