use std::collections::BTreeMap;

use fst::{set::OpBuilder, SetBuilder};
use sdset::SetBuf;

use crate::automaton::normalize_str;
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::update::{next_update_id, Update};
use crate::{store, MResult};

pub struct SynonymsAddition {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    synonyms: BTreeMap<String, Vec<String>>,
}

impl SynonymsAddition {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> SynonymsAddition {
        SynonymsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            synonyms: BTreeMap::new(),
        }
    }

    pub fn add_synonym<S, T, I>(&mut self, synonym: S, alternatives: I)
    where
        S: AsRef<str>,
        T: AsRef<str>,
        I: IntoIterator<Item = T>,
    {
        let synonym = normalize_str(synonym.as_ref());
        let alternatives = alternatives.into_iter().map(|s| s.as_ref().to_lowercase());
        self.synonyms
            .entry(synonym)
            .or_insert_with(Vec::new)
            .extend(alternatives);
    }

    pub fn finalize(self, writer: &mut heed::RwTxn) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_synonyms_addition(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.synonyms,
        )?;
        Ok(update_id)
    }
}

pub fn push_synonyms_addition(
    writer: &mut heed::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: BTreeMap<String, Vec<String>>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::SynonymsAddition(addition);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_synonyms_addition(
    writer: &mut heed::RwTxn,
    main_store: store::Main,
    synonyms_store: store::Synonyms,
    addition: BTreeMap<String, Vec<String>>,
) -> MResult<()> {
    let mut synonyms_builder = SetBuilder::memory();

    for (word, alternatives) in addition {
        synonyms_builder.insert(&word).unwrap();

        let alternatives = {
            let alternatives = SetBuf::from_dirty(alternatives);
            let mut alternatives_builder = SetBuilder::memory();
            alternatives_builder.extend_iter(alternatives).unwrap();
            let bytes = alternatives_builder.into_inner().unwrap();
            fst::Set::from_bytes(bytes).unwrap()
        };

        synonyms_store.put_synonyms(writer, word.as_bytes(), &alternatives)?;
    }

    let delta_synonyms = synonyms_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    let synonyms = match main_store.synonyms_fst(writer)? {
        Some(synonyms) => {
            let op = OpBuilder::new()
                .add(synonyms.stream())
                .add(delta_synonyms.stream())
                .r#union();

            let mut synonyms_builder = SetBuilder::memory();
            synonyms_builder.extend_stream(op).unwrap();
            synonyms_builder
                .into_inner()
                .and_then(fst::Set::from_bytes)
                .unwrap()
        }
        None => delta_synonyms,
    };

    main_store.put_synonyms_fst(writer, &synonyms)?;

    Ok(())
}
