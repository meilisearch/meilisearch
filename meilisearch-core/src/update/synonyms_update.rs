use std::collections::BTreeMap;

use fst::SetBuilder;
use sdset::SetBuf;

use crate::database::{MainT, UpdateT};
use crate::automaton::normalize_str;
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::update::{next_update_id, Update};
use crate::{store, MResult};

pub struct SynonymsUpdate {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    synonyms: BTreeMap<String, Vec<String>>,
}

impl SynonymsUpdate {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> SynonymsUpdate {
        SynonymsUpdate {
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

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_synonyms_update(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.synonyms,
        )?;
        Ok(update_id)
    }
}

pub fn push_synonyms_update(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: BTreeMap<String, Vec<String>>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::synonyms_update(addition);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_synonyms_update(
    writer: &mut heed::RwTxn<MainT>,
    main_store: store::Main,
    synonyms_store: store::Synonyms,
    addition: BTreeMap<String, Vec<String>>,
) -> MResult<()> {
    let mut synonyms_builder = SetBuilder::memory();
    synonyms_store.clear(writer)?;
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

    let synonyms = synonyms_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    main_store.put_synonyms_fst(writer, &synonyms)?;

    Ok(())
}
