use std::collections::BTreeSet;

use fst::{set::OpBuilder, SetBuilder};

use crate::automaton::normalize_str;
use crate::update::{next_update_id, Update};
use crate::{store, MResult};

pub struct StopWordsAddition {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: crossbeam_channel::Sender<()>,
    stop_words: BTreeSet<String>,
}

impl StopWordsAddition {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: crossbeam_channel::Sender<()>,
    ) -> StopWordsAddition {
        StopWordsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            stop_words: BTreeSet::new(),
        }
    }

    pub fn add_stop_word<S: AsRef<str>>(&mut self, stop_word: S) {
        let stop_word = normalize_str(stop_word.as_ref());
        self.stop_words.insert(stop_word);
    }

    pub fn finalize(self, writer: &mut heed::RwTxn) -> MResult<u64> {
        let _ = self.updates_notifier.send(());
        let update_id = push_stop_words_addition(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.stop_words,
        )?;
        Ok(update_id)
    }
}

pub fn push_stop_words_addition(
    writer: &mut heed::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: BTreeSet<String>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::StopWordsAddition(addition);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_stop_words_addition(
    writer: &mut heed::RwTxn,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    addition: BTreeSet<String>,
) -> MResult<()> {
    let mut stop_words_builder = SetBuilder::memory();

    for word in addition {
        stop_words_builder.insert(&word).unwrap();
        // we remove every posting list associated to a new stop word
        postings_lists_store.del_postings_list(writer, word.as_bytes())?;
    }

    // create the new delta stop words fst
    let delta_stop_words = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    // we also need to remove all the stop words from the main fst
    if let Some(word_fst) = main_store.words_fst(writer)? {
        let op = OpBuilder::new()
            .add(&word_fst)
            .add(&delta_stop_words)
            .difference();

        let mut word_fst_builder = SetBuilder::memory();
        word_fst_builder.extend_stream(op).unwrap();
        let word_fst = word_fst_builder
            .into_inner()
            .and_then(fst::Set::from_bytes)
            .unwrap();

        main_store.put_words_fst(writer, &word_fst)?;
    }

    // now we add all of these stop words to the main store
    let stop_words_fst = main_store.stop_words_fst(writer)?.unwrap_or_default();

    let op = OpBuilder::new()
        .add(&stop_words_fst)
        .add(&delta_stop_words)
        .r#union();

    let mut stop_words_builder = SetBuilder::memory();
    stop_words_builder.extend_stream(op).unwrap();
    let stop_words_fst = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    main_store.put_stop_words_fst(writer, &stop_words_fst)?;

    Ok(())
}
