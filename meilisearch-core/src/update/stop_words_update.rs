use std::collections::BTreeSet;

use fst::{set::OpBuilder, SetBuilder};

use crate::automaton::normalize_str;
use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::update::documents_addition::reindex_all_documents;
use crate::update::{next_update_id, Update};
use crate::{store, MResult};

pub struct StopWordsUpdate {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    stop_words: BTreeSet<String>,
}

impl StopWordsUpdate {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> StopWordsUpdate {
        StopWordsUpdate {
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

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_stop_words_update(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.stop_words,
        )?;
        Ok(update_id)
    }
}

pub fn push_stop_words_update(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    update: BTreeSet<String>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::stop_words_update(update);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_stop_words_update(
    writer: &mut heed::RwTxn<MainT>,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    stop_words: BTreeSet<String>,
) -> MResult<()> {

    let old_stop_words: BTreeSet<String> = main_store
        .stop_words_fst(writer)?
        .unwrap_or_default()
        .stream()
        .into_strs().unwrap().into_iter().collect();

    let deletion: BTreeSet<String> = old_stop_words.clone().difference(&stop_words).cloned().collect();
    let addition: BTreeSet<String> = stop_words.clone().difference(&old_stop_words).cloned().collect();

    if !addition.is_empty() {
        apply_stop_words_addition(
            writer,
            main_store,
            postings_lists_store,
            addition
        )?;
    }

    if !deletion.is_empty() {
        apply_stop_words_deletion(
            writer,
            main_store,
            documents_fields_store,
            documents_fields_counts_store,
            postings_lists_store,
            docs_words_store,
            deletion
        )?;
    }

    Ok(())
}

fn apply_stop_words_addition(
    writer: &mut heed::RwTxn<MainT>,
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

    // now we add all of these stop words from the main store
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

fn apply_stop_words_deletion(
    writer: &mut heed::RwTxn<MainT>,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    deletion: BTreeSet<String>,
) -> MResult<()> {
    let mut stop_words_builder = SetBuilder::memory();

    for word in deletion {
        stop_words_builder.insert(&word).unwrap();
    }

    // create the new delta stop words fst
    let delta_stop_words = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    // now we delete all of these stop words from the main store
    let stop_words_fst = main_store.stop_words_fst(writer)?.unwrap_or_default();

    let op = OpBuilder::new()
        .add(&stop_words_fst)
        .add(&delta_stop_words)
        .difference();

    let mut stop_words_builder = SetBuilder::memory();
    stop_words_builder.extend_stream(op).unwrap();
    let stop_words_fst = stop_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    main_store.put_stop_words_fst(writer, &stop_words_fst)?;

    // now that we have setup the stop words
    // lets reindex everything...
    if let Ok(number) = main_store.number_of_documents(writer) {
        if number > 0 {
            reindex_all_documents(
                writer,
                main_store,
                documents_fields_store,
                documents_fields_counts_store,
                postings_lists_store,
                docs_words_store,
            )?;
        }
    }

    Ok(())
}
