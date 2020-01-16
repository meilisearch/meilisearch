use crate::database::{MainT, UpdateT};
use crate::update::{next_update_id, Update};
use crate::{store, MResult, RankedMap};

pub fn apply_clear_all(
    writer: &mut heed::RwTxn<MainT>,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    prefix_documents_cache: store::PrefixDocumentsCache,
    prefix_postings_lists_cache: store::PrefixPostingsListsCache,
) -> MResult<()> {
    main_store.put_words_fst(writer, &fst::Set::default())?;
    main_store.put_ranked_map(writer, &RankedMap::default())?;
    main_store.put_number_of_documents(writer, |_| 0)?;
    documents_fields_store.clear(writer)?;
    documents_fields_counts_store.clear(writer)?;
    postings_lists_store.clear(writer)?;
    docs_words_store.clear(writer)?;
    prefix_documents_cache.clear(writer)?;
    prefix_postings_lists_cache.clear(writer)?;

    Ok(())
}

pub fn push_clear_all(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;
    let update = Update::clear_all();
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}
