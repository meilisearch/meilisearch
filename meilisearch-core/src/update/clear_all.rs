use crate::database::{MainT, UpdateT};
use crate::update::{next_update_id, Update};
use crate::{store, MResult, RankedMap};

pub fn apply_clear_all(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
) -> MResult<()> {
    index.main.put_words_fst(writer, &fst::Set::default())?;
    index.main.put_external_docids(writer, &fst::Map::default())?;
    index.main.put_internal_docids(writer, &sdset::SetBuf::default())?;
    index.main.put_ranked_map(writer, &RankedMap::default())?;
    index.main.put_number_of_documents(writer, |_| 0)?;
    index.main.put_sorted_document_ids_cache(writer, &[])?;
    index.documents_fields.clear(writer)?;
    index.documents_fields_counts.clear(writer)?;
    index.postings_lists.clear(writer)?;
    index.docs_words.clear(writer)?;
    index.prefix_documents_cache.clear(writer)?;
    index.prefix_postings_lists_cache.clear(writer)?;
    index.facets.clear(writer)?;

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
