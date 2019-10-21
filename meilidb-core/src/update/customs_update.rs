use crate::store;
use crate::update::{next_update_id, Update};
use heed::Result as ZResult;

pub fn apply_customs_update(
    writer: &mut heed::RwTxn,
    main_store: store::Main,
    customs: &[u8],
) -> ZResult<()> {
    main_store.put_customs(writer, customs)
}

pub fn push_customs_update(
    writer: &mut heed::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    customs: Vec<u8>,
) -> ZResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::Customs(customs);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}
