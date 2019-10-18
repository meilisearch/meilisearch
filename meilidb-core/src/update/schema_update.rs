use crate::update::{next_update_id, Update};
use crate::{error::UnsupportedOperation, store, MResult};
use meilidb_schema::Schema;

pub fn apply_schema_update(
    writer: &mut zlmdb::RwTxn,
    main_store: store::Main,
    new_schema: &Schema,
) -> MResult<()> {
    if main_store.schema(writer)?.is_some() {
        return Err(UnsupportedOperation::SchemaAlreadyExists.into());
    }

    main_store
        .put_schema(writer, new_schema)
        .map_err(Into::into)
}

pub fn push_schema_update(
    writer: &mut zlmdb::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    schema: Schema,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::Schema(schema);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}
