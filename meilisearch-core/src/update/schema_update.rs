use meilisearch_schema::{Diff, Schema};

use crate::database::{MainT, UpdateT};
use crate::update::documents_addition::reindex_all_documents;
use crate::update::{next_update_id, Update};
use crate::{error::UnsupportedOperation, store, MResult};

pub fn apply_schema_update(
    writer: &mut heed::RwTxn<MainT>,
    new_schema: &Schema,
    index: &store::Index,
) -> MResult<()> {
    use UnsupportedOperation::{
        CanOnlyIntroduceNewSchemaAttributesAtEnd, CannotRemoveSchemaAttribute,
        CannotReorderSchemaAttribute, CannotUpdateSchemaIdentifier,
    };

    let mut need_full_reindexing = false;

    if let Some(old_schema) = index.main.schema(writer)? {
        for diff in meilisearch_schema::diff(&old_schema, new_schema) {
            match diff {
                Diff::IdentChange { .. } => return Err(CannotUpdateSchemaIdentifier.into()),
                Diff::AttrMove { .. } => return Err(CannotReorderSchemaAttribute.into()),
                Diff::AttrPropsChange { old, new, .. } => {
                    if new.indexed != old.indexed {
                        need_full_reindexing = true;
                    }
                    if new.ranked != old.ranked {
                        need_full_reindexing = true;
                    }
                }
                Diff::NewAttr { pos, .. } => {
                    // new attribute not at the end of the schema
                    if pos < old_schema.number_of_attributes() {
                        return Err(CanOnlyIntroduceNewSchemaAttributesAtEnd.into());
                    }
                }
                Diff::RemovedAttr { .. } => return Err(CannotRemoveSchemaAttribute.into()),
            }
        }
    }

    index.main.put_schema(writer, new_schema)?;

    if need_full_reindexing {
        reindex_all_documents(writer, index)?
    }

    Ok(())
}

pub fn push_schema_update(
    writer: &mut heed::RwTxn<UpdateT>,
    index: &store::Index,
    schema: Schema,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, index.updates, index.updates_results)?;

    let update = Update::schema(schema);
    index.updates.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}
