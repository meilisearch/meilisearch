use meilisearch_schema::{Diff, Schema};

use crate::database::{MainT, UpdateT};
use crate::update::documents_addition::reindex_all_documents;
use crate::update::{next_update_id, Update};
use crate::{error::UnsupportedOperation, store, MResult};

pub fn apply_schema_update(
    writer: &mut heed::RwTxn<MainT>,
    new_schema: &Schema,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    prefix_documents_cache_store: store::PrefixDocumentsCache,
) -> MResult<()> {
    use UnsupportedOperation::{
        CanOnlyIntroduceNewSchemaAttributesAtEnd, CannotRemoveSchemaAttribute,
        CannotReorderSchemaAttribute, CannotUpdateSchemaIdentifier,
    };

    let mut need_full_reindexing = false;

    if let Some(old_schema) = main_store.schema(writer)? {
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

    main_store.put_schema(writer, new_schema)?;

    if need_full_reindexing {
        reindex_all_documents(
            writer,
            main_store,
            documents_fields_store,
            documents_fields_counts_store,
            postings_lists_store,
            docs_words_store,
            prefix_documents_cache_store,
        )?
    }

    Ok(())
}

pub fn push_schema_update(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    schema: Schema,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::schema(schema);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}
