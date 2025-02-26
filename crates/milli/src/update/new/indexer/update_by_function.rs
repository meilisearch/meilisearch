use bumpalo::collections::CollectIn;
use bumpalo::Bump;
use bumparaw_collections::RawMap;
use rhai::{Dynamic, Engine, OptimizationLevel, Scope, AST};
use roaring::RoaringBitmap;
use rustc_hash::FxBuildHasher;
use scoped_thread_pool::{PartitionChunks, ThreadPool};

use super::document_changes::DocumentChangeContext;
use super::DocumentChanges;
use crate::documents::Error::InvalidDocumentFormat;
use crate::documents::PrimaryKey;
use crate::error::{FieldIdMapMissingEntry, InternalError};
use crate::update::new::document::Versions;
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::{Deletion, DocumentChange, KvReaderFieldId, Update};
use crate::{all_obkv_to_json, Error, FieldsIdsMap, Object, Result, UserError};

pub struct UpdateByFunction {
    documents: RoaringBitmap,
    context: Option<Object>,
    code: String,
}

pub struct UpdateByFunctionChanges<'index> {
    primary_key: &'index PrimaryKey<'index>,
    engine: Engine,
    ast: AST,
    context: Option<Dynamic>,
    documents: PartitionChunks<'index, u32>,
}

impl UpdateByFunction {
    pub fn new(documents: RoaringBitmap, context: Option<Object>, code: String) -> Self {
        UpdateByFunction { documents, context, code }
    }

    pub fn into_changes<'index>(
        self,
        primary_key: &'index PrimaryKey,
        allocator: &'index Bump,
        thread_pool: &ThreadPool<crate::Error>,
        chunk_size: usize,
    ) -> Result<UpdateByFunctionChanges<'index>> {
        let Self { documents, context, code } = self;

        // Setup the security and limits of the Engine
        let mut engine = Engine::new();
        engine.set_optimization_level(OptimizationLevel::Full);
        engine.set_max_call_levels(1000);
        // It is an arbitrary value. We need to let users define this in the settings.
        engine.set_max_operations(1_000_000);
        engine.set_max_variables(1000);
        engine.set_max_functions(30);
        engine.set_max_expr_depths(100, 1000);
        engine.set_max_string_size(1024 * 1024 * 1024); // 1 GiB
        engine.set_max_array_size(10_000);
        engine.set_max_map_size(10_000);

        let ast = engine.compile(code).map_err(UserError::DocumentEditionCompilationError)?;
        let context = match context {
            Some(context) => {
                Some(serde_json::from_value(context.into()).map_err(InternalError::SerdeJson)?)
            }
            None => None,
        };

        let documents: bumpalo::collections::Vec<'_, _> =
            documents.into_iter().collect_in(allocator);
        let documents = documents.into_bump_slice();

        let documents = PartitionChunks::new(documents, chunk_size, thread_pool.thread_count());

        Ok(UpdateByFunctionChanges { primary_key, engine, ast, context, documents })
    }
}

impl<'index> DocumentChanges<'index> for UpdateByFunctionChanges<'index> {
    type Item = u32;

    fn item_to_document_change<'doc, T: MostlySend + 'doc>(
        &self,
        context: &'doc DocumentChangeContext<T>,
        docid: &'doc Self::Item,
    ) -> Result<Option<DocumentChange<'doc>>>
    where
        'index: 'doc,
    {
        let DocumentChangeContext {
            index,
            db_fields_ids_map,
            rtxn: txn,
            new_fields_ids_map,
            doc_alloc,
            ..
        } = context;

        let docid = *docid;

        // safety: Both documents *must* exists in the database as
        //         their IDs comes from the list of documents ids.
        let document = index.document(txn, docid)?;
        let rhai_document = obkv_to_rhaimap(document, db_fields_ids_map)?;
        let json_document = all_obkv_to_json(document, db_fields_ids_map)?;

        let document_id = self
            .primary_key
            .document_id(document, db_fields_ids_map)?
            .map_err(|_| InvalidDocumentFormat)?;

        let mut scope = Scope::new();
        if let Some(context) = self.context.as_ref().cloned() {
            scope.push_constant_dynamic("context", context.clone());
        }
        scope.push("doc", rhai_document);
        // We run the user script which edits "doc" scope variable reprensenting
        // the document and ignore the output and even the type of it, i.e., Dynamic.
        let _ = self
            .engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast)
            .map_err(UserError::DocumentEditionRuntimeError)?;

        match scope.remove::<Dynamic>("doc") {
            // If the "doc" variable has been set to (), we effectively delete the document.
            Some(doc) if doc.is_unit() => Ok(Some(DocumentChange::Deletion(Deletion::create(
                docid,
                doc_alloc.alloc_str(&document_id),
            )))),
            None => unreachable!("missing doc variable from the Rhai scope"),
            Some(new_document) => match new_document.try_cast() {
                Some(new_rhai_document) => {
                    let mut buffer = bumpalo::collections::Vec::new_in(doc_alloc);
                    serde_json::to_writer(&mut buffer, &new_rhai_document)
                        .map_err(InternalError::SerdeJson)?;
                    let raw_new_doc = serde_json::from_slice(buffer.into_bump_slice())
                        .map_err(InternalError::SerdeJson)?;

                    // Note: This condition is not perfect. Sometimes it detect changes
                    //       like with floating points numbers and consider updating
                    //       the document even if nothing actually changed.
                    //
                    // Future: Use a custom function rhai function to track changes.
                    //         <https://docs.rs/rhai/latest/rhai/struct.Engine.html#method.register_indexer_set>
                    if json_document != rhaimap_to_object(new_rhai_document) {
                        let mut global_fields_ids_map = new_fields_ids_map.borrow_mut_or_yield();
                        let new_document_id = self
                            .primary_key
                            .extract_fields_and_docid(
                                raw_new_doc,
                                &mut *global_fields_ids_map,
                                doc_alloc,
                            )?
                            .to_de();

                        if document_id != new_document_id {
                            Err(Error::UserError(UserError::DocumentEditionCannotModifyPrimaryKey))
                        } else {
                            let raw_new_doc = RawMap::from_raw_value_and_hasher(
                                raw_new_doc,
                                FxBuildHasher,
                                doc_alloc,
                            )
                            .map_err(InternalError::SerdeJson)?;

                            Ok(Some(DocumentChange::Update(Update::create(
                                docid,
                                new_document_id,
                                Versions::single(raw_new_doc),
                                true, // It is like document replacement
                            ))))
                        }
                    } else {
                        Ok(None)
                    }
                }
                None => Err(Error::UserError(UserError::DocumentEditionDocumentMustBeObject)),
            },
        }
    }

    fn len(&self) -> usize {
        self.documents.slice().len()
    }

    fn items(&self, thread_index: usize, task_index: usize) -> Option<&[Self::Item]> {
        self.documents.partition(thread_index, task_index)
    }
}

fn obkv_to_rhaimap(obkv: &KvReaderFieldId, fields_ids_map: &FieldsIdsMap) -> Result<rhai::Map> {
    let all_keys = obkv.iter().map(|(k, _v)| k).collect::<Vec<_>>();
    let map: Result<rhai::Map> = all_keys
        .iter()
        .copied()
        .flat_map(|id| obkv.get(id).map(|value| (id, value)))
        .map(|(id, value)| {
            let name = fields_ids_map.name(id).ok_or(FieldIdMapMissingEntry::FieldId {
                field_id: id,
                process: "all_obkv_to_rhaimap",
            })?;
            let value = serde_json::from_slice(value).map_err(InternalError::SerdeJson)?;
            Ok((name.into(), value))
        })
        .collect();

    map
}

fn rhaimap_to_object(map: rhai::Map) -> Object {
    let mut output = Object::new();
    for (key, value) in map {
        let value = serde_json::to_value(&value).unwrap();
        output.insert(key.into(), value);
    }
    output
}
