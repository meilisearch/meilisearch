use std::borrow::Cow;
use std::collections::{HashMap, BTreeMap};

use fst::{set::OpBuilder, SetBuilder};
use indexmap::IndexMap;
use meilisearch_schema::{Schema, FieldId};
use meilisearch_types::DocumentId;
use sdset::{duo::Union, SetOperation};
use serde::Deserialize;
use serde_json::Value;

use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::facets;
use crate::raw_indexer::RawIndexer;
use crate::serde::Deserializer;
use crate::store::{self, DocumentsFields, DocumentsFieldsCounts, DiscoverIds};
use crate::update::helpers::{index_value, value_to_number, extract_document_id};
use crate::update::{apply_documents_deletion, compute_short_prefixes, next_update_id, Update};
use crate::{Error, MResult, RankedMap};

pub struct DocumentsAddition<D> {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    // Whether the user explicitly set the primary key in the update
    primary_key: Option<String>,
    documents: Vec<D>,
    is_partial: bool,
}

impl<D> DocumentsAddition<D> {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> DocumentsAddition<D> {
        DocumentsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            documents: Vec::new(),
            is_partial: false,
            primary_key: None,
        }
    }

    pub fn new_partial(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> DocumentsAddition<D> {
        DocumentsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            documents: Vec::new(),
            is_partial: true,
            primary_key: None,
        }
    }

    pub fn set_primary_key(&mut self, primary_key: String) {
        self.primary_key = Some(primary_key);
    }

    pub fn update_document(&mut self, document: D) {
        self.documents.push(document);
    }

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64>
    where
        D: serde::Serialize,
    {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_documents_addition(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.documents,
            self.is_partial,
            self.primary_key,
        )?;
        Ok(update_id)
    }
}

impl<D> Extend<D> for DocumentsAddition<D> {
    fn extend<T: IntoIterator<Item = D>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}

pub fn push_documents_addition<D: serde::Serialize>(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: Vec<D>,
    is_partial: bool,
    primary_key: Option<String>,
) -> MResult<u64> {
    let mut values = Vec::with_capacity(addition.len());
    for add in addition {
        let vec = serde_json::to_vec(&add)?;
        let add = serde_json::from_slice(&vec)?;
        values.push(add);
    }

    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = if is_partial {
        Update::documents_partial(primary_key, values)
    } else {
        Update::documents_addition(primary_key, values)
    };

    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

#[allow(clippy::too_many_arguments)]
fn index_document<A: AsRef<[u8]>>(
    writer: &mut heed::RwTxn<MainT>,
    documents_fields: DocumentsFields,
    documents_fields_counts: DocumentsFieldsCounts,
    ranked_map: &mut RankedMap,
    indexer: &mut RawIndexer<A>,
    schema: &Schema,
    field_id: FieldId,
    document_id: DocumentId,
    value: &Value,
) -> MResult<()>
{
    let serialized = serde_json::to_vec(value)?;
    documents_fields.put_document_field(writer, document_id, field_id, &serialized)?;

    if let Some(indexed_pos) = schema.is_searchable(field_id) {
        let number_of_words = index_value(indexer, document_id, indexed_pos, value);
        if let Some(number_of_words) = number_of_words {
            documents_fields_counts.put_document_field_count(
                writer,
                document_id,
                indexed_pos,
                number_of_words as u16,
            )?;
        }
    }

    if schema.is_ranked(field_id) {
        let number = value_to_number(value).unwrap_or_default();
        ranked_map.insert(document_id, field_id, number);
    }

    Ok(())
}

pub fn apply_addition(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    new_documents: Vec<IndexMap<String, Value>>,
    partial: bool,
    primary_key: Option<String>,
) -> MResult<()>
{
    let mut schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    // Retrieve the documents ids related structures
    let external_docids = index.main.external_docids(writer)?;
    let internal_docids = index.main.internal_docids(writer)?;
    let mut available_ids = DiscoverIds::new(&internal_docids);

    let primary_key = match schema.primary_key() {
        Some(primary_key) => primary_key.to_string(),
        None => {
            let name = primary_key.ok_or(Error::MissingPrimaryKey)?;
            schema.set_primary_key(&name)?;
            name
        }
    };

    // 1. store documents ids for future deletion
    let mut documents_additions = HashMap::new();
    let mut new_external_docids = BTreeMap::new();
    let mut new_internal_docids = Vec::with_capacity(new_documents.len());

    for mut document in new_documents {
        let external_docids_get = |docid: &str| {
            match (external_docids.get(docid), new_external_docids.get(docid)) {
                (_, Some(&id))
                | (Some(id), _) => Some(id as u32),
                (None, None) => None,
            }
        };

        let (internal_docid, external_docid) =
            extract_document_id(
                &primary_key,
                &document,
                &external_docids_get,
                &mut available_ids,
            )?;

        new_external_docids.insert(external_docid, internal_docid.0 as u64);
        new_internal_docids.push(internal_docid);

        if partial {
            let mut deserializer = Deserializer {
                document_id: internal_docid,
                reader: writer,
                documents_fields: index.documents_fields,
                schema: &schema,
                fields: None,
            };

            let old_document = Option::<HashMap<String, Value>>::deserialize(&mut deserializer)?;
            if let Some(old_document) = old_document {
                for (key, value) in old_document {
                    document.entry(key).or_insert(value);
                }
            }
        }
        documents_additions.insert(internal_docid, document);
    }

    // 2. remove the documents postings lists
    let number_of_inserted_documents = documents_additions.len();
    let documents_ids = new_external_docids.iter().map(|(id, _)| id.clone()).collect();
    apply_documents_deletion(writer, index, documents_ids)?;

    let mut ranked_map = match index.main.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    let stop_words = index.main.stop_words_fst(writer)?.map_data(Cow::into_owned)?;


    let mut indexer = RawIndexer::new(&stop_words);

    // For each document in this update
    for (document_id, document) in &documents_additions {
        // For each key-value pair in the document.
        for (attribute, value) in document {
            let (field_id, _) = schema.insert_with_position(&attribute)?;
            index_document(
                writer,
                index.documents_fields,
                index.documents_fields_counts,
                &mut ranked_map,
                &mut indexer,
                &schema,
                field_id,
                *document_id,
                &value,
            )?;
        }
    }

    write_documents_addition_index(
        writer,
        index,
        &ranked_map,
        number_of_inserted_documents,
        indexer,
    )?;

    index.main.put_schema(writer, &schema)?;

    let new_external_docids = fst::Map::from_iter(new_external_docids.iter().map(|(ext, id)| (ext, *id as u64)))?;
    let new_internal_docids = sdset::SetBuf::from_dirty(new_internal_docids);
    index.main.merge_external_docids(writer, &new_external_docids)?;
    index.main.merge_internal_docids(writer, &new_internal_docids)?;

    // recompute all facet attributes after document update.
    if let Some(attributes_for_facetting) = index.main.attributes_for_faceting(writer)? {
        let docids = index.main.internal_docids(writer)?;
        let facet_map = facets::facet_map_from_docids(writer, index, &docids, attributes_for_facetting.as_ref())?;
        index.facets.add(writer, facet_map)?;
    }

    // update is finished; update sorted document id cache with new state
    let mut document_ids = index.main.internal_docids(writer)?.to_vec();
    super::cache_document_ids_sorted(writer, &ranked_map, index, &mut document_ids)?;

    Ok(())
}

pub fn apply_documents_partial_addition(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    new_documents: Vec<IndexMap<String, Value>>,
    primary_key: Option<String>,
) -> MResult<()> {
    apply_addition(writer, index, new_documents, true, primary_key)
}

pub fn apply_documents_addition(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    new_documents: Vec<IndexMap<String, Value>>,
    primary_key: Option<String>,
) -> MResult<()> {
    apply_addition(writer, index, new_documents, false, primary_key)
}

pub fn reindex_all_documents(writer: &mut heed::RwTxn<MainT>, index: &store::Index) -> MResult<()> {
    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let mut ranked_map = RankedMap::default();

    // 1. retrieve all documents ids
    let mut documents_ids_to_reindex = Vec::new();
    for result in index.documents_fields_counts.documents_ids(writer)? {
        let document_id = result?;
        documents_ids_to_reindex.push(document_id);
    }

    // 2. remove the documents posting lists
    index.main.put_words_fst(writer, &fst::Set::default())?;
    index.main.put_ranked_map(writer, &ranked_map)?;
    index.main.put_number_of_documents(writer, |_| 0)?;
    index.facets.clear(writer)?;
    index.postings_lists.clear(writer)?;
    index.docs_words.clear(writer)?;

    let stop_words = index.main
        .stop_words_fst(writer)?
        .map_data(Cow::into_owned)
        .unwrap();

    let number_of_inserted_documents = documents_ids_to_reindex.len();
    let mut indexer = RawIndexer::new(&stop_words);
    let mut ram_store = HashMap::new();

    if let Some(ref attributes_for_facetting) = index.main.attributes_for_faceting(writer)? {
        let facet_map = facets::facet_map_from_docids(writer, &index, &documents_ids_to_reindex, &attributes_for_facetting)?;
        index.facets.add(writer, facet_map)?;
    }
    // ^-- https://github.com/meilisearch/MeiliSearch/pull/631#issuecomment-626624470 --v
    for document_id in &documents_ids_to_reindex {
        for result in index.documents_fields.document_fields(writer, *document_id)? {
            let (field_id, bytes) = result?;
            let value: Value = serde_json::from_slice(bytes)?;
            ram_store.insert((document_id, field_id), value);
        }

        // For each key-value pair in the document.
        for ((document_id, field_id), value) in ram_store.drain() {
            index_document(
                writer,
                index.documents_fields,
                index.documents_fields_counts,
                &mut ranked_map,
                &mut indexer,
                &schema,
                field_id,
                *document_id,
                &value,
            )?;
        }
    }

    // 4. write the new index in the main store
    write_documents_addition_index(
        writer,
        index,
        &ranked_map,
        number_of_inserted_documents,
        indexer,
    )?;

    index.main.put_schema(writer, &schema)?;

    // recompute all facet attributes after document update.
    if let Some(attributes_for_facetting) = index.main.attributes_for_faceting(writer)? {
        let docids = index.main.internal_docids(writer)?;
        let facet_map = facets::facet_map_from_docids(writer, index, &docids, attributes_for_facetting.as_ref())?;
        index.facets.add(writer, facet_map)?;
    }

    // update is finished; update sorted document id cache with new state
    let mut document_ids = index.main.internal_docids(writer)?.to_vec();
    super::cache_document_ids_sorted(writer, &ranked_map, index, &mut document_ids)?;

    Ok(())
}

pub fn write_documents_addition_index<A: AsRef<[u8]>>(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    ranked_map: &RankedMap,
    number_of_inserted_documents: usize,
    indexer: RawIndexer<A>,
) -> MResult<()>
{
    let indexed = indexer.build();
    let mut delta_words_builder = SetBuilder::memory();

    for (word, delta_set) in indexed.words_doc_indexes {
        delta_words_builder.insert(&word).unwrap();

        let set = match index.postings_lists.postings_list(writer, &word)? {
            Some(postings) => Union::new(&postings.matches, &delta_set).into_set_buf(),
            None => delta_set,
        };

        index.postings_lists.put_postings_list(writer, &word, &set)?;
    }

    for (id, words) in indexed.docs_words {
        index.docs_words.put_doc_words(writer, id, &words)?;
    }

    let delta_words = delta_words_builder.into_set();

    let words_fst = index.main.words_fst(writer)?;
    let words = if !words_fst.is_empty() {
        let op = OpBuilder::new()
            .add(words_fst.stream())
            .add(delta_words.stream())
            .r#union();

        let mut words_builder = SetBuilder::memory();
        words_builder.extend_stream(op).unwrap();
        words_builder.into_set()
    } else {
        delta_words
    };

    index.main.put_words_fst(writer, &words)?;
    index.main.put_ranked_map(writer, ranked_map)?;
    index.main.put_number_of_documents(writer, |old| old + number_of_inserted_documents as u64)?;

    compute_short_prefixes(writer, &words, index)?;

    Ok(())
}
