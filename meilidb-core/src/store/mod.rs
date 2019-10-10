mod docs_words;
mod documents_fields;
mod main;
mod postings_lists;
mod synonyms;
mod updates;
mod updates_results;

pub use self::docs_words::DocsWords;
pub use self::documents_fields::{DocumentsFields, DocumentFieldsIter};
pub use self::main::Main;
pub use self::postings_lists::PostingsLists;
pub use self::synonyms::Synonyms;
pub use self::updates::Updates;
pub use self::updates_results::UpdatesResults;

use std::collections::HashSet;
use meilidb_schema::{Schema, SchemaAttr};
use serde::de;
use crate::{update, query_builder::QueryBuilder, DocumentId, MResult, Error};
use crate::serde::Deserializer;

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

fn main_name(name: &str) -> String {
    format!("store-{}", name)
}

fn postings_lists_name(name: &str) -> String {
    format!("store-{}-postings-lists", name)
}

fn documents_fields_name(name: &str) -> String {
    format!("store-{}-documents-fields", name)
}

fn synonyms_name(name: &str) -> String {
    format!("store-{}-synonyms", name)
}

fn docs_words_name(name: &str) -> String {
    format!("store-{}-docs-words", name)
}

fn updates_name(name: &str) -> String {
    format!("store-{}-updates", name)
}

fn updates_results_name(name: &str) -> String {
    format!("store-{}-updates-results", name)
}

#[derive(Clone)]
pub struct Index {
    pub main: Main,
    pub postings_lists: PostingsLists,
    pub documents_fields: DocumentsFields,
    pub synonyms: Synonyms,
    pub docs_words: DocsWords,

    pub updates: Updates,
    pub updates_results: UpdatesResults,
    updates_notifier: crossbeam_channel::Sender<()>,
}

impl Index {
    pub fn document<R: rkv::Readable, T: de::DeserializeOwned>(
        &self,
        reader: &R,
        attributes: Option<&HashSet<&str>>,
        document_id: DocumentId,
    ) -> MResult<Option<T>>
    {
        let schema = self.main.schema(reader)?;
        let schema = schema.ok_or(Error::SchemaMissing)?;

        let attributes = match attributes {
            Some(attributes) => attributes.into_iter().map(|name| schema.attribute(name)).collect(),
            None => None,
        };

        let mut deserializer = Deserializer {
            document_id,
            reader,
            documents_fields: self.documents_fields,
            schema: &schema,
            attributes: attributes.as_ref(),
        };

        // TODO: currently we return an error if all document fields are missing,
        //       returning None would have been better
        Ok(T::deserialize(&mut deserializer).map(Some)?)
    }

    pub fn document_attribute<T: de::DeserializeOwned, R: rkv::Readable>(
        &self,
        reader: &R,
        document_id: DocumentId,
        attribute: SchemaAttr,
    ) -> MResult<Option<T>>
    {
        let bytes = self.documents_fields.document_attribute(reader, document_id, attribute)?;
        match bytes {
            Some(bytes) => Ok(Some(rmp_serde::from_read_ref(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn schema_update(&self, mut writer: rkv::Writer, schema: Schema) -> MResult<()> {
        update::push_schema_update(&mut writer, self.updates, self.updates_results, schema)?;
        writer.commit()?;
        let _ = self.updates_notifier.send(());
        Ok(())
    }

    pub fn documents_addition<D>(&self) -> update::DocumentsAddition<D> {
        update::DocumentsAddition::new(
            self.updates,
            self.updates_results,
            self.updates_notifier.clone(),
        )
    }

    pub fn documents_deletion<D>(&self) -> update::DocumentsDeletion {
        update::DocumentsDeletion::new(
            self.updates,
            self.updates_results,
            self.updates_notifier.clone(),
        )
    }

    pub fn synonyms_addition(&self) -> update::SynonymsAddition {
        update::SynonymsAddition::new(
            self.updates,
            self.updates_results,
            self.updates_notifier.clone(),
        )
    }

    pub fn synonyms_deletion(&self) -> update::SynonymsDeletion {
        update::SynonymsDeletion::new(
            self.updates,
            self.updates_results,
            self.updates_notifier.clone(),
        )
    }

    pub fn update_status<T: rkv::Readable>(
        &self,
        reader: &T,
        update_id: u64,
    ) -> MResult<update::UpdateStatus>
    {
        update::update_status(
            reader,
            self.updates,
            self.updates_results,
            update_id,
        )
    }

    pub fn query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self.main, self.postings_lists, self.synonyms)
    }
}

pub fn create(
    env: &rkv::Rkv,
    name: &str,
    updates_notifier: crossbeam_channel::Sender<()>,
) -> Result<Index, rkv::StoreError>
{
    open_options(env, name, rkv::StoreOptions::create(), updates_notifier)
}

pub fn open(
    env: &rkv::Rkv,
    name: &str,
    updates_notifier: crossbeam_channel::Sender<()>,
) -> Result<Index, rkv::StoreError>
{
    let mut options = rkv::StoreOptions::default();
    options.create = false;
    open_options(env, name, options, updates_notifier)
}

fn open_options(
    env: &rkv::Rkv,
    name: &str,
    options: rkv::StoreOptions,
    updates_notifier: crossbeam_channel::Sender<()>,
) -> Result<Index, rkv::StoreError>
{
    // create all the store names
    let main_name = main_name(name);
    let postings_lists_name = postings_lists_name(name);
    let documents_fields_name = documents_fields_name(name);
    let synonyms_name = synonyms_name(name);
    let docs_words_name = docs_words_name(name);
    let updates_name = updates_name(name);
    let updates_results_name = updates_results_name(name);

    // open all the stores
    let main = env.open_single(main_name.as_str(), options)?;
    let postings_lists = env.open_single(postings_lists_name.as_str(), options)?;
    let documents_fields = env.open_single(documents_fields_name.as_str(), options)?;
    let synonyms = env.open_single(synonyms_name.as_str(), options)?;
    let docs_words = env.open_single(docs_words_name.as_str(), options)?;
    let updates = env.open_single(updates_name.as_str(), options)?;
    let updates_results = env.open_single(updates_results_name.as_str(), options)?;

    Ok(Index {
        main: Main { main },
        postings_lists: PostingsLists { postings_lists },
        documents_fields: DocumentsFields { documents_fields },
        synonyms: Synonyms { synonyms },
        docs_words: DocsWords { docs_words },
        updates: Updates { updates },
        updates_results: UpdatesResults { updates_results },
        updates_notifier,
    })
}
