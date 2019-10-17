mod docs_words;
mod documents_fields;
mod documents_fields_counts;
mod main;
mod postings_lists;
mod synonyms;
mod updates;
mod updates_results;

pub use self::docs_words::DocsWords;
pub use self::documents_fields::{DocumentsFields, DocumentFieldsIter};
pub use self::documents_fields_counts::{DocumentsFieldsCounts, DocumentFieldsCountsIter, DocumentsIdsIter};
pub use self::main::Main;
pub use self::postings_lists::PostingsLists;
pub use self::synonyms::Synonyms;
pub use self::updates::Updates;
pub use self::updates_results::UpdatesResults;

use std::collections::HashSet;

use meilidb_schema::{Schema, SchemaAttr};
use serde::de;
use zerocopy::{AsBytes, FromBytes};
use zlmdb::Result as ZResult;

use crate::criterion::Criteria;
use crate::serde::Deserializer;
use crate::{update, query_builder::QueryBuilder, DocumentId, MResult, Error};

type BEU64 = zerocopy::U64<byteorder::BigEndian>;
type BEU16 = zerocopy::U16<byteorder::BigEndian>;

#[derive(Debug, Copy, Clone)]
#[derive(AsBytes, FromBytes)]
#[repr(C)]
pub struct DocumentAttrKey { docid: BEU64, attr: BEU16 }

impl DocumentAttrKey {
    fn new(docid: DocumentId, attr: SchemaAttr) -> DocumentAttrKey {
        DocumentAttrKey { docid: BEU64::new(docid.0), attr: BEU16::new(attr.0) }
    }
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

fn documents_fields_counts_name(name: &str) -> String {
    format!("store-{}-documents-fields-counts", name)
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
    pub documents_fields_counts: DocumentsFieldsCounts,
    pub synonyms: Synonyms,
    pub docs_words: DocsWords,

    pub updates: Updates,
    pub updates_results: UpdatesResults,
    updates_notifier: crossbeam_channel::Sender<()>,
}

impl Index {
    pub fn document<T: de::DeserializeOwned>(
        &self,
        reader: &zlmdb::RoTxn,
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

    pub fn document_attribute<T: de::DeserializeOwned>(
        &self,
        reader: &zlmdb::RoTxn,
        document_id: DocumentId,
        attribute: SchemaAttr,
    ) -> MResult<Option<T>>
    {
        let bytes = self.documents_fields.document_attribute(reader, document_id, attribute)?;
        match bytes {
            Some(bytes) => Ok(Some(serde_json::from_slice(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn schema_update(&self, writer: &mut zlmdb::RwTxn, schema: Schema) -> MResult<u64> {
        let _ = self.updates_notifier.send(());
        update::push_schema_update(writer, self.updates, self.updates_results, schema)
    }

    pub fn customs_update(&self, writer: &mut zlmdb::RwTxn, customs: Vec<u8>) -> ZResult<u64> {
        let _ = self.updates_notifier.send(());
        update::push_customs_update(writer, self.updates, self.updates_results, customs)
    }

    pub fn documents_addition<D>(&self) -> update::DocumentsAddition<D> {
        update::DocumentsAddition::new(
            self.updates,
            self.updates_results,
            self.updates_notifier.clone(),
        )
    }

    pub fn documents_deletion(&self) -> update::DocumentsDeletion {
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

    pub fn current_update_id(&self, reader: &zlmdb::RoTxn) -> MResult<Option<u64>> {
        match self.updates.last_update_id(reader)? {
            Some((id, _)) => Ok(Some(id)),
            None => Ok(None),
        }
    }

    pub fn update_status(
        &self,
        reader: &zlmdb::RoTxn,
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
        QueryBuilder::new(
            self.main,
            self.postings_lists,
            self.documents_fields_counts,
            self.synonyms,
        )
    }

    pub fn query_builder_with_criteria<'c, 'f, 'd>(
        &self,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, 'f, 'd>
    {
        QueryBuilder::with_criteria(
            self.main,
            self.postings_lists,
            self.documents_fields_counts,
            self.synonyms,
            criteria,
        )
    }
}

pub fn create(
    env: &zlmdb::Env,
    name: &str,
    updates_notifier: crossbeam_channel::Sender<()>,
) -> MResult<Index>
{
    // create all the store names
    let main_name = main_name(name);
    let postings_lists_name = postings_lists_name(name);
    let documents_fields_name = documents_fields_name(name);
    let documents_fields_counts_name = documents_fields_counts_name(name);
    let synonyms_name = synonyms_name(name);
    let docs_words_name = docs_words_name(name);
    let updates_name = updates_name(name);
    let updates_results_name = updates_results_name(name);

    // open all the stores
    let main = env.create_dyn_database(Some(&main_name))?;
    let postings_lists = env.create_database(Some(&postings_lists_name))?;
    let documents_fields = env.create_database(Some(&documents_fields_name))?;
    let documents_fields_counts = env.create_database(Some(&documents_fields_counts_name))?;
    let synonyms = env.create_database(Some(&synonyms_name))?;
    let docs_words = env.create_database(Some(&docs_words_name))?;
    let updates = env.create_database(Some(&updates_name))?;
    let updates_results = env.create_database(Some(&updates_results_name))?;

    Ok(Index {
        main: Main { main },
        postings_lists: PostingsLists { postings_lists },
        documents_fields: DocumentsFields { documents_fields },
        documents_fields_counts: DocumentsFieldsCounts { documents_fields_counts },
        synonyms: Synonyms { synonyms },
        docs_words: DocsWords { docs_words },
        updates: Updates { updates },
        updates_results: UpdatesResults { updates_results },
        updates_notifier,
    })
}

pub fn open(
    env: &zlmdb::Env,
    name: &str,
    updates_notifier: crossbeam_channel::Sender<()>,
) -> MResult<Option<Index>>
{
    // create all the store names
    let main_name = main_name(name);
    let postings_lists_name = postings_lists_name(name);
    let documents_fields_name = documents_fields_name(name);
    let documents_fields_counts_name = documents_fields_counts_name(name);
    let synonyms_name = synonyms_name(name);
    let docs_words_name = docs_words_name(name);
    let updates_name = updates_name(name);
    let updates_results_name = updates_results_name(name);

    // open all the stores
    let main = match env.open_dyn_database(Some(&main_name))? {
        Some(main) => main,
        None => return Ok(None),
    };
    let postings_lists = match env.open_database(Some(&postings_lists_name))? {
        Some(postings_lists) => postings_lists,
        None => return Ok(None),
    };
    let documents_fields = match env.open_database(Some(&documents_fields_name))? {
        Some(documents_fields) => documents_fields,
        None => return Ok(None),
    };
    let documents_fields_counts = match env.open_database(Some(&documents_fields_counts_name))? {
        Some(documents_fields_counts) => documents_fields_counts,
        None => return Ok(None),
    };
    let synonyms = match env.open_database(Some(&synonyms_name))? {
        Some(synonyms) => synonyms,
        None => return Ok(None),
    };
    let docs_words = match env.open_database(Some(&docs_words_name))? {
        Some(docs_words) => docs_words,
        None => return Ok(None),
    };
    let updates = match env.open_database(Some(&updates_name))? {
        Some(updates) => updates,
        None => return Ok(None),
    };
    let updates_results = match env.open_database(Some(&updates_results_name))? {
        Some(updates_results) => updates_results,
        None => return Ok(None),
    };

    Ok(Some(Index {
        main: Main { main },
        postings_lists: PostingsLists { postings_lists },
        documents_fields: DocumentsFields { documents_fields },
        documents_fields_counts: DocumentsFieldsCounts { documents_fields_counts },
        synonyms: Synonyms { synonyms },
        docs_words: DocsWords { docs_words },
        updates: Updates { updates },
        updates_results: UpdatesResults { updates_results },
        updates_notifier,
    }))
}
