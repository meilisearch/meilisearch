mod docs_words;
mod prefix_documents_cache;
mod prefix_postings_lists_cache;
mod documents_fields;
mod documents_fields_counts;
mod main;
mod postings_lists;
mod synonyms;
mod updates;
mod updates_results;

pub use self::docs_words::DocsWords;
pub use self::prefix_documents_cache::PrefixDocumentsCache;
pub use self::prefix_postings_lists_cache::PrefixPostingsListsCache;
pub use self::documents_fields::{DocumentFieldsIter, DocumentsFields};
pub use self::documents_fields_counts::{
    DocumentFieldsCountsIter, DocumentsFieldsCounts, DocumentsIdsIter,
};
pub use self::main::Main;
pub use self::postings_lists::PostingsLists;
pub use self::synonyms::Synonyms;
pub use self::updates::Updates;
pub use self::updates_results::UpdatesResults;

use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::TryInto;
use std::{mem, ptr};

use heed::Result as ZResult;
use heed::{BytesEncode, BytesDecode};
use meilisearch_schema::{IndexedPos, FieldId};
use sdset::{Set, SetBuf};
use serde::de::{self, Deserialize};
use zerocopy::{AsBytes, FromBytes};

use crate::criterion::Criteria;
use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::serde::Deserializer;
use crate::settings::SettingsUpdate;
use crate::{query_builder::QueryBuilder, update, DocIndex, DocumentId, Error, MResult};

type BEU64 = zerocopy::U64<byteorder::BigEndian>;
type BEU16 = zerocopy::U16<byteorder::BigEndian>;

#[derive(Debug, Copy, Clone, AsBytes, FromBytes)]
#[repr(C)]
pub struct DocumentFieldIndexedKey {
    docid: BEU64,
    indexed_pos: BEU16,
}

impl DocumentFieldIndexedKey {
    fn new(docid: DocumentId, indexed_pos: IndexedPos) -> DocumentFieldIndexedKey {
        DocumentFieldIndexedKey {
            docid: BEU64::new(docid.0),
            indexed_pos: BEU16::new(indexed_pos.0),
        }
    }
}

#[derive(Debug, Copy, Clone, AsBytes, FromBytes)]
#[repr(C)]
pub struct DocumentFieldStoredKey {
    docid: BEU64,
    field_id: BEU16,
}

impl DocumentFieldStoredKey {
    fn new(docid: DocumentId, field_id: FieldId) -> DocumentFieldStoredKey {
        DocumentFieldStoredKey {
            docid: BEU64::new(docid.0),
            field_id: BEU16::new(field_id.0),
        }
    }
}

#[derive(Default, Debug)]
pub struct Postings<'a> {
    pub docids: Cow<'a, Set<DocumentId>>,
    pub matches: Cow<'a, Set<DocIndex>>,
}

pub struct PostingsCodec;

impl<'a> BytesEncode<'a> for PostingsCodec {
    type EItem = Postings<'a>;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let u64_size = mem::size_of::<u64>();
        let docids_size = item.docids.len() * mem::size_of::<DocumentId>();
        let matches_size = item.matches.len() * mem::size_of::<DocIndex>();

        let mut buffer = Vec::with_capacity(u64_size + docids_size + matches_size);

        let docids_len = item.docids.len();
        buffer.extend_from_slice(&docids_len.to_be_bytes());
        buffer.extend_from_slice(item.docids.as_bytes());
        buffer.extend_from_slice(item.matches.as_bytes());

        Some(Cow::Owned(buffer))
    }
}

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

fn from_bytes_to_set<'a, T: 'a>(bytes: &'a [u8]) -> Option<Cow<'a, Set<T>>>
where T: Clone + FromBytes
{
    match zerocopy::LayoutVerified::<_, [T]>::new_slice(bytes) {
        Some(layout) => Some(Cow::Borrowed(Set::new_unchecked(layout.into_slice()))),
        None => {
            let len = bytes.len();
            let elem_size = mem::size_of::<T>();

            // ensure that it is the alignment that is wrong
            // and the length is valid
            if len % elem_size == 0 && !aligned_to(bytes, mem::align_of::<T>()) {
                let elems = len / elem_size;
                let mut vec = Vec::<T>::with_capacity(elems);

                unsafe {
                    let dst = vec.as_mut_ptr() as *mut u8;
                    ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                    vec.set_len(elems);
                }

                return Some(Cow::Owned(SetBuf::new_unchecked(vec)));
            }

            None
        }
    }
}

impl<'a> BytesDecode<'a> for PostingsCodec {
    type DItem = Postings<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let u64_size = mem::size_of::<u64>();
        let docid_size = mem::size_of::<DocumentId>();

        let (len_bytes, bytes) = bytes.split_at(u64_size);
        let docids_len = len_bytes.try_into().ok().map(u64::from_be_bytes)? as usize;
        let docids_size = docids_len * docid_size;

        let docids_bytes = &bytes[..docids_size];
        let matches_bytes = &bytes[docids_size..];

        let docids = from_bytes_to_set(docids_bytes)?;
        let matches = from_bytes_to_set(matches_bytes)?;

        Some(Postings { docids, matches })
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

fn prefix_documents_cache_name(name: &str) -> String {
    format!("store-{}-prefix-documents-cache", name)
}

fn prefix_postings_lists_cache_name(name: &str) -> String {
    format!("store-{}-prefix-postings-lists-cache", name)
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
    pub prefix_documents_cache: PrefixDocumentsCache,
    pub prefix_postings_lists_cache: PrefixPostingsListsCache,

    pub updates: Updates,
    pub updates_results: UpdatesResults,
    pub(crate) updates_notifier: UpdateEventsEmitter,
}

impl Index {
    pub fn document<T: de::DeserializeOwned, R: AsRef<str>>(
        &self,
        reader: &heed::RoTxn<MainT>,
        attributes: Option<HashSet<R>>,
        document_id: DocumentId,
    ) -> MResult<Option<T>> {
        let schema = self.main.schema(reader)?;
        let schema = schema.ok_or(Error::SchemaMissing)?;

        let attributes = match attributes {
            Some(attributes) => Some(attributes.iter().filter_map(|name| schema.id(name.as_ref())).collect()),
            None => None,
        };

        let mut deserializer = Deserializer {
            document_id,
            reader,
            documents_fields: self.documents_fields,
            schema: &schema,
            fields: attributes.as_ref(),
        };

        Ok(Option::<T>::deserialize(&mut deserializer)?)
    }

    pub fn document_attribute<T: de::DeserializeOwned>(
        &self,
        reader: &heed::RoTxn<MainT>,
        document_id: DocumentId,
        attribute: FieldId,
    ) -> MResult<Option<T>> {
        let bytes = self
            .documents_fields
            .document_attribute(reader, document_id, attribute)?;
        match bytes {
            Some(bytes) => Ok(Some(serde_json::from_slice(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn document_attribute_bytes<'txn>(
        &self,
        reader: &'txn heed::RoTxn<MainT>,
        document_id: DocumentId,
        attribute: FieldId,
    ) -> MResult<Option<&'txn [u8]>> {
        let bytes = self
            .documents_fields
            .document_attribute(reader, document_id, attribute)?;
        match bytes {
            Some(bytes) => Ok(Some(bytes)),
            None => Ok(None),
        }
    }

    pub fn customs_update(&self, writer: &mut heed::RwTxn<UpdateT>, customs: Vec<u8>) -> ZResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        update::push_customs_update(writer, self.updates, self.updates_results, customs)
    }

    pub fn settings_update(&self, writer: &mut heed::RwTxn<UpdateT>, update: SettingsUpdate) -> ZResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        update::push_settings_update(writer, self.updates, self.updates_results, update)
    }

    pub fn documents_addition<D>(&self) -> update::DocumentsAddition<D> {
        update::DocumentsAddition::new(
            self.updates,
            self.updates_results,
            self.updates_notifier.clone(),
        )
    }

    pub fn documents_partial_addition<D>(&self) -> update::DocumentsAddition<D> {
        update::DocumentsAddition::new_partial(
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

    pub fn clear_all(&self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        update::push_clear_all(writer, self.updates, self.updates_results)
    }

    pub fn current_update_id(&self, reader: &heed::RoTxn<UpdateT>) -> MResult<Option<u64>> {
        match self.updates.last_update(reader)? {
            Some((id, _)) => Ok(Some(id)),
            None => Ok(None),
        }
    }

    pub fn update_status(
        &self,
        reader: &heed::RoTxn<UpdateT>,
        update_id: u64,
    ) -> MResult<Option<update::UpdateStatus>> {
        update::update_status(reader, self.updates, self.updates_results, update_id)
    }

    pub fn all_updates_status(&self, reader: &heed::RoTxn<UpdateT>) -> MResult<Vec<update::UpdateStatus>> {
        let mut updates = Vec::new();
        let mut last_update_result_id = 0;

        // retrieve all updates results
        if let Some((last_id, _)) = self.updates_results.last_update(reader)? {
            updates.reserve(last_id as usize);

            for id in 0..=last_id {
                if let Some(update) = self.update_status(reader, id)? {
                    updates.push(update);
                    last_update_result_id = id + 1;
                }
            }
        }

        // retrieve all enqueued updates
        if let Some((last_id, _)) = self.updates.last_update(reader)? {
            for id in last_update_result_id..=last_id {
                if let Some(update) = self.update_status(reader, id)? {
                    updates.push(update);
                }
            }
        }

        Ok(updates)
    }

    pub fn query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(
            self.main,
            self.postings_lists,
            self.documents_fields_counts,
            self.synonyms,
            self.prefix_documents_cache,
            self.prefix_postings_lists_cache,
        )
    }

    pub fn query_builder_with_criteria<'c, 'f, 'd>(
        &self,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, 'f, 'd> {
        QueryBuilder::with_criteria(
            self.main,
            self.postings_lists,
            self.documents_fields_counts,
            self.synonyms,
            self.prefix_documents_cache,
            self.prefix_postings_lists_cache,
            criteria,
        )
    }
}

pub fn create(
    env: &heed::Env,
    update_env: &heed::Env,
    name: &str,
    updates_notifier: UpdateEventsEmitter,
) -> MResult<Index> {
    // create all the store names
    let main_name = main_name(name);
    let postings_lists_name = postings_lists_name(name);
    let documents_fields_name = documents_fields_name(name);
    let documents_fields_counts_name = documents_fields_counts_name(name);
    let synonyms_name = synonyms_name(name);
    let docs_words_name = docs_words_name(name);
    let prefix_documents_cache_name = prefix_documents_cache_name(name);
    let prefix_postings_lists_cache_name = prefix_postings_lists_cache_name(name);
    let updates_name = updates_name(name);
    let updates_results_name = updates_results_name(name);

    // open all the stores
    let main = env.create_poly_database(Some(&main_name))?;
    let postings_lists = env.create_database(Some(&postings_lists_name))?;
    let documents_fields = env.create_database(Some(&documents_fields_name))?;
    let documents_fields_counts = env.create_database(Some(&documents_fields_counts_name))?;
    let synonyms = env.create_database(Some(&synonyms_name))?;
    let docs_words = env.create_database(Some(&docs_words_name))?;
    let prefix_documents_cache = env.create_database(Some(&prefix_documents_cache_name))?;
    let prefix_postings_lists_cache = env.create_database(Some(&prefix_postings_lists_cache_name))?;
    let updates = update_env.create_database(Some(&updates_name))?;
    let updates_results = update_env.create_database(Some(&updates_results_name))?;

    Ok(Index {
        main: Main { main },
        postings_lists: PostingsLists { postings_lists },
        documents_fields: DocumentsFields { documents_fields },
        documents_fields_counts: DocumentsFieldsCounts { documents_fields_counts },
        synonyms: Synonyms { synonyms },
        docs_words: DocsWords { docs_words },
        prefix_postings_lists_cache: PrefixPostingsListsCache { prefix_postings_lists_cache },
        prefix_documents_cache: PrefixDocumentsCache { prefix_documents_cache },
        updates: Updates { updates },
        updates_results: UpdatesResults { updates_results },
        updates_notifier,
    })
}

pub fn open(
    env: &heed::Env,
    update_env: &heed::Env,
    name: &str,
    updates_notifier: UpdateEventsEmitter,
) -> MResult<Option<Index>> {
    // create all the store names
    let main_name = main_name(name);
    let postings_lists_name = postings_lists_name(name);
    let documents_fields_name = documents_fields_name(name);
    let documents_fields_counts_name = documents_fields_counts_name(name);
    let synonyms_name = synonyms_name(name);
    let docs_words_name = docs_words_name(name);
    let prefix_documents_cache_name = prefix_documents_cache_name(name);
    let prefix_postings_lists_cache_name = prefix_postings_lists_cache_name(name);
    let updates_name = updates_name(name);
    let updates_results_name = updates_results_name(name);

    // open all the stores
    let main = match env.open_poly_database(Some(&main_name))? {
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
    let prefix_documents_cache = match env.open_database(Some(&prefix_documents_cache_name))? {
        Some(prefix_documents_cache) => prefix_documents_cache,
        None => return Ok(None),
    };
    let prefix_postings_lists_cache = match env.open_database(Some(&prefix_postings_lists_cache_name))? {
        Some(prefix_postings_lists_cache) => prefix_postings_lists_cache,
        None => return Ok(None),
    };
    let updates = match update_env.open_database(Some(&updates_name))? {
        Some(updates) => updates,
        None => return Ok(None),
    };
    let updates_results = match update_env.open_database(Some(&updates_results_name))? {
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
        prefix_documents_cache: PrefixDocumentsCache { prefix_documents_cache },
        prefix_postings_lists_cache: PrefixPostingsListsCache { prefix_postings_lists_cache },
        updates: Updates { updates },
        updates_results: UpdatesResults { updates_results },
        updates_notifier,
    }))
}

pub fn clear(
    writer: &mut heed::RwTxn<MainT>,
    update_writer: &mut heed::RwTxn<UpdateT>,
    index: &Index,
) -> MResult<()> {
    // clear all the stores
    index.main.clear(writer)?;
    index.postings_lists.clear(writer)?;
    index.documents_fields.clear(writer)?;
    index.documents_fields_counts.clear(writer)?;
    index.synonyms.clear(writer)?;
    index.docs_words.clear(writer)?;
    index.prefix_documents_cache.clear(writer)?;
    index.prefix_postings_lists_cache.clear(writer)?;
    index.updates.clear(update_writer)?;
    index.updates_results.clear(update_writer)?;
    Ok(())
}
