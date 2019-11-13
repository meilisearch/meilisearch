use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};

use fst::SetBuilder;
use meilisearch_schema::Schema;
use sdset::{duo::Difference, duo::DifferenceByKey, SetBuf, SetOperation};

use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::serde::extract_document_id;
use crate::store::{self, Postings};
use crate::update::{next_update_id, compute_short_prefixes, Update};
use crate::{DocumentId, Error, MResult, RankedMap};

pub struct DocumentsDeletion {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    documents: Vec<DocumentId>,
}

impl DocumentsDeletion {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> DocumentsDeletion {
        DocumentsDeletion {
            updates_store,
            updates_results_store,
            updates_notifier,
            documents: Vec::new(),
        }
    }

    pub fn delete_document_by_id(&mut self, document_id: DocumentId) {
        self.documents.push(document_id);
    }

    pub fn delete_document<D>(&mut self, schema: &Schema, document: D) -> MResult<()>
    where
        D: serde::Serialize,
    {
        let identifier = schema.identifier_name();
        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        self.delete_document_by_id(document_id);

        Ok(())
    }

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_documents_deletion(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.documents,
        )?;
        Ok(update_id)
    }
}

impl Extend<DocumentId> for DocumentsDeletion {
    fn extend<T: IntoIterator<Item = DocumentId>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}

pub fn push_documents_deletion(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    deletion: Vec<DocumentId>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::documents_deletion(deletion);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_documents_deletion(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    deletion: Vec<DocumentId>,
) -> MResult<()> {
    let idset = SetBuf::from_dirty(deletion);

    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let mut ranked_map = match index.main.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    // collect the ranked attributes according to the schema
    let ranked_attrs: Vec<_> = schema
        .iter()
        .filter_map(
            |(_, attr, prop)| {
                if prop.is_ranked() {
                    Some(attr)
                } else {
                    None
                }
            },
        )
        .collect();

    for id in idset.as_slice() {
        // remove all the ranked attributes from the ranked_map
        for ranked_attr in &ranked_attrs {
            ranked_map.remove(*id, *ranked_attr);
        }
    }

    let mut deleted_documents = HashSet::new();
    let mut removed_words = BTreeSet::new();

    // iter over every postings lists and remove the documents matches from those,
    // delete the entire postings list entry when emptied
    let mut iter = index.postings_lists.postings_lists.iter_mut(writer)?;
    while let Some(result) = iter.next() {
        let (word, postings_list) = result?;

        let Postings { docids, matches } = postings_list;
        let new_docids: SetBuf<DocumentId> = Difference::new(&docids, &idset).into_set_buf();

        let removed_docids: SetBuf<DocumentId> = Difference::new(&idset, &docids).into_set_buf();
        deleted_documents.extend(removed_docids);

        if new_docids.is_empty() {
            iter.del_current()?;
            removed_words.insert(word.to_owned());
        } else {
            let op = DifferenceByKey::new(&matches, &idset, |d| d.document_id, |id| *id);
            let matches = op.into_set_buf();

            let docids = Cow::Owned(new_docids);
            let matches = Cow::Owned(matches);
            let postings_list = Postings { docids, matches };

            iter.put_current(word, &postings_list)?;
        }
    }

    drop(iter);

    // remove data associated to each deleted document
    for id in idset {
        index.documents_fields_counts.del_all_document_fields_counts(writer, id)?;
        index.documents_fields.del_all_document_fields(writer, id)?;
    }

    let deleted_documents_len = deleted_documents.len() as u64;
    let removed_words = fst::Set::from_iter(removed_words).unwrap();
    let words = match index.main.words_fst(writer)? {
        Some(words_set) => {
            let op = fst::set::OpBuilder::new()
                .add(words_set.stream())
                .add(removed_words.stream())
                .difference();

            let mut words_builder = SetBuilder::memory();
            words_builder.extend_stream(op).unwrap();
            words_builder
                .into_inner()
                .and_then(fst::Set::from_bytes)
                .unwrap()
        }
        None => fst::Set::default(),
    };

    index.main.put_words_fst(writer, &words)?;
    index.main.put_ranked_map(writer, &ranked_map)?;
    index.main.put_number_of_documents(writer, |old| old - deleted_documents_len)?;

    compute_short_prefixes(writer, index)?;

    Ok(())
}
