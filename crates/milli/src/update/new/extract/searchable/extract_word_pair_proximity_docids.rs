use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use bumpalo::Bump;
use heed::RoTxn;

use super::tokenize_document::{tokenizer_builder, DocumentTokenizer};
use crate::proximity::{index_proximity, MAX_DISTANCE};
use crate::update::new::document::Document;
use crate::update::new::extract::cache::BalancedCaches;
use crate::update::new::indexer::document_changes::{
    extract, DocumentChangeContext, DocumentChanges, Extractor, IndexingContext,
};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, ThreadLocal};
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{FieldId, GlobalFieldsIdsMap, Index, Result, MAX_POSITION_PER_ATTRIBUTE};

impl<'a, 'extractor> Extractor<'extractor> for WordPairProximityDocidsExtractor<'a> {
    type Data = RefCell<BalancedCaches<'extractor>>;

    fn init_data(&self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(BalancedCaches::new_in(
            self.buckets,
            self.grenad_parameters.max_memory_by_thread(self.buckets),
            extractor_alloc,
        )))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        for change in changes {
            let change = change?;
            self.extract_document_change(context, change)?;
        }
        Ok(())
    }
}

pub struct WordPairProximityDocidsExtractor<'a> {
    tokenizer: &'a DocumentTokenizer<'a>,
    grenad_parameters: &'a GrenadParameters,
    buckets: usize,
}

impl<'a> WordPairProximityDocidsExtractor<'a> {
    fn attributes_to_extract<'b>(
        rtxn: &'b RoTxn,
        index: &'b Index,
    ) -> Result<Option<Vec<&'b str>>> {
        index.user_defined_searchable_fields(rtxn).map_err(Into::into)
    }

    fn attributes_to_skip<'b>(_rtxn: &'b RoTxn, _index: &'b Index) -> Result<Vec<&'b str>> {
        Ok(Vec::new())
    }

    // This method is reimplemented to count the number of words in the document in each field
    // and to store the docids of the documents that have a number of words in a given field
    // equal to or under than MAX_COUNTED_WORDS.
    fn extract_document_change(
        &self,
        context: &DocumentChangeContext<RefCell<BalancedCaches>>,
        document_change: DocumentChange,
    ) -> Result<()> {
        let document_tokenizer = self.tokenizer;
        let doc_alloc = &context.doc_alloc;

        let index = context.index;
        let rtxn = &context.rtxn;

        let mut key_buffer = bumpalo::collections::Vec::new_in(doc_alloc);
        let mut del_word_pair_proximity = bumpalo::collections::Vec::new_in(doc_alloc);
        let mut add_word_pair_proximity = bumpalo::collections::Vec::new_in(doc_alloc);

        let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();
        let new_fields_ids_map = &mut *new_fields_ids_map;

        let mut cached_sorter = context.data.borrow_mut_or_yield();
        let cached_sorter = &mut *cached_sorter;

        // is a vecdequeue, and will be smol, so can stay on the heap for now
        let mut word_positions: VecDeque<(Rc<str>, u16)> =
            VecDeque::with_capacity(MAX_DISTANCE as usize);

        let docid = document_change.docid();
        match document_change {
            DocumentChange::Deletion(inner) => {
                let document = inner.current(rtxn, index, context.db_fields_ids_map)?;
                process_document_tokens(
                    document,
                    document_tokenizer,
                    new_fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        del_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
            }
            DocumentChange::Update(inner) => {
                if !inner.has_changed_for_fields(
                    document_tokenizer.attribute_to_extract,
                    rtxn,
                    index,
                    context.db_fields_ids_map,
                )? {
                    return Ok(());
                }

                let document = inner.current(rtxn, index, context.db_fields_ids_map)?;
                process_document_tokens(
                    document,
                    document_tokenizer,
                    new_fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        del_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
                let document = inner.merged(rtxn, index, context.db_fields_ids_map)?;
                process_document_tokens(
                    document,
                    document_tokenizer,
                    new_fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        add_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
            }
            DocumentChange::Insertion(inner) => {
                let document = inner.inserted();
                process_document_tokens(
                    document,
                    document_tokenizer,
                    new_fields_ids_map,
                    &mut word_positions,
                    &mut |(w1, w2), prox| {
                        add_word_pair_proximity.push(((w1, w2), prox));
                    },
                )?;
            }
        }

        del_word_pair_proximity.sort_unstable();
        del_word_pair_proximity.dedup_by(|(k1, _), (k2, _)| k1 == k2);
        for ((w1, w2), prox) in del_word_pair_proximity.iter() {
            let key = build_key(*prox, w1, w2, &mut key_buffer);
            cached_sorter.insert_del_u32(key, docid)?;
        }

        add_word_pair_proximity.sort_unstable();
        add_word_pair_proximity.dedup_by(|(k1, _), (k2, _)| k1 == k2);
        for ((w1, w2), prox) in add_word_pair_proximity.iter() {
            let key = build_key(*prox, w1, w2, &mut key_buffer);
            cached_sorter.insert_add_u32(key, docid)?;
        }
        Ok(())
    }

    pub fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        thread_pool: &scoped_thread_pool::ThreadPool<crate::Error>,
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        MSP: Fn() -> bool + Sync,
    {
        let rtxn = indexing_context.index.read_txn()?;
        let stop_words = indexing_context.index.stop_words(&rtxn)?;
        let allowed_separators = indexing_context.index.allowed_separators(&rtxn)?;
        let allowed_separators: Option<Vec<_>> =
            allowed_separators.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let dictionary = indexing_context.index.dictionary(&rtxn)?;
        let dictionary: Option<Vec<_>> =
            dictionary.as_ref().map(|s| s.iter().map(String::as_str).collect());
        let mut builder = tokenizer_builder(
            stop_words.as_ref(),
            allowed_separators.as_deref(),
            dictionary.as_deref(),
        );
        let tokenizer = builder.build();

        let attributes_to_extract = Self::attributes_to_extract(&rtxn, indexing_context.index)?;
        let attributes_to_skip = Self::attributes_to_skip(&rtxn, indexing_context.index)?;
        let localized_attributes_rules =
            indexing_context.index.localized_attributes_rules(&rtxn)?.unwrap_or_default();

        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            attribute_to_extract: attributes_to_extract.as_deref(),
            attribute_to_skip: attributes_to_skip.as_slice(),
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };

        let extractor_data: WordPairProximityDocidsExtractor = WordPairProximityDocidsExtractor {
            tokenizer: &document_tokenizer,
            grenad_parameters: indexing_context.grenad_parameters,
            buckets: thread_pool.thread_count(),
        };

        let datastore = ThreadLocal::new();

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();
            extract(
                thread_pool,
                document_changes,
                &extractor_data,
                indexing_context,
                extractor_allocs,
                &datastore,
                step,
            )?;
        }

        Ok(datastore.into_iter().map(RefCell::into_inner).collect())
    }
}

fn build_key<'a>(
    prox: u8,
    w1: &str,
    w2: &str,
    key_buffer: &'a mut bumpalo::collections::Vec<u8>,
) -> &'a [u8] {
    key_buffer.clear();
    key_buffer.push(prox);
    key_buffer.extend_from_slice(w1.as_bytes());
    key_buffer.push(0);
    key_buffer.extend_from_slice(w2.as_bytes());
    key_buffer.as_slice()
}

fn word_positions_into_word_pair_proximity(
    word_positions: &mut VecDeque<(Rc<str>, u16)>,
    word_pair_proximity: &mut impl FnMut((Rc<str>, Rc<str>), u8),
) {
    let (head_word, head_position) = word_positions.pop_front().unwrap();
    for (word, position) in word_positions.iter() {
        let prox = index_proximity(head_position as u32, *position as u32) as u8;
        if prox > 0 && prox < MAX_DISTANCE as u8 {
            word_pair_proximity((head_word.clone(), word.clone()), prox);
        }
    }
}

fn drain_word_positions(
    word_positions: &mut VecDeque<(Rc<str>, u16)>,
    word_pair_proximity: &mut impl FnMut((Rc<str>, Rc<str>), u8),
) {
    while !word_positions.is_empty() {
        word_positions_into_word_pair_proximity(word_positions, word_pair_proximity);
    }
}

fn process_document_tokens<'doc>(
    document: impl Document<'doc>,
    document_tokenizer: &DocumentTokenizer,
    fields_ids_map: &mut GlobalFieldsIdsMap,
    word_positions: &mut VecDeque<(Rc<str>, u16)>,
    word_pair_proximity: &mut impl FnMut((Rc<str>, Rc<str>), u8),
) -> Result<()> {
    let mut field_id = None;
    let mut token_fn = |_fname: &str, fid: FieldId, pos: u16, word: &str| {
        if field_id != Some(fid) {
            field_id = Some(fid);
            drain_word_positions(word_positions, word_pair_proximity);
        }
        // drain the proximity window until the head word is considered close to the word we are inserting.
        while word_positions
            .front()
            .map_or(false, |(_w, p)| index_proximity(*p as u32, pos as u32) >= MAX_DISTANCE)
        {
            word_positions_into_word_pair_proximity(word_positions, word_pair_proximity);
        }

        // insert the new word.
        word_positions.push_back((Rc::from(word), pos));
        Ok(())
    };
    document_tokenizer.tokenize_document(document, fields_ids_map, &mut token_fn)?;

    drain_word_positions(word_positions, word_pair_proximity);
    Ok(())
}
