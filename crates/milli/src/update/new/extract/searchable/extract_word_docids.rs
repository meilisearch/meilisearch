use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::DerefMut as _;

use bumpalo::collections::vec::Vec as BumpVec;
use bumpalo::Bump;

use super::match_searchable_field;
use super::tokenize_document::{tokenizer_builder, DocumentTokenizer};
use crate::fields_ids_map::metadata::Metadata;
use crate::update::new::document::DocumentContext;
use crate::update::new::extract::cache::BalancedCaches;
use crate::update::new::extract::perm_json_p::contained_in;
use crate::update::new::extract::searchable::has_searchable_children;
use crate::update::new::indexer::document_changes::{
    extract, DocumentChanges, Extractor, IndexingContext,
};
use crate::update::new::indexer::settings_changes::{
    settings_change_extract, DocumentsIndentifiers, SettingsChangeExtractor,
};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, MostlySend, ThreadLocal};
use crate::update::new::{DocumentChange, DocumentIdentifiers};
use crate::update::settings::SettingsDelta;
use crate::{
    bucketed_position, DocumentId, FieldId, PatternMatch, Result, UserError,
    MAX_POSITION_PER_ATTRIBUTE,
};

const MAX_COUNTED_WORDS: usize = 30;

pub struct WordDocidsBalancedCaches<'extractor> {
    word_fid_docids: BalancedCaches<'extractor>,
    word_docids: BalancedCaches<'extractor>,
    exact_word_docids: BalancedCaches<'extractor>,
    word_position_docids: BalancedCaches<'extractor>,
    fid_word_count_docids: BalancedCaches<'extractor>,
    fid_word_count: HashMap<FieldId, (Option<usize>, Option<usize>)>,
    current_docid: Option<DocumentId>,
}

unsafe impl MostlySend for WordDocidsBalancedCaches<'_> {}

/// Whether to extract or skip fields during word extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldDbExtraction {
    /// Extract the word and put it in to the fid-based databases.
    Extract,
    /// Do not store the word in the fid-based databases.
    Skip,
}

impl<'extractor> WordDocidsBalancedCaches<'extractor> {
    pub fn new_in(buckets: usize, max_memory: Option<usize>, alloc: &'extractor Bump) -> Self {
        Self {
            word_fid_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            word_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            exact_word_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            word_position_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            fid_word_count_docids: BalancedCaches::new_in(buckets, max_memory, alloc),
            fid_word_count: HashMap::new(),
            current_docid: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_add_u32(
        &mut self,
        field_id: FieldId,
        position: u16,
        word: &str,
        exact: bool,
        field_db_extraction: FieldDbExtraction,
        docid: u32,
        bump: &Bump,
    ) -> Result<()> {
        let word_bytes = word.as_bytes();
        if exact {
            self.exact_word_docids.insert_add_u32(word_bytes, docid)?;
        } else {
            self.word_docids.insert_add_u32(word_bytes, docid)?;
        }

        let buffer_size = word_bytes.len() + 1 + size_of::<FieldId>();
        let mut buffer = BumpVec::with_capacity_in(buffer_size, bump);

        if field_db_extraction == FieldDbExtraction::Extract {
            buffer.clear();
            buffer.extend_from_slice(word_bytes);
            buffer.push(0);
            buffer.extend_from_slice(&field_id.to_be_bytes());
            self.word_fid_docids.insert_add_u32(&buffer, docid)?;
        }

        let position = bucketed_position(position);
        buffer.clear();
        buffer.extend_from_slice(word_bytes);
        buffer.push(0);
        buffer.extend_from_slice(&position.to_be_bytes());
        self.word_position_docids.insert_add_u32(&buffer, docid)?;

        if self.current_docid.is_some_and(|id| docid != id) {
            self.flush_fid_word_count(&mut buffer)?;
        }

        if field_db_extraction == FieldDbExtraction::Extract {
            self.fid_word_count
                .entry(field_id)
                .and_modify(|(_current_count, new_count)| *new_count.get_or_insert(0) += 1)
                .or_insert((None, Some(1)));
        }

        self.current_docid = Some(docid);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_del_u32(
        &mut self,
        field_id: FieldId,
        position: u16,
        word: &str,
        exact: bool,
        field_db_extraction: FieldDbExtraction,
        docid: u32,
        bump: &Bump,
    ) -> Result<()> {
        let word_bytes = word.as_bytes();
        if exact {
            self.exact_word_docids.insert_del_u32(word_bytes, docid)?;
        } else {
            self.word_docids.insert_del_u32(word_bytes, docid)?;
        }

        let buffer_size = word_bytes.len() + 1 + size_of::<FieldId>();
        let mut buffer = BumpVec::with_capacity_in(buffer_size, bump);

        if field_db_extraction == FieldDbExtraction::Extract {
            buffer.clear();
            buffer.extend_from_slice(word_bytes);
            buffer.push(0);
            buffer.extend_from_slice(&field_id.to_be_bytes());
            self.word_fid_docids.insert_del_u32(&buffer, docid)?;
        }

        let position = bucketed_position(position);
        buffer.clear();
        buffer.extend_from_slice(word_bytes);
        buffer.push(0);
        buffer.extend_from_slice(&position.to_be_bytes());
        self.word_position_docids.insert_del_u32(&buffer, docid)?;

        if self.current_docid.is_some_and(|id| docid != id) {
            self.flush_fid_word_count(&mut buffer)?;
        }

        if field_db_extraction == FieldDbExtraction::Extract {
            self.fid_word_count
                .entry(field_id)
                .and_modify(|(current_count, _new_count)| *current_count.get_or_insert(0) += 1)
                .or_insert((Some(1), None));
        }

        self.current_docid = Some(docid);

        Ok(())
    }

    fn flush_fid_word_count(&mut self, buffer: &mut BumpVec<u8>) -> Result<()> {
        for (fid, (current_count, new_count)) in self.fid_word_count.drain() {
            if current_count != new_count {
                if let Some(current_count) =
                    current_count.filter(|current_count| *current_count <= MAX_COUNTED_WORDS)
                {
                    buffer.clear();
                    buffer.extend_from_slice(&fid.to_be_bytes());
                    buffer.push(current_count as u8);
                    self.fid_word_count_docids
                        .insert_del_u32(buffer, self.current_docid.unwrap())?;
                }
                if let Some(new_count) =
                    new_count.filter(|new_count| *new_count <= MAX_COUNTED_WORDS)
                {
                    buffer.clear();
                    buffer.extend_from_slice(&fid.to_be_bytes());
                    buffer.push(new_count as u8);
                    self.fid_word_count_docids
                        .insert_add_u32(buffer, self.current_docid.unwrap())?;
                }
            }
        }

        Ok(())
    }
}

pub struct WordDocidsCaches<'extractor> {
    pub word_docids: Vec<BalancedCaches<'extractor>>,
    pub word_fid_docids: Vec<BalancedCaches<'extractor>>,
    pub exact_word_docids: Vec<BalancedCaches<'extractor>>,
    pub word_position_docids: Vec<BalancedCaches<'extractor>>,
    pub fid_word_count_docids: Vec<BalancedCaches<'extractor>>,
}

impl<'extractor> WordDocidsCaches<'extractor> {
    fn new() -> Self {
        Self {
            word_docids: Vec::new(),
            word_fid_docids: Vec::new(),
            exact_word_docids: Vec::new(),
            word_position_docids: Vec::new(),
            fid_word_count_docids: Vec::new(),
        }
    }

    fn push(&mut self, other: WordDocidsBalancedCaches<'extractor>) -> Result<()> {
        let WordDocidsBalancedCaches {
            word_docids,
            word_fid_docids,
            exact_word_docids,
            word_position_docids,
            fid_word_count_docids,
            fid_word_count: _,
            current_docid: _,
        } = other;

        self.word_docids.push(word_docids);
        self.word_fid_docids.push(word_fid_docids);
        self.exact_word_docids.push(exact_word_docids);
        self.word_position_docids.push(word_position_docids);
        self.fid_word_count_docids.push(fid_word_count_docids);

        Ok(())
    }
}

pub struct WordDocidsExtractorData<'a> {
    tokenizer: DocumentTokenizer<'a>,
    max_memory_by_thread: Option<usize>,
    buckets: usize,
    searchable_attributes: Option<Vec<&'a str>>,
}

impl<'extractor> Extractor<'extractor> for WordDocidsExtractorData<'_> {
    type Data = RefCell<Option<WordDocidsBalancedCaches<'extractor>>>;

    fn init_data(&self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(Some(WordDocidsBalancedCaches::new_in(
            self.buckets,
            self.max_memory_by_thread,
            extractor_alloc,
        ))))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentContext<Self::Data>,
    ) -> Result<()> {
        for change in changes {
            let change = change?;
            WordDocidsExtractors::extract_document_change(
                context,
                &self.tokenizer,
                self.searchable_attributes.as_deref(),
                change,
            )?;
        }
        Ok(())
    }
}

pub struct WordDocidsExtractors;

impl WordDocidsExtractors {
    pub fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<WordDocidsCaches<'extractor>>
    where
        MSP: Fn() -> bool + Sync,
    {
        // Warning: this is duplicated code from extract_word_pair_proximity_docids.rs
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
        let localized_attributes_rules =
            indexing_context.index.localized_attributes_rules(&rtxn)?.unwrap_or_default();
        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };
        let extractor_data = WordDocidsExtractorData {
            tokenizer: document_tokenizer,
            max_memory_by_thread: indexing_context.grenad_parameters.max_memory_by_thread(),
            buckets: rayon::current_num_threads(),
            searchable_attributes: indexing_context.index.user_defined_searchable_fields(&rtxn)?,
        };
        let datastore = ThreadLocal::new();
        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();
            extract(
                document_changes,
                &extractor_data,
                indexing_context,
                extractor_allocs,
                &datastore,
                step,
            )?;
        }

        let mut merger = WordDocidsCaches::new();
        for cache in datastore.into_iter().flat_map(RefCell::into_inner) {
            merger.push(cache)?;
        }

        Ok(merger)
    }

    fn extract_document_change(
        context: &DocumentContext<RefCell<Option<WordDocidsBalancedCaches>>>,
        document_tokenizer: &DocumentTokenizer,
        searchable_attributes: Option<&[&str]>,
        document_change: DocumentChange,
    ) -> Result<()> {
        let index = &context.index;
        let rtxn = &context.rtxn;
        let mut cached_sorter_ref = context.data.borrow_mut_or_yield();
        let cached_sorter = cached_sorter_ref.as_mut().unwrap();
        let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();
        let new_fields_ids_map = new_fields_ids_map.deref_mut();
        let doc_alloc = &context.doc_alloc;

        let exact_attributes = index.exact_attributes(rtxn)?;
        let disabled_typos_terms = index.disabled_typos_terms(rtxn)?;
        let is_exact = |fname: &str, word: &str| {
            exact_attributes.iter().any(|attr| contained_in(fname, attr))
                || disabled_typos_terms.is_exact(word)
        };

        let mut should_tokenize = |field_name: &str| {
            let Some((field_id, meta)) = new_fields_ids_map.id_with_metadata_or_insert(field_name)
            else {
                return Err(UserError::AttributeLimitReached.into());
            };

            let pattern_match = if meta.is_searchable() {
                PatternMatch::Match
            } else {
                // TODO: should be a match on the field_name using `match_field_legacy` function,
                //       but for legacy reasons we iterate over all the fields to fill the field_id_map.
                PatternMatch::Parent
            };

            Ok((field_id, pattern_match))
        };

        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_del_u32(
                        fid,
                        pos,
                        word,
                        is_exact(fname, word),
                        FieldDbExtraction::Extract,
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index, context.db_fields_ids_map)?,
                    &mut should_tokenize,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Update(inner) => {
                if !inner.has_changed_for_fields(
                    &mut |field_name: &str| {
                        match_searchable_field(field_name, searchable_attributes)
                    },
                    &context.rtxn,
                    context.index,
                    context.db_fields_ids_map,
                )? {
                    return Ok(());
                }

                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_del_u32(
                        fid,
                        pos,
                        word,
                        is_exact(fname, word),
                        FieldDbExtraction::Extract,
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.current(rtxn, index, context.db_fields_ids_map)?,
                    &mut should_tokenize,
                    &mut token_fn,
                )?;

                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_add_u32(
                        fid,
                        pos,
                        word,
                        is_exact(fname, word),
                        FieldDbExtraction::Extract,
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.merged(rtxn, index, context.db_fields_ids_map)?,
                    &mut should_tokenize,
                    &mut token_fn,
                )?;
            }
            DocumentChange::Insertion(inner) => {
                let mut token_fn = |fname: &str, fid, pos, word: &str| {
                    cached_sorter.insert_add_u32(
                        fid,
                        pos,
                        word,
                        is_exact(fname, word),
                        FieldDbExtraction::Extract,
                        inner.docid(),
                        doc_alloc,
                    )
                };
                document_tokenizer.tokenize_document(
                    inner.inserted(),
                    &mut should_tokenize,
                    &mut token_fn,
                )?;
            }
        }

        let buffer_size = size_of::<FieldId>();
        let mut buffer = BumpVec::with_capacity_in(buffer_size, &context.doc_alloc);
        cached_sorter.flush_fid_word_count(&mut buffer)
    }
}

pub struct WordDocidsSettingsExtractorsData<'a, SD> {
    tokenizer: DocumentTokenizer<'a>,
    max_memory_by_thread: Option<usize>,
    buckets: usize,
    settings_delta: &'a SD,
}

impl<'extractor, SD: SettingsDelta + Sync> SettingsChangeExtractor<'extractor>
    for WordDocidsSettingsExtractorsData<'_, SD>
{
    type Data = RefCell<Option<WordDocidsBalancedCaches<'extractor>>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> crate::Result<Self::Data> {
        Ok(RefCell::new(Some(WordDocidsBalancedCaches::new_in(
            self.buckets,
            self.max_memory_by_thread,
            extractor_alloc,
        ))))
    }

    fn process<'doc>(
        &'doc self,
        documents: impl Iterator<Item = crate::Result<DocumentIdentifiers<'doc>>>,
        context: &'doc DocumentContext<Self::Data>,
    ) -> crate::Result<()> {
        for document in documents {
            let document = document?;
            SettingsChangeWordDocidsExtractors::extract_document_from_settings_change(
                document,
                context,
                &self.tokenizer,
                self.settings_delta,
            )?;
        }
        Ok(())
    }
}

pub struct SettingsChangeWordDocidsExtractors;

impl SettingsChangeWordDocidsExtractors {
    pub fn run_extraction<'fid, 'indexer, 'index, 'extractor, SD, MSP>(
        settings_delta: &SD,
        documents: &'indexer DocumentsIndentifiers<'indexer>,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        step: IndexingStep,
    ) -> Result<WordDocidsCaches<'extractor>>
    where
        SD: SettingsDelta + Sync,
        MSP: Fn() -> bool + Sync,
    {
        // Warning: this is duplicated code from extract_word_pair_proximity_docids.rs
        // TODO we need to read the new AND old settings to support changing global parameters
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
        let localized_attributes_rules =
            indexing_context.index.localized_attributes_rules(&rtxn)?.unwrap_or_default();
        let document_tokenizer = DocumentTokenizer {
            tokenizer: &tokenizer,
            localized_attributes_rules: &localized_attributes_rules,
            max_positions_per_attributes: MAX_POSITION_PER_ATTRIBUTE,
        };
        let extractor_data = WordDocidsSettingsExtractorsData {
            tokenizer: document_tokenizer,
            max_memory_by_thread: indexing_context.grenad_parameters.max_memory_by_thread(),
            buckets: rayon::current_num_threads(),
            settings_delta,
        };
        let datastore = ThreadLocal::new();
        {
            let span = tracing::debug_span!(target: "indexing::documents::extract", "vectors");
            let _entered = span.enter();

            settings_change_extract(
                documents,
                &extractor_data,
                indexing_context,
                extractor_allocs,
                &datastore,
                step,
            )?;
        }

        let mut merger = WordDocidsCaches::new();
        for cache in datastore.into_iter().flat_map(RefCell::into_inner) {
            merger.push(cache)?;
        }

        Ok(merger)
    }

    /// Extracts document words from a settings change.
    fn extract_document_from_settings_change<SD: SettingsDelta>(
        document: DocumentIdentifiers<'_>,
        context: &DocumentContext<RefCell<Option<WordDocidsBalancedCaches>>>,
        document_tokenizer: &DocumentTokenizer,
        settings_delta: &SD,
    ) -> Result<()> {
        let mut cached_sorter_ref = context.data.borrow_mut_or_yield();
        let cached_sorter = cached_sorter_ref.as_mut().unwrap();
        let doc_alloc = &context.doc_alloc;

        let new_fields_ids_map = settings_delta.new_fields_ids_map();
        let old_fields_ids_map = context.index.fields_ids_map_with_metadata(&context.rtxn)?;
        let old_searchable = settings_delta.old_searchable_attributes().as_ref();
        let new_searchable = settings_delta.new_searchable_attributes().as_ref();

        let current_document = document.current(
            &context.rtxn,
            context.index,
            old_fields_ids_map.as_fields_ids_map(),
        )?;

        #[derive(Debug, Clone, Copy, PartialEq)]
        enum ActionToOperate {
            ReindexAllFields,
            // TODO improve by listing field prefixes
            IndexAddedFields,
            SkipDocument,
        }

        let mut action = ActionToOperate::SkipDocument;
        // Here we do a preliminary check to determine the action to take.
        // This check doesn't trigger the tokenizer as we never return
        // PatternMatch::Match.
        document_tokenizer.tokenize_document(
            current_document,
            &mut |field_name| {
                let fid = new_fields_ids_map.id(field_name).expect("All fields IDs must exist");

                // If the document must be reindexed, early return NoMatch to stop the scanning process.
                if action == ActionToOperate::ReindexAllFields {
                    return Ok((fid, PatternMatch::NoMatch));
                }

                let old_field_metadata = old_fields_ids_map.metadata(fid).unwrap();
                let new_field_metadata = new_fields_ids_map.metadata(fid).unwrap();

                action = match (old_field_metadata, new_field_metadata) {
                    // At least one field is added or removed from the exact fields => ReindexAllFields
                    (Metadata { exact: old_exact, .. }, Metadata { exact: new_exact, .. })
                        if old_exact != new_exact =>
                    {
                        ActionToOperate::ReindexAllFields
                    }
                    // At least one field is removed from the searchable fields => ReindexAllFields
                    (Metadata { searchable: Some(_), .. }, Metadata { searchable: None, .. }) => {
                        ActionToOperate::ReindexAllFields
                    }
                    // At least one field is added in the searchable fields => IndexAddedFields
                    (Metadata { searchable: None, .. }, Metadata { searchable: Some(_), .. }) => {
                        // We can safely overwrite the action, because we early return when action is ReindexAllFields.
                        ActionToOperate::IndexAddedFields
                    }
                    _ => action,
                };

                Ok((fid, PatternMatch::Parent))
            },
            &mut |_, _, _, _| Ok(()),
        )?;

        // Early return when we don't need to index the document
        if action == ActionToOperate::SkipDocument {
            return Ok(());
        }

        let mut should_tokenize = |field_name: &str| {
            let field_id = new_fields_ids_map.id(field_name).expect("All fields IDs must exist");
            let old_field_metadata = old_fields_ids_map.metadata(field_id).unwrap();
            let new_field_metadata = new_fields_ids_map.metadata(field_id).unwrap();

            let pattern_match = match action {
                ActionToOperate::ReindexAllFields => {
                    if old_field_metadata.is_searchable() || new_field_metadata.is_searchable() {
                        PatternMatch::Match
                    // If any old or new field is searchable then we need to iterate over all fields
                    // else if any field matches we need to iterate over all fields
                    } else if has_searchable_children(
                        field_name,
                        old_searchable.zip(new_searchable).map(|(old, new)| old.iter().chain(new)),
                    ) {
                        PatternMatch::Parent
                    } else {
                        PatternMatch::NoMatch
                    }
                }
                ActionToOperate::IndexAddedFields => {
                    // Was not searchable but now is
                    if !old_field_metadata.is_searchable() && new_field_metadata.is_searchable() {
                        PatternMatch::Match
                    // If the field is now a parent of a searchable field
                    } else if has_searchable_children(field_name, new_searchable) {
                        PatternMatch::Parent
                    } else {
                        PatternMatch::NoMatch
                    }
                }
                ActionToOperate::SkipDocument => unreachable!(),
            };

            Ok((field_id, pattern_match))
        };

        let old_disabled_typos_terms = settings_delta.old_disabled_typos_terms();
        let new_disabled_typos_terms = settings_delta.new_disabled_typos_terms();
        let mut token_fn = |_field_name: &str, field_id, pos, word: &str| {
            let old_field_metadata = old_fields_ids_map.metadata(field_id).unwrap();
            let new_field_metadata = new_fields_ids_map.metadata(field_id).unwrap();

            match (old_field_metadata, new_field_metadata) {
                (
                    Metadata { searchable: Some(_), exact: old_exact, .. },
                    Metadata { searchable: None, .. },
                ) => cached_sorter.insert_del_u32(
                    field_id,
                    pos,
                    word,
                    old_exact || old_disabled_typos_terms.is_exact(word),
                    // We deleted the field globally
                    FieldDbExtraction::Skip,
                    document.docid(),
                    doc_alloc,
                ),
                (
                    Metadata { searchable: None, .. },
                    Metadata { searchable: Some(_), exact: new_exact, .. },
                ) => cached_sorter.insert_add_u32(
                    field_id,
                    pos,
                    word,
                    new_exact || new_disabled_typos_terms.is_exact(word),
                    FieldDbExtraction::Extract,
                    document.docid(),
                    doc_alloc,
                ),
                (Metadata { searchable: None, .. }, Metadata { searchable: None, .. }) => {
                    unreachable!()
                }
                (Metadata { exact: old_exact, .. }, Metadata { exact: new_exact, .. }) => {
                    cached_sorter.insert_del_u32(
                        field_id,
                        pos,
                        word,
                        old_exact || old_disabled_typos_terms.is_exact(word),
                        // The field has already been extracted
                        FieldDbExtraction::Skip,
                        document.docid(),
                        doc_alloc,
                    )?;
                    cached_sorter.insert_add_u32(
                        field_id,
                        pos,
                        word,
                        new_exact || new_disabled_typos_terms.is_exact(word),
                        // The field has already been extracted
                        FieldDbExtraction::Skip,
                        document.docid(),
                        doc_alloc,
                    )
                }
            }
        };

        // TODO we must tokenize twice when we change global parameters like stop words,
        //      the language settings, dictionary, separators, non-separators...
        document_tokenizer.tokenize_document(
            current_document,
            &mut should_tokenize,
            &mut token_fn,
        )?;

        Ok(())
    }
}
