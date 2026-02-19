use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::DerefMut as _;

use bumpalo::collections::Vec as BVec;
use bumpalo::Bump;
use hashbrown::HashMap;
use serde_json::Value;

use super::super::cache::BalancedCaches;
use super::facet_document::{extract_document_facets, extract_geo_document};
use super::FacetKind;
use crate::fields_ids_map::metadata::Metadata;
use crate::filterable_attributes_rules::match_faceted_field;
use crate::heed_codec::facet::OrderedF64Codec;
use crate::update::del_add::DelAdd;
use crate::update::new::channel::FieldIdDocidFacetSender;
use crate::update::new::document::{Document as _, DocumentContext};
use crate::update::new::extract::{extract_geo_coordinates, perm_json_p};
use crate::update::new::indexer::document_changes::{
    extract, DocumentChanges, Extractor, IndexingContext,
};
use crate::update::new::indexer::settings_change_extract;
use crate::update::new::indexer::settings_changes::{
    DocumentsIndentifiers, SettingsChangeExtractor,
};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::steps::IndexingStep;
use crate::update::new::thread_local::{FullySend, ThreadLocal};
use crate::update::new::{DocumentChange, DocumentIdentifiers};
use crate::update::settings::SettingsDelta;
use crate::update::GrenadParameters;
use crate::{
    DocumentId, FieldId, FieldIdMapMissingEntry, FilterFeatures, FilterableAttributesFeatures,
    FilterableAttributesRule, InternalError, PatternMatch, Result, UserError,
    MAX_FACET_VALUE_LENGTH,
};

pub struct FacetedExtractorData<'a, 'b> {
    sender: &'a FieldIdDocidFacetSender<'a, 'b>,
    grenad_parameters: &'a GrenadParameters,
    buckets: usize,
    filterable_attributes: &'a [FilterableAttributesRule],
    sortable_fields: &'a HashSet<String>,
    asc_desc_fields: &'a HashSet<String>,
    distinct_field: &'a Option<String>,
    is_geo_enabled: bool,
}

impl<'extractor> Extractor<'extractor> for FacetedExtractorData<'_, '_> {
    type Data = RefCell<BalancedCaches<'extractor>>;

    fn init_data(&self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(BalancedCaches::new_in(
            self.buckets,
            self.grenad_parameters.max_memory_by_thread(),
            extractor_alloc,
        )))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentContext<Self::Data>,
    ) -> Result<()> {
        for change in changes {
            let change = change?;
            FacetedDocidsExtractor::extract_document_change(
                context,
                self.filterable_attributes,
                self.sortable_fields,
                self.asc_desc_fields,
                self.distinct_field,
                self.is_geo_enabled,
                change,
                self.sender,
            )?
        }
        Ok(())
    }
}

pub struct FacetedDocidsExtractor;

impl FacetedDocidsExtractor {
    #[allow(clippy::too_many_arguments)]
    fn extract_document_change(
        context: &DocumentContext<RefCell<BalancedCaches>>,
        filterable_attributes: &[FilterableAttributesRule],
        sortable_fields: &HashSet<String>,
        asc_desc_fields: &HashSet<String>,
        distinct_field: &Option<String>,
        is_geo_enabled: bool,
        document_change: DocumentChange,
        sender: &FieldIdDocidFacetSender,
    ) -> Result<()> {
        let index = context.index;
        let rtxn = &context.rtxn;
        let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();
        let mut cached_sorter = context.data.borrow_mut_or_yield();
        let mut del_add_facet_value = DelAddFacetValue::new(&context.doc_alloc);
        let docid = document_change.docid();

        // Using a macro avoid borrowing the parameters as mutable in both closures at
        // the same time by postponing their creation
        macro_rules! facet_fn {
            (del) => {
                |fid: FieldId, meta: Metadata, depth: perm_json_p::Depth, value: &Value| {
                    Self::facet_fn_with_options(
                        &context.doc_alloc,
                        cached_sorter.deref_mut(),
                        BalancedCaches::insert_del_u32,
                        &mut del_add_facet_value,
                        DelAddFacetValue::insert_del,
                        docid,
                        fid,
                        meta,
                        filterable_attributes,
                        depth,
                        value,
                    )
                }
            };
            (add) => {
                |fid: FieldId, meta: Metadata, depth: perm_json_p::Depth, value: &Value| {
                    Self::facet_fn_with_options(
                        &context.doc_alloc,
                        cached_sorter.deref_mut(),
                        BalancedCaches::insert_add_u32,
                        &mut del_add_facet_value,
                        DelAddFacetValue::insert_add,
                        docid,
                        fid,
                        meta,
                        filterable_attributes,
                        depth,
                        value,
                    )
                }
            };
        }

        match document_change {
            DocumentChange::Deletion(inner) => {
                let mut del = facet_fn!(del);

                extract_document_facets(
                    inner.current(rtxn, index, context.db_fields_ids_map)?,
                    |field_name| {
                        match_faceted_field(
                            field_name,
                            filterable_attributes,
                            sortable_fields,
                            asc_desc_fields,
                            distinct_field,
                        )
                    },
                    &mut |name| {
                        new_fields_ids_map
                            .id_with_metadata_or_insert(name)
                            .ok_or(UserError::AttributeLimitReached.into())
                    },
                    &mut del,
                )?;

                if is_geo_enabled {
                    extract_geo_document(
                        inner.current(rtxn, index, context.db_fields_ids_map)?,
                        inner.external_document_id(),
                        new_fields_ids_map.deref_mut(),
                        &mut del,
                    )?;
                }
            }
            DocumentChange::Update(inner) => {
                let has_changed_for_facets = inner.has_changed_for_fields(
                    &mut |field_name| {
                        match_faceted_field(
                            field_name,
                            filterable_attributes,
                            sortable_fields,
                            asc_desc_fields,
                            distinct_field,
                        )
                    },
                    rtxn,
                    index,
                    context.db_fields_ids_map,
                )?;

                // 1. Maybe update doc
                if has_changed_for_facets {
                    extract_document_facets(
                        inner.current(rtxn, index, context.db_fields_ids_map)?,
                        |field_name| {
                            match_faceted_field(
                                field_name,
                                filterable_attributes,
                                sortable_fields,
                                asc_desc_fields,
                                distinct_field,
                            )
                        },
                        &mut |name| {
                            new_fields_ids_map
                                .id_with_metadata_or_insert(name)
                                .ok_or(UserError::AttributeLimitReached.into())
                        },
                        &mut facet_fn!(del),
                    )?;

                    extract_document_facets(
                        inner.merged(rtxn, index, context.db_fields_ids_map)?,
                        |field_name| {
                            match_faceted_field(
                                field_name,
                                filterable_attributes,
                                sortable_fields,
                                asc_desc_fields,
                                distinct_field,
                            )
                        },
                        &mut |name| {
                            new_fields_ids_map
                                .id_with_metadata_or_insert(name)
                                .ok_or(UserError::AttributeLimitReached.into())
                        },
                        &mut facet_fn!(add),
                    )?;
                }

                // 2. Maybe update geo
                if is_geo_enabled
                    && inner.has_changed_for_geo_fields(rtxn, index, context.db_fields_ids_map)?
                {
                    extract_geo_document(
                        inner.current(rtxn, index, context.db_fields_ids_map)?,
                        inner.external_document_id(),
                        new_fields_ids_map.deref_mut(),
                        &mut facet_fn!(del),
                    )?;
                    extract_geo_document(
                        inner.merged(rtxn, index, context.db_fields_ids_map)?,
                        inner.external_document_id(),
                        new_fields_ids_map.deref_mut(),
                        &mut facet_fn!(add),
                    )?;
                }
            }
            DocumentChange::Insertion(inner) => {
                let mut add = facet_fn!(add);

                extract_document_facets(
                    inner.inserted(),
                    |field_name| {
                        match_faceted_field(
                            field_name,
                            filterable_attributes,
                            sortable_fields,
                            asc_desc_fields,
                            distinct_field,
                        )
                    },
                    &mut |name| {
                        new_fields_ids_map
                            .id_with_metadata_or_insert(name)
                            .ok_or(UserError::AttributeLimitReached.into())
                    },
                    &mut add,
                )?;

                if is_geo_enabled {
                    extract_geo_document(
                        inner.inserted(),
                        inner.external_document_id(),
                        new_fields_ids_map.deref_mut(),
                        &mut add,
                    )?;
                }
            }
        };

        del_add_facet_value.send_data(docid, sender, &context.doc_alloc).unwrap();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn facet_fn_with_options<'extractor, 'doc>(
        doc_alloc: &'doc Bump,
        cached_sorter: &mut BalancedCaches<'extractor>,
        cache_fn: impl Fn(&mut BalancedCaches<'extractor>, &[u8], u32) -> Result<()>,
        del_add_facet_value: &mut DelAddFacetValue<'doc>,
        facet_fn: impl Fn(&mut DelAddFacetValue<'doc>, FieldId, BVec<'doc, u8>, FacetKind),
        docid: DocumentId,
        fid: FieldId,
        meta: Metadata,
        filterable_attributes: &[FilterableAttributesRule],
        depth: perm_json_p::Depth,
        value: &Value,
    ) -> Result<()> {
        // if the field is not faceted, do nothing
        if !meta.is_faceted(filterable_attributes) {
            return Ok(());
        }

        let features = meta.filterable_attributes_features(filterable_attributes);

        let mut buffer = BVec::new_in(doc_alloc);
        // Exists
        // key: fid
        buffer.push(FacetKind::Exists as u8);
        buffer.extend_from_slice(&fid.to_be_bytes());
        cache_fn(cached_sorter, &buffer, docid)?;

        match value {
            // Number
            // key: fid - level - orderedf64 - originalf64
            Value::Number(number) => {
                let mut ordered = [0u8; 16];
                if number
                    .as_f64()
                    .and_then(|n| OrderedF64Codec::serialize_into(n, &mut ordered).ok())
                    .is_some()
                {
                    let mut number = BVec::with_capacity_in(16, doc_alloc);
                    number.extend_from_slice(&ordered);
                    facet_fn(del_add_facet_value, fid, number, FacetKind::Number);

                    buffer.clear();
                    buffer.push(FacetKind::Number as u8);
                    buffer.extend_from_slice(&fid.to_be_bytes());
                    buffer.push(0); // level 0
                    buffer.extend_from_slice(&ordered);
                    cache_fn(cached_sorter, &buffer, docid)
                } else {
                    Ok(())
                }
            }
            // String
            // key: fid - level - truncated_string
            Value::String(s) if !s.is_empty() => {
                let mut string = BVec::new_in(doc_alloc);
                string.extend_from_slice(s.as_bytes());
                facet_fn(del_add_facet_value, fid, string, FacetKind::String);

                let normalized = crate::normalize_facet(s);
                let truncated = truncate_str(&normalized);
                buffer.clear();
                buffer.push(FacetKind::String as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                buffer.push(0); // level 0
                buffer.extend_from_slice(truncated.as_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            // Bool is handled as a string
            Value::Bool(b) => {
                let b = if *b { "true" } else { "false" };
                let mut string = BVec::new_in(doc_alloc);
                string.extend_from_slice(b.as_bytes());
                facet_fn(del_add_facet_value, fid, string, FacetKind::String);

                buffer.clear();
                buffer.push(FacetKind::String as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                buffer.push(0); // level 0
                buffer.extend_from_slice(b.as_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            // Null
            // key: fid
            Value::Null
                if depth == perm_json_p::Depth::OnBaseKey && features.is_filterable_null() =>
            {
                buffer.clear();
                buffer.push(FacetKind::Null as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            // Empty
            // key: fid
            Value::Array(a)
                if a.is_empty()
                    && depth == perm_json_p::Depth::OnBaseKey
                    && features.is_filterable_empty() =>
            {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            Value::String(_)
                if depth == perm_json_p::Depth::OnBaseKey && features.is_filterable_empty() =>
            {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            Value::Object(o)
                if o.is_empty()
                    && depth == perm_json_p::Depth::OnBaseKey
                    && features.is_filterable_empty() =>
            {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            // Otherwise, do nothing
            _ => Ok(()),
        }
    }
}

struct DelAddFacetValue<'doc> {
    strings: HashMap<
        (FieldId, &'doc str),
        Option<BVec<'doc, u8>>,
        hashbrown::DefaultHashBuilder,
        &'doc Bump,
    >,
    f64s: HashMap<(FieldId, BVec<'doc, u8>), DelAdd, hashbrown::DefaultHashBuilder, &'doc Bump>,
    doc_alloc: &'doc Bump,
}

impl<'doc> DelAddFacetValue<'doc> {
    fn new(doc_alloc: &'doc Bump) -> Self {
        Self { strings: HashMap::new_in(doc_alloc), f64s: HashMap::new_in(doc_alloc), doc_alloc }
    }

    fn insert_add(&mut self, fid: FieldId, value: BVec<'doc, u8>, kind: FacetKind) {
        match kind {
            FacetKind::Number => {
                let key = (fid, value);
                if let Some(DelAdd::Deletion) = self.f64s.get(&key) {
                    self.f64s.remove(&key);
                } else {
                    self.f64s.insert(key, DelAdd::Addition);
                }
            }
            FacetKind::String => {
                if let Ok(s) = std::str::from_utf8(&value) {
                    let normalized = crate::normalize_facet(s);
                    let truncated = self.doc_alloc.alloc_str(truncate_str(&normalized));
                    self.strings.insert((fid, truncated), Some(value));
                }
            }
            _ => (),
        }
    }

    fn insert_del(&mut self, fid: FieldId, value: BVec<'doc, u8>, kind: FacetKind) {
        match kind {
            FacetKind::Number => {
                let key = (fid, value);
                if let Some(DelAdd::Addition) = self.f64s.get(&key) {
                    self.f64s.remove(&key);
                } else {
                    self.f64s.insert(key, DelAdd::Deletion);
                }
            }
            FacetKind::String => {
                if let Ok(s) = std::str::from_utf8(&value) {
                    let normalized = crate::normalize_facet(s);
                    let truncated = self.doc_alloc.alloc_str(truncate_str(&normalized));
                    self.strings.insert((fid, truncated), None);
                }
            }
            _ => (),
        }
    }

    fn send_data(
        self,
        docid: DocumentId,
        sender: &FieldIdDocidFacetSender,
        doc_alloc: &Bump,
    ) -> crate::Result<()> {
        let mut buffer = bumpalo::collections::Vec::new_in(doc_alloc);
        for ((fid, truncated), value) in self.strings {
            buffer.clear();
            buffer.extend_from_slice(&fid.to_be_bytes());
            buffer.extend_from_slice(&docid.to_be_bytes());
            buffer.extend_from_slice(truncated.as_bytes());
            match &value {
                Some(value) => sender.write_facet_string(&buffer, value)?,
                None => sender.delete_facet_string(&buffer)?,
            }
        }

        for ((fid, value), deladd) in self.f64s {
            buffer.clear();
            buffer.extend_from_slice(&fid.to_be_bytes());
            buffer.extend_from_slice(&docid.to_be_bytes());
            buffer.extend_from_slice(&value);
            match deladd {
                DelAdd::Deletion => sender.delete_facet_f64(&buffer)?,
                DelAdd::Addition => sender.write_facet_f64(&buffer)?,
            }
        }

        Ok(())
    }
}

/// Truncates a string to the biggest valid LMDB key size.
fn truncate_str(s: &str) -> &str {
    let index = s
        .char_indices()
        .map(|(idx, _)| idx)
        .chain(std::iter::once(s.len()))
        .take_while(|idx| idx <= &MAX_FACET_VALUE_LENGTH)
        .last();

    &s[..index.unwrap_or(0)]
}

impl FacetedDocidsExtractor {
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::extract::faceted")]
    pub fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP>(
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        sender: &FieldIdDocidFacetSender,
        step: IndexingStep,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        MSP: Fn() -> bool + Sync,
    {
        let index = indexing_context.index;
        let rtxn = index.read_txn()?;
        let filterable_attributes = index.filterable_attributes_rules(&rtxn)?;
        let sortable_fields = index.sortable_fields(&rtxn)?;
        let asc_desc_fields = index.asc_desc_fields(&rtxn)?;
        let distinct_field = index.distinct_field(&rtxn)?.map(|s| s.to_string());
        let is_geo_enabled = index.is_geo_enabled(&rtxn)?;
        let datastore = ThreadLocal::new();

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();

            let extractor = FacetedExtractorData {
                grenad_parameters: indexing_context.grenad_parameters,
                buckets: rayon::current_num_threads(),
                sender,
                filterable_attributes: &filterable_attributes,
                sortable_fields: &sortable_fields,
                asc_desc_fields: &asc_desc_fields,
                distinct_field: &distinct_field,
                is_geo_enabled,
            };
            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                step,
            )?;
        }

        Ok(datastore.into_iter().map(RefCell::into_inner).collect())
    }

    pub fn run_extraction_from_settings<'fid, 'indexer, 'index, 'extractor, 'a, 'b, SD, MSP>(
        settings_delta: &SD,
        documents: &'indexer DocumentsIndentifiers<'indexer>,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        fid_docid_facet_sender: &'a FieldIdDocidFacetSender<'a, 'b>,
        step: IndexingStep,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        SD: SettingsDelta + Sync,
        MSP: Fn() -> bool + Sync,
    {
        let span =
            tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
        let _entered = span.enter();

        let datastore = ThreadLocal::new();
        let extractor = FacetsSettingsExtractorsData {
            fid_docid_facet_sender,
            max_memory_by_thread: indexing_context.grenad_parameters.max_memory_by_thread(),
            buckets: rayon::current_num_threads(),
            settings_delta,
        };
        settings_change_extract(
            documents,
            &extractor,
            indexing_context,
            extractor_allocs,
            &datastore,
            step,
        )?;

        Ok(datastore.into_iter().map(RefCell::into_inner).collect())
    }

    fn extract_document_from_settings_change<SD>(
        document: DocumentIdentifiers<'_>,
        context: &DocumentContext<RefCell<BalancedCaches>>,
        settings_delta: &SD,
        fid_docid_facet_sender: &FieldIdDocidFacetSender,
    ) -> Result<()>
    where
        SD: SettingsDelta,
    {
        let mut cached_sorter = context.data.borrow_mut_or_yield();
        let mut del_add_facet_value = DelAddFacetValue::new(&context.doc_alloc);
        let new_filterable_attributes_rules = settings_delta.new_filterable_rules();
        let old_fields_ids_map = settings_delta.old_fields_ids_map();
        let docid = document.docid();

        let mut add = |fid: FieldId, meta: Metadata, depth: perm_json_p::Depth, value: &Value| {
            Self::facet_fn_with_options(
                &context.doc_alloc,
                cached_sorter.deref_mut(),
                BalancedCaches::insert_add_u32,
                &mut del_add_facet_value,
                DelAddFacetValue::insert_add,
                docid,
                fid,
                meta,
                new_filterable_attributes_rules,
                depth,
                value,
            )
        };

        let current_document = document.current(
            &context.rtxn,
            context.index,
            old_fields_ids_map.as_fields_ids_map(),
        )?;

        let old_fields_ids_map = settings_delta.old_fields_ids_map();
        let old_filterable_rules = settings_delta.old_filterable_rules();

        let new_fields_ids_map = settings_delta.new_fields_ids_map();
        let new_filterable_rules = settings_delta.new_filterable_rules();

        extract_document_facets(
            current_document,
            // TODO extract into another function
            |field_name| {
                let Some((field_id, metadata)) = old_fields_ids_map.id_with_metadata(field_name)
                else {
                    return PatternMatch::NoMatch;
                };

                let FilterableAttributesFeatures { facet_search: old_facet_search, filter } =
                    metadata.filterable_attributes_features(old_filterable_rules);
                let FilterFeatures { equality: old_equality, comparison: old_comparison } = filter;
                let old_asc_desc = metadata.asc_desc.is_some();
                let old_sortable = metadata.sortable;
                let old_distinct = metadata.distinct;

                let new_facet_search;
                let new_equality;
                let new_comparison;
                let new_asc_desc;
                let new_sortable;
                let new_distinct;
                // TODO do not duplicate this logic and put it in a function
                //      for delete_old_fid_from_facet_databases to use it
                match new_fields_ids_map.metadata(field_id) {
                    Some(metadata) => {
                        let FilterableAttributesFeatures { facet_search, filter } =
                            metadata.filterable_attributes_features(new_filterable_rules);
                        let FilterFeatures { equality, comparison } = filter;
                        new_facet_search = facet_search;
                        new_equality = equality;
                        new_comparison = comparison;
                        new_asc_desc = metadata.asc_desc.is_some();
                        new_sortable = metadata.sortable;
                        new_distinct = metadata.distinct;
                    }
                    None => {
                        // This will trigger a clean deletion from everywhere
                        new_facet_search = false;
                        new_equality = false;
                        new_comparison = false;
                        new_asc_desc = false;
                        new_sortable = false;
                        new_distinct = false;
                    }
                };

                let is_old_faceted = old_equality
                    || old_comparison
                    || old_facet_search
                    || old_asc_desc
                    || old_sortable
                    || old_distinct;

                let is_new_faceted = new_equality
                    || new_comparison
                    || new_facet_search
                    || new_asc_desc
                    || new_sortable
                    || new_distinct;

                if !is_old_faceted && is_new_faceted {
                    PatternMatch::Match
                } else {
                    // We force the system to go down the rabbit hole
                    // and avoid an early break if we return NoMatch
                    PatternMatch::Parent
                }
            },
            &mut |name| {
                new_fields_ids_map.id_with_metadata(name).ok_or_else(|| {
                    InternalError::FieldIdMapMissingEntry(FieldIdMapMissingEntry::FieldName {
                        field_name: name.to_string(),
                        process: "extract_document_facets",
                    })
                    .into()
                })
            },
            &mut add,
        )?;

        // TODO do not duplicate the content of the extract_geo_document function
        if let Some((lat_fid, lng_fid)) = settings_delta.new_geo_fields_ids() {
            if settings_delta.old_geo_fields_ids().is_none() {
                if let Some(geo_value) = current_document.geo_field()? {
                    let external_id = document.external_document_id();
                    if let Some([lat, lng]) = extract_geo_coordinates(external_id, geo_value)? {
                        let lat_meta = new_fields_ids_map.metadata(lat_fid).unwrap();
                        let lng_meta = new_fields_ids_map.metadata(lng_fid).unwrap();

                        add(lat_fid, lat_meta, perm_json_p::Depth::OnBaseKey, &lat.into())?;
                        add(lng_fid, lng_meta, perm_json_p::Depth::OnBaseKey, &lng.into())?;
                    }
                }
            }
        }

        del_add_facet_value.send_data(docid, fid_docid_facet_sender, &context.doc_alloc).unwrap();
        Ok(())
    }
}

struct FacetsSettingsExtractorsData<'a, 'b, SD> {
    fid_docid_facet_sender: &'a FieldIdDocidFacetSender<'a, 'b>,
    max_memory_by_thread: Option<usize>,
    buckets: usize,
    settings_delta: &'a SD,
}

impl<'extractor, SD: SettingsDelta + Sync> SettingsChangeExtractor<'extractor>
    for FacetsSettingsExtractorsData<'_, '_, SD>
{
    type Data = RefCell<BalancedCaches<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> crate::Result<Self::Data> {
        Ok(RefCell::new(BalancedCaches::new_in(
            self.buckets,
            self.max_memory_by_thread,
            extractor_alloc,
        )))
    }

    fn process<'doc>(
        &'doc self,
        documents: impl Iterator<Item = crate::Result<DocumentIdentifiers<'doc>>>,
        context: &'doc DocumentContext<Self::Data>,
    ) -> crate::Result<()> {
        for document in documents {
            let document = document?;
            FacetedDocidsExtractor::extract_document_from_settings_change(
                document,
                context,
                self.settings_delta,
                self.fid_docid_facet_sender,
            )?;
        }
        Ok(())
    }
}
