use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};

use charabia::normalizer::NormalizerOption;
use charabia::{Language, Normalize, StrDetection, Token};
use grenad::Sorter;
use heed::types::{Bytes, SerdeJson};
use heed::{BytesDecode, BytesEncode, RoTxn, RwTxn};

use super::fst_merger_builder::FstMergerBuilder;
use super::KvReaderDelAdd;
use crate::heed_codec::facet::FacetGroupKey;
use crate::update::del_add::{DelAdd, KvWriterDelAdd};
use crate::update::{create_sorter, MergeDeladdBtreesetString};
use crate::{
    BEU16StrCodec, FieldId, GlobalFieldsIdsMap, Index, LocalizedAttributesRule, Result,
    MAX_FACET_VALUE_LENGTH,
};

pub struct FacetSearchBuilder<'indexer> {
    registered_facets: HashMap<FieldId, usize>,
    normalized_facet_string_docids_sorter: Sorter<MergeDeladdBtreesetString>,
    global_fields_ids_map: GlobalFieldsIdsMap<'indexer>,
    localized_attributes_rules: Vec<LocalizedAttributesRule>,
    // Buffered data below
    buffer: Vec<u8>,
    localized_field_ids: HashMap<FieldId, Option<Vec<Language>>>,
}

impl<'indexer> FacetSearchBuilder<'indexer> {
    pub fn new(
        global_fields_ids_map: GlobalFieldsIdsMap<'indexer>,
        localized_attributes_rules: Vec<LocalizedAttributesRule>,
    ) -> Self {
        let registered_facets = HashMap::new();
        let normalized_facet_string_docids_sorter = create_sorter(
            grenad::SortAlgorithm::Stable,
            MergeDeladdBtreesetString,
            grenad::CompressionType::None,
            None,
            None,
            Some(0),
            true,
        );

        Self {
            registered_facets,
            normalized_facet_string_docids_sorter,
            buffer: Vec::new(),
            global_fields_ids_map,
            localized_attributes_rules,
            localized_field_ids: HashMap::new(),
        }
    }

    pub fn register_from_key(
        &mut self,
        deladd: DelAdd,
        facet_key: FacetGroupKey<&str>,
    ) -> Result<()> {
        let FacetGroupKey { field_id, level: _level, left_bound } = facet_key;

        if deladd == DelAdd::Addition {
            self.registered_facets.entry(field_id).and_modify(|count| *count += 1).or_insert(1);
        }

        let locales = self.locales(field_id);
        let hyper_normalized_value = normalize_facet_string(left_bound, locales);

        let set = BTreeSet::from_iter(std::iter::once(left_bound));

        // as the facet string is the same, we can put the deletion and addition in the same obkv.
        self.buffer.clear();
        let mut obkv = KvWriterDelAdd::new(&mut self.buffer);
        let val = SerdeJson::bytes_encode(&set).map_err(heed::Error::Encoding)?;
        obkv.insert(deladd, val)?;
        obkv.finish()?;

        let key: (u16, &str) = (field_id, hyper_normalized_value.as_ref());
        let key_bytes = BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
        self.normalized_facet_string_docids_sorter.insert(key_bytes, &self.buffer)?;

        Ok(())
    }

    fn locales(&mut self, field_id: FieldId) -> Option<&[Language]> {
        if let Entry::Vacant(e) = self.localized_field_ids.entry(field_id) {
            let Some(field_name) = self.global_fields_ids_map.name(field_id) else {
                unreachable!("Field id {field_id} not found in the global fields ids map");
            };

            let locales = self
                .localized_attributes_rules
                .iter()
                .find(|rule| rule.match_str(field_name))
                .map(|rule| rule.locales.clone());

            e.insert(locales);
        }

        self.localized_field_ids.get(&field_id).unwrap().as_deref()
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_fst")]
    pub fn merge_and_write(self, index: &Index, wtxn: &mut RwTxn, rtxn: &RoTxn) -> Result<()> {
        tracing::trace!("merge facet strings for facet search: {:?}", self.registered_facets);

        let reader = self.normalized_facet_string_docids_sorter.into_reader_cursors()?;
        let mut builder = grenad::MergerBuilder::new(MergeDeladdBtreesetString);
        builder.extend(reader);

        let database = index.facet_id_normalized_string_strings.remap_types::<Bytes, Bytes>();

        let mut merger_iter = builder.build().into_stream_merger_iter()?;
        let mut current_field_id = None;
        let mut fst;
        let mut fst_merger_builder: Option<FstMergerBuilder> = None;
        while let Some((key, deladd)) = merger_iter.next()? {
            let (field_id, normalized_facet_string) =
                BEU16StrCodec::bytes_decode(key).map_err(heed::Error::Encoding)?;

            if current_field_id != Some(field_id) {
                if let (Some(current_field_id), Some(fst_merger_builder)) =
                    (current_field_id, fst_merger_builder)
                {
                    let mmap = fst_merger_builder.build(&mut callback)?;
                    index.facet_id_string_fst.remap_data_type::<Bytes>().put(
                        wtxn,
                        &current_field_id,
                        &mmap,
                    )?;
                }

                fst = index.facet_id_string_fst.get(rtxn, &field_id)?;
                fst_merger_builder = Some(FstMergerBuilder::new(fst.as_ref())?);
                current_field_id = Some(field_id);
            }

            let previous = database.get(rtxn, key)?;
            let deladd: &KvReaderDelAdd = deladd.into();
            let del = deladd.get(DelAdd::Deletion);
            let add = deladd.get(DelAdd::Addition);

            match merge_btreesets(previous, del, add)? {
                Operation::Write(value) => {
                    match fst_merger_builder.as_mut() {
                        Some(fst_merger_builder) => {
                            fst_merger_builder.register(
                                DelAdd::Addition,
                                normalized_facet_string.as_bytes(),
                                &mut callback,
                            )?;
                        }
                        None => unreachable!(),
                    }
                    let key = (field_id, normalized_facet_string);
                    let key_bytes =
                        BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
                    database.put(wtxn, &key_bytes, &value)?;
                }
                Operation::Delete => {
                    match fst_merger_builder.as_mut() {
                        Some(fst_merger_builder) => {
                            fst_merger_builder.register(
                                DelAdd::Deletion,
                                normalized_facet_string.as_bytes(),
                                &mut callback,
                            )?;
                        }
                        None => unreachable!(),
                    }
                    let key = (field_id, normalized_facet_string);
                    let key_bytes =
                        BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
                    database.delete(wtxn, &key_bytes)?;
                }
                Operation::Ignore => (),
            }
        }

        if let (Some(field_id), Some(fst_merger_builder)) = (current_field_id, fst_merger_builder) {
            let mmap = fst_merger_builder.build(&mut callback)?;
            index.facet_id_string_fst.remap_data_type::<Bytes>().put(wtxn, &field_id, &mmap)?;
        }

        Ok(())
    }
}

fn callback(_bytes: &[u8], _deladd: DelAdd, _is_modified: bool) -> Result<()> {
    Ok(())
}

fn merge_btreesets(
    current: Option<&[u8]>,
    del: Option<&[u8]>,
    add: Option<&[u8]>,
) -> Result<Operation> {
    let mut result: BTreeSet<String> = match current {
        Some(current) => SerdeJson::bytes_decode(current).map_err(heed::Error::Encoding)?,
        None => BTreeSet::new(),
    };
    if let Some(del) = del {
        let del: BTreeSet<String> = SerdeJson::bytes_decode(del).map_err(heed::Error::Encoding)?;
        result = result.difference(&del).cloned().collect();
    }
    if let Some(add) = add {
        let add: BTreeSet<String> = SerdeJson::bytes_decode(add).map_err(heed::Error::Encoding)?;
        result.extend(add);
    }

    // TODO remove allocation
    let result = SerdeJson::bytes_encode(&result).map_err(heed::Error::Encoding)?.into_owned();
    if Some(result.as_ref()) == current {
        Ok(Operation::Ignore)
    } else if result.is_empty() {
        Ok(Operation::Delete)
    } else {
        Ok(Operation::Write(result))
    }
}

/// Normalizes the facet string and truncates it to the max length.
fn normalize_facet_string(facet_string: &str, locales: Option<&[Language]>) -> String {
    let options: NormalizerOption = NormalizerOption { lossy: true, ..Default::default() };
    let mut detection = StrDetection::new(facet_string, locales);

    let script = detection.script();
    // Detect the language of the facet string only if several locales are explicitly provided.
    let language = match locales {
        Some(&[language]) => Some(language),
        Some(multiple_locales) if multiple_locales.len() > 1 => detection.language(),
        _ => None,
    };

    let token = Token {
        lemma: std::borrow::Cow::Borrowed(facet_string),
        script,
        language,
        ..Default::default()
    };

    // truncate the facet string to the max length
    token
        .normalize(&options)
        .lemma
        .char_indices()
        .take_while(|(idx, _)| *idx < MAX_FACET_VALUE_LENGTH)
        .map(|(_, c)| c)
        .collect()
}

enum Operation {
    Write(Vec<u8>),
    Delete,
    Ignore,
}
