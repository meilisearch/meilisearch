use std::rc::Rc;

use meilisearch_types::{
    error::ResponseError,
    heed::RoTxn,
    milli::{
        self, filtered_universe, progress::Progress, Filter, FilterCondition, IndexFilter,
        IndexFilterCondition,
    },
    Index,
};
use std::collections::HashMap;

use crate::{Error, IndexScheduler, Result};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ForeignIndexUid(pub Rc<str>);

/// The maximum number of documents a foreign filter can retrieve per index
///
/// This is to avoid potential performance issues with large foreign filters.
/// If the foreign filter is retrieving too many documents, it will return an error.
const MAX_FOREIGN_FILTER_DOCIDS: u64 = 100;

impl std::borrow::Borrow<str> for ForeignIndexUid {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ForeignIndexUid {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceFieldName(pub Rc<str>);

impl AsRef<str> for SourceFieldName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceIndexUid(pub Rc<str>);

impl std::borrow::Borrow<str> for SourceIndexUid {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SourceIndexUid {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

pub type ForeignKeysPerIndex = HashMap<SourceIndexUid, Vec<(ForeignIndexUid, SourceFieldName)>>;

/// Convert a filter into an index filter by evaluating the foreign filters
///
/// this function is a wrapper around the `filters_into_index_filters`.
pub fn filter_into_index_filter<'a>(
    filter: Filter<'a>,
    index: &Index,
    rtxn: &RoTxn,
    index_scheduler: &IndexScheduler,
    progress: &Progress,
    index_uid: &str,
) -> Result<IndexFilter<'a>> {
    let foreign_keys = index
        .foreign_keys(rtxn)
        .map_err(|err| Error::from_milli(milli::Error::from(err), Some(index_uid.to_string())))?;

    let foreign_keys = foreign_keys
        .into_iter()
        .map(|fk| {
            (
                ForeignIndexUid(Rc::from(fk.foreign_index_uid)),
                SourceFieldName(Rc::from(fk.field_name)),
            )
        })
        .collect();
    let source_index_uid = SourceIndexUid(Rc::from(index_uid));
    let foreign_keys_per_index =
        Some((source_index_uid.clone(), foreign_keys)).into_iter().collect();

    filters_into_index_filters(
        vec![(source_index_uid.clone(), Some(filter))],
        &foreign_keys_per_index,
        index_scheduler,
        progress,
    )
    .map(|mut filters| {
        // there is exactly one filter that can't be None
        filters.pop().unwrap().unwrap()
    })
}

/// Convert a vector of filters into a vector of index filters by evaluating the foreign filters
///
/// This function will open each foreign index once and process the filters.
pub fn filters_into_index_filters<'a>(
    filters: Vec<(SourceIndexUid, Option<Filter<'a>>)>,
    foreign_keys_per_index: &ForeignKeysPerIndex,
    index_scheduler: &IndexScheduler,
    progress: &Progress,
) -> Result<Vec<Option<IndexFilter<'a>>>> {
    // list all the foreign filters and check their validity
    let mut foreign_filters = Vec::new();
    for (index_uid, filter) in filters.iter() {
        let Some(filter) = filter else { continue };
        for foreign_filter in filter.condition.list_foreign_filters() {
            let FilterCondition::Foreign { fid, op } = foreign_filter.clone() else {
                unreachable!()
            };

            // get the foreign keys settings for the index
            let foreign_keys = foreign_keys_per_index.get(index_uid).ok_or(Error::Milli {
                error: milli::Error::UserError(milli::UserError::InvalidFilter(
                    "Index does not have foreign keys".to_string(),
                )),
                index_uid: Some(index_uid.as_ref().to_string()),
            })?;

            // get the foreign index uid for the foreign key
            let (foreign_index_uid, _) = foreign_keys
                .iter()
                .find(|(_f_index, s_fname)| s_fname.as_ref() == fid.fragment())
                .ok_or(Error::Milli {
                    error: milli::Error::UserError(milli::UserError::InvalidFilter(format!(
                        "Field `{}` is not a foreign key",
                        fid.fragment()
                    ))),
                    index_uid: Some(index_uid.as_ref().to_string()),
                })?;

            // convert inner foreign filter into an index filter, throw an error if there is a nested foreign filter
            let index_filter = IndexFilter::from(condition_to_index_condition(*op, &mut |_| {
                Err(Error::Milli {
                    error: milli::Error::UserError(milli::UserError::InvalidFilter(
                        "Nested foreign filters are not supported".to_string(),
                    )),
                    index_uid: Some(index_uid.as_ref().to_string()),
                })
            })?);

            // index_uid and foreign_index_uid are RCs and can be cloned safely
            foreign_filters.push((
                index_uid.clone(),
                foreign_index_uid.clone(),
                fid,
                Some(index_filter),
                None,
            ));
        }
    }

    // group the foreign filters by foreign index
    let mut filters_per_foreign_index: HashMap<ForeignIndexUid, Vec<usize>> = HashMap::new();
    for (i, (_, foreign_index_uid, _, _, _)) in foreign_filters.iter().enumerate() {
        filters_per_foreign_index.entry(foreign_index_uid.clone()).or_default().push(i);
    }

    // open each foreign index once and process the filters
    // TODO: do remote document filtering here (linear: EXP-1027)
    // local
    for (foreign_index_uid, filter_indices) in filters_per_foreign_index.iter() {
        let foreign_index = index_scheduler.index(foreign_index_uid.as_ref())?;
        let foreign_rtxn = foreign_index.read_txn()?;
        let foreign_external_docids = foreign_index.external_documents_ids();

        // Gather the internal docids for each filter
        let mut filters_internal_docids = Vec::new();
        for filter_index in filter_indices.iter() {
            let (_, foreign_index_uid, _, index_filter, _) = &foreign_filters[*filter_index];

            // filter the foreign index
            let docids = filtered_universe(&foreign_index, &foreign_rtxn, index_filter, progress)
                .map_err(|err| {
                Error::from_milli(err, Some(foreign_index_uid.as_ref().to_string()))
            })?;

            filters_internal_docids.push(docids);
        }

        // Build the In filter for each filter converting the internal docids to external docids
        //
        // Fetch all the external docids once
        let docids_to_fetch = filters_internal_docids
            .iter()
            .fold(roaring::RoaringBitmap::new(), |bitmap, docids| bitmap | docids);
        if docids_to_fetch.len() > MAX_FOREIGN_FILTER_DOCIDS {
            return Err(Error::Milli {
                error: milli::Error::UserError(milli::UserError::InvalidFilter(
                    format!("Foreign filter is retrieving too many documents, foreign filters can't retrieve more than {MAX_FOREIGN_FILTER_DOCIDS} documents per index"),
                )),
                index_uid: Some(foreign_index_uid.as_ref().to_string()),
            });
        }
        let mut internal_to_external_docids = HashMap::new();
        // TODO: optimize DB scan (linear: EXP-1117)
        for result in foreign_external_docids.iter(&foreign_rtxn)? {
            let (external, internal) = result?;
            if docids_to_fetch.contains(internal) {
                internal_to_external_docids.insert(internal, external.to_string());
            }
        }

        // Build the In filter for each filter
        for (filter_index, docids) in filter_indices.iter().zip(filters_internal_docids.into_iter())
        {
            let mut inner = Vec::new();
            for internal in docids.iter() {
                if let Some(external) = internal_to_external_docids.get(&internal) {
                    inner.push(external.to_string().into());
                }
            }

            foreign_filters[*filter_index].4 = Some(inner);
        }
    }

    let mut in_iter = foreign_filters.into_iter();
    filters
        .into_iter()
        .map(|(_index_uid, filter)| {
            let Some(filter) = filter else { return Ok(None) };
            condition_to_index_condition(filter.condition, &mut |_| {
                let Some((_, _, fid, _, Some(els))) = in_iter.next() else { unreachable!() };
                Ok(IndexFilterCondition::In { fid, els })
            })
            .map(|condition| Some(IndexFilter { condition }))
        })
        .collect()
}

/// Convert a vector of filters into a vector of index filters without evaluating the foreign filters
///
/// This function will not open any foreign index but will panic if a foreign filter is encountered.
pub fn filters_into_index_filters_unchecked<'a>(
    filters: Vec<Option<Filter<'a>>>,
) -> Result<Vec<Option<IndexFilter<'a>>>> {
    filters
        .into_iter()
        .map(|filter| {
            let Some(filter) = filter else { return Ok(None) };
            condition_to_index_condition(filter.condition, &mut |_| unreachable!())
                .map(|condition| Some(IndexFilter { condition }))
        })
        .collect::<Result<_>>()
}

fn condition_to_index_condition<'a, F>(
    filter: FilterCondition<'a>,
    foreign_filter: &mut F,
) -> Result<IndexFilterCondition<'a>>
where
    F: FnMut(FilterCondition<'a>) -> Result<IndexFilterCondition<'a>>,
{
    match filter {
        FilterCondition::Not(filter) => condition_to_index_condition(*filter, foreign_filter)
            .map(Box::new)
            .map(IndexFilterCondition::Not),
        FilterCondition::Condition { fid, op } => Ok(IndexFilterCondition::Condition { fid, op }),
        FilterCondition::In { fid, els } => Ok(IndexFilterCondition::In { fid, els }),
        FilterCondition::Or(filters) => filters
            .into_iter()
            .map(|filter| condition_to_index_condition(filter, foreign_filter))
            .collect::<Result<_>>()
            .map(IndexFilterCondition::Or),

        FilterCondition::And(filters) => filters
            .into_iter()
            .map(|filter| condition_to_index_condition(filter, foreign_filter))
            .collect::<Result<_>>()
            .map(IndexFilterCondition::And),

        FilterCondition::VectorExists { fid, embedder, filter } => {
            Ok(IndexFilterCondition::VectorExists { fid, embedder, filter })
        }
        FilterCondition::GeoLowerThan { point, radius, resolution } => {
            Ok(IndexFilterCondition::GeoLowerThan { point, radius, resolution })
        }
        FilterCondition::GeoBoundingBox { top_right_point, bottom_left_point } => {
            Ok(IndexFilterCondition::GeoBoundingBox { top_right_point, bottom_left_point })
        }
        FilterCondition::GeoPolygon { points } => Ok(IndexFilterCondition::GeoPolygon { points }),
        FilterCondition::Foreign { .. } => foreign_filter(filter),
    }
}

/// Retrieve the foreign keys settings for a list of indexes
///
/// This function will open each index once and retrieve the foreign keys settings.
pub fn retrieve_foreign_keys_settings<'a>(
    index_scheduler: &IndexScheduler,
    index_uids: impl IntoIterator<Item = &'a SourceIndexUid>,
) -> Result<ForeignKeysPerIndex, ResponseError> {
    let mut foreign_keys_settings = HashMap::new();
    for index_uid in index_uids.into_iter() {
        if foreign_keys_settings.contains_key(index_uid.as_ref()) {
            continue;
        }

        let index = index_scheduler.index(index_uid.as_ref())?;
        let rtxn = index.read_txn()?;
        let foreign_keys = index
            .foreign_keys(&rtxn)?
            .into_iter()
            .map(|fk| {
                (
                    ForeignIndexUid(Rc::from(fk.foreign_index_uid)),
                    SourceFieldName(Rc::from(fk.field_name)),
                )
            })
            .collect();
        foreign_keys_settings.insert(index_uid.clone(), foreign_keys);
    }
    Ok(foreign_keys_settings)
}
