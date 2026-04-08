use std::borrow::Cow;
use std::fmt::{Debug, Write as FmtWrite};
use std::ops::Bound::{self, Excluded, Included, Unbounded};

pub use filter_parser::Condition;
use filter_parser::{IndexFilterCondition, VectorFilter};
use heed::types::LazyDecode;
use heed::BytesEncode;
use memchr::memmem::Finder;
use roaring::{MultiOps, RoaringBitmap};

use super::facet_range_search;
use crate::constants::{
    RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME, RESERVED_VECTORS_FIELD_NAME,
};
use crate::error::{Error, UserError};
use crate::filterable_attributes_rules::{filtered_matching_patterns, matching_features};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::index::db_name::FACET_ID_STRING_DOCIDS;
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::facet::filter::{FilterError, MAX_FILTER_DEPTH};
use crate::search::facet::BadGeoError;
use crate::{
    distance_between_two_points, lat_lng_to_xyz, FieldId, FieldsIdsMap,
    FilterableAttributesFeatures, FilterableAttributesRule, Index, InternalError, Result,
    SerializationError, SHARD_FIELD,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexFilter<'a> {
    pub condition: IndexFilterCondition<'a>,
}

impl<'a> From<IndexFilterCondition<'a>> for IndexFilter<'a> {
    fn from(condition: IndexFilterCondition<'a>) -> Self {
        IndexFilter { condition }
    }
}

impl IndexFilter<'_> {
    pub fn into_owned(self) -> IndexFilter<'static> {
        IndexFilter { condition: self.condition.into_owned() }
    }
}

impl<'a> IndexFilter<'a> {
    pub fn evaluate(&self, rtxn: &heed::RoTxn<'_>, index: &Index) -> Result<RoaringBitmap> {
        // to avoid doing this for each recursive call we're going to do it ONCE ahead of time
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let filterable_attributes_rules = index.filterable_attributes_rules(rtxn)?;

        for fid in self.condition.fids(MAX_FILTER_DEPTH) {
            let attribute = fid.fragment();
            if matching_features(attribute, &filterable_attributes_rules)
                .is_some_and(|(_, features)| features.is_filterable())
                || attribute == RESERVED_VECTORS_FIELD_NAME
                || attribute == SHARD_FIELD
            {
                continue;
            }

            // If the field is not filterable, return an error
            return Err(fid.to_external_error(FilterError::AttributeNotFilterable {
                attribute,
                filterable_patterns: filtered_matching_patterns(
                    &filterable_attributes_rules,
                    &|features| features.is_filterable(),
                ),
            }))?;
        }

        self.inner_evaluate(rtxn, index, &fields_ids_map, &filterable_attributes_rules, None)
    }

    fn evaluate_operator(
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        field_id: FieldId,
        universe_hint: Option<&RoaringBitmap>,
        operator: &Condition<'a>,
        features: &FilterableAttributesFeatures,
        rule_index: usize,
    ) -> Result<RoaringBitmap> {
        let numbers_db = index.facet_id_f64_docids;
        let strings_db = index.facet_id_string_docids;

        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.

        let (number_bounds, (left_str, right_str)) = match operator {
            // return an error if the filter is not allowed for this field
            Condition::GreaterThan(_)
            | Condition::GreaterThanOrEqual(_)
            | Condition::LowerThan(_)
            | Condition::LowerThanOrEqual(_)
            | Condition::Between { .. }
                if !features.is_filterable_comparison() =>
            {
                return Err(generate_filter_error(
                    rtxn, index, field_id, operator, features, rule_index,
                ));
            }
            Condition::Empty if !features.is_filterable_empty() => {
                return Err(generate_filter_error(
                    rtxn, index, field_id, operator, features, rule_index,
                ));
            }
            Condition::Null if !features.is_filterable_null() => {
                return Err(generate_filter_error(
                    rtxn, index, field_id, operator, features, rule_index,
                ));
            }
            Condition::Exists if !features.is_filterable_exists() => {
                return Err(generate_filter_error(
                    rtxn, index, field_id, operator, features, rule_index,
                ));
            }
            Condition::Equal(_) | Condition::NotEqual(_) if !features.is_filterable_equality() => {
                return Err(generate_filter_error(
                    rtxn, index, field_id, operator, features, rule_index,
                ));
            }
            Condition::GreaterThan(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Excluded(number), Included(f64::MAX)));
                let str_bounds = (Excluded(val.fragment()), Unbounded);
                (number_bounds, str_bounds)
            }
            Condition::GreaterThanOrEqual(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Included(number), Included(f64::MAX)));
                let str_bounds = (Included(val.fragment()), Unbounded);
                (number_bounds, str_bounds)
            }
            Condition::LowerThan(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Included(f64::MIN), Excluded(number)));
                let str_bounds = (Unbounded, Excluded(val.fragment()));
                (number_bounds, str_bounds)
            }
            Condition::LowerThanOrEqual(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Included(f64::MIN), Included(number)));
                let str_bounds = (Unbounded, Included(val.fragment()));
                (number_bounds, str_bounds)
            }
            Condition::Between { from, to } => {
                let from_number = from.parse_finite_float().ok();
                let to_number = to.parse_finite_float().ok();

                let number_bounds =
                    from_number.zip(to_number).map(|(from, to)| (Included(from), Included(to)));
                let str_bounds = (Included(from.fragment()), Included(to.fragment()));
                (number_bounds, str_bounds)
            }
            Condition::Null => {
                let is_null = index.null_faceted_documents_ids(rtxn, field_id)?;
                return Ok(is_null);
            }
            Condition::Empty => {
                let is_empty = index.empty_faceted_documents_ids(rtxn, field_id)?;
                return Ok(is_empty);
            }
            Condition::Exists => {
                let exist = index.exists_faceted_documents_ids(rtxn, field_id)?;
                return Ok(exist);
            }
            Condition::Equal(val) => {
                let string_docids = strings_db
                    .get(
                        rtxn,
                        &FacetGroupKey {
                            field_id,
                            level: 0,
                            left_bound: &crate::normalize_facet(val.fragment()),
                        },
                    )?
                    .map(|v| v.bitmap)
                    .unwrap_or_default();
                let number = val.parse_finite_float().ok();
                let number_docids = match number {
                    Some(n) => numbers_db
                        .get(rtxn, &FacetGroupKey { field_id, level: 0, left_bound: n })?
                        .map(|v| v.bitmap)
                        .unwrap_or_default(),
                    None => RoaringBitmap::new(),
                };
                return Ok(string_docids | number_docids);
            }
            Condition::NotEqual(val) => {
                let operator = Condition::Equal(val.clone());
                let docids = Self::evaluate_operator(
                    rtxn, index, field_id, None, &operator, features, rule_index,
                )?;
                let all_ids = index.documents_ids(rtxn)?;
                return Ok(all_ids - docids);
            }
            Condition::Contains { keyword: _, word } => {
                let value = crate::normalize_facet(word.fragment());
                let finder = Finder::new(&value);
                let base = FacetGroupKey { field_id, level: 0, left_bound: "" };
                let docids = strings_db
                    .prefix_iter(rtxn, &base)?
                    .remap_data_type::<LazyDecode<FacetGroupValueCodec>>()
                    .filter_map(|result| -> Option<Result<RoaringBitmap>> {
                        match result {
                            Ok((FacetGroupKey { left_bound, .. }, lazy_group_value)) => {
                                if finder.find(left_bound.as_bytes()).is_some() {
                                    Some(lazy_group_value.decode().map(|gv| gv.bitmap).map_err(
                                        |_| {
                                            InternalError::from(SerializationError::Decoding {
                                                db_name: Some(FACET_ID_STRING_DOCIDS),
                                            })
                                            .into()
                                        },
                                    ))
                                } else {
                                    None
                                }
                            }
                            Err(_e) => {
                                Some(Err(InternalError::from(SerializationError::Decoding {
                                    db_name: Some(FACET_ID_STRING_DOCIDS),
                                })
                                .into()))
                            }
                        }
                    })
                    .union()?;

                return Ok(docids);
            }
            Condition::StartsWith { keyword: _, word } => {
                // The idea here is that "STARTS WITH baba" is the same as "baba <= value < babb".
                // We just incremented the last letter to find the upper bound.
                // The upper bound may not be valid utf8, but lmdb doesn't care as it works over bytes.

                let value = crate::normalize_facet(word.fragment());
                let mut value2 = value.as_bytes().to_owned();

                let last = match value2.last_mut() {
                    Some(last) => last,
                    None => {
                        // The prefix is empty, so all documents that have the field will match.
                        return index
                            .exists_faceted_documents_ids(rtxn, field_id)
                            .map_err(|e| e.into());
                    }
                };

                if *last == u8::MAX {
                    // u8::MAX is a forbidden UTF-8 byte, we're guaranteed it cannot be sent through a filter to meilisearch, but just in case, we're going to return something
                    tracing::warn!(
                        "Found non utf-8 character in filter. That shouldn't be possible"
                    );
                    return Ok(RoaringBitmap::new());
                }
                *last += 1;

                // This is very similar to `heed::Bytes` but its `EItem` is `&[u8]` instead of `[u8]`
                struct BytesRef;
                impl<'a> BytesEncode<'a> for BytesRef {
                    type EItem = &'a [u8];

                    fn bytes_encode(
                        item: &'a Self::EItem,
                    ) -> std::result::Result<Cow<'a, [u8]>, heed::BoxedError> {
                        Ok(Cow::Borrowed(item))
                    }
                }

                let mut docids = RoaringBitmap::new();
                let bytes_db =
                    index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<BytesRef>>();
                find_docids_of_facet_within_bounds::<BytesRef>(
                    rtxn,
                    bytes_db,
                    field_id,
                    &Included(value.as_bytes()),
                    &Excluded(value2.as_slice()),
                    universe_hint,
                    &mut docids,
                )?;

                return Ok(docids);
            }
        };

        let mut output = RoaringBitmap::new();

        if let Some((left_number, right_number)) = number_bounds {
            Self::explore_facet_levels(
                rtxn,
                numbers_db,
                field_id,
                &left_number,
                &right_number,
                universe_hint,
                &mut output,
            )?;
        }

        Self::explore_facet_levels(
            rtxn,
            strings_db,
            field_id,
            &left_str,
            &right_str,
            universe_hint,
            &mut output,
        )?;

        Ok(output)
    }

    fn evaluate_shard_operator(
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        universe_hint: Option<&RoaringBitmap>,
        operator: &Condition<'a>,
    ) -> Result<RoaringBitmap> {
        Ok(match operator {
            Condition::Equal(token) => {
                let shard_name = token.fragment();
                let shard_docids = index.shard_docids();
                let docids = if let Some(universe_hint) = universe_hint {
                    shard_docids.docids_intersection(rtxn, shard_name, universe_hint)?
                } else {
                    shard_docids.docids(rtxn, shard_name)?
                };
                docids.ok_or_else(|| {
                    Error::UserError(UserError::FilterShardNotExist {
                        shard: shard_name.to_owned(),
                    })
                })?
            }
            Condition::NotEqual(token) => {
                let to_remove = Self::evaluate_shard_operator(
                    rtxn,
                    index,
                    universe_hint,
                    &Condition::Equal(token.clone()),
                )?;

                match universe_hint {
                    Some(universe_hint) => universe_hint - to_remove,
                    None => index.documents_ids(rtxn)? - to_remove,
                }
            }
            unsupported => {
                return Err(Error::UserError(UserError::FilterShardOperatorNotAllowed {
                    operator: unsupported.operator().to_string(),
                }))
            }
        })
    }

    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_levels<'data, BoundCodec>(
        rtxn: &'data heed::RoTxn<'data>,
        db: heed::Database<FacetGroupKeyCodec<BoundCodec>, FacetGroupValueCodec>,
        field_id: FieldId,
        left: &'data Bound<<BoundCodec as heed::BytesEncode<'data>>::EItem>,
        right: &'data Bound<<BoundCodec as heed::BytesEncode<'data>>::EItem>,
        universe_hint: Option<&RoaringBitmap>,
        output: &mut RoaringBitmap,
    ) -> Result<()>
    where
        BoundCodec: for<'b> BytesEncode<'b>,
        for<'b> <BoundCodec as BytesEncode<'b>>::EItem: Sized + PartialOrd,
    {
        match (left, right) {
            // lower TO upper when lower > upper must return no result
            (Included(l), Included(r)) if l > r => return Ok(()),
            (Included(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Included(r)) if l >= r => return Ok(()),
            (_, _) => (),
        }
        facet_range_search::find_docids_of_facet_within_bounds::<BoundCodec>(
            rtxn,
            db,
            field_id,
            left,
            right,
            universe_hint,
            output,
        )?;

        Ok(())
    }

    fn inner_evaluate(
        &self,
        rtxn: &heed::RoTxn<'_>,
        index: &Index,
        field_ids_map: &FieldsIdsMap,
        filterable_attribute_rules: &[FilterableAttributesRule],
        universe_hint: Option<&RoaringBitmap>,
    ) -> Result<RoaringBitmap> {
        if universe_hint.is_some_and(|u| u.is_empty()) {
            return Ok(RoaringBitmap::new());
        }

        match &self.condition {
            IndexFilterCondition::Not(f) => {
                let selected = Self::inner_evaluate(
                    &(f.as_ref().clone()).into(),
                    rtxn,
                    index,
                    field_ids_map,
                    filterable_attribute_rules,
                    universe_hint,
                )?;
                match universe_hint {
                    Some(universe_hint) => Ok(universe_hint - selected),
                    None => {
                        let all_ids = index.documents_ids(rtxn)?;
                        Ok(all_ids - selected)
                    }
                }
            }
            IndexFilterCondition::In { fid, els } if fid.fragment() == SHARD_FIELD => els
                .iter()
                .map(|el| Condition::Equal(el.clone()))
                .map(|op| Self::evaluate_shard_operator(rtxn, index, universe_hint, &op))
                .union(),
            IndexFilterCondition::In { fid, els } => {
                let Some(field_id) = field_ids_map.id(fid.fragment()) else {
                    return Ok(RoaringBitmap::new());
                };
                let Some((rule_index, features)) =
                    matching_features(fid.fragment(), filterable_attribute_rules)
                else {
                    return Ok(RoaringBitmap::new());
                };

                els.iter()
                    .map(|el| Condition::Equal(el.clone()))
                    .map(|op| {
                        Self::evaluate_operator(
                            rtxn,
                            index,
                            field_id,
                            universe_hint,
                            &op,
                            &features,
                            rule_index,
                        )
                    })
                    .union()
            }
            IndexFilterCondition::Condition { fid, op } if fid.fragment() == SHARD_FIELD => {
                Self::evaluate_shard_operator(rtxn, index, universe_hint, op)
            }
            IndexFilterCondition::Condition { fid, op } => {
                let value = fid.fragment();
                let Some(field_id) = field_ids_map.id(value) else {
                    return Ok(RoaringBitmap::new());
                };
                let Some((rule_index, features)) =
                    matching_features(fid.fragment(), filterable_attribute_rules)
                else {
                    return Ok(RoaringBitmap::new());
                };

                Self::evaluate_operator(
                    rtxn,
                    index,
                    field_id,
                    universe_hint,
                    op,
                    &features,
                    rule_index,
                )
            }
            IndexFilterCondition::Or(subfilters) => subfilters
                .iter()
                .cloned()
                .map(|f| {
                    Self::inner_evaluate(
                        &f.into(),
                        rtxn,
                        index,
                        field_ids_map,
                        filterable_attribute_rules,
                        universe_hint,
                    )
                })
                .union(),
            IndexFilterCondition::And(subfilters) => {
                let mut subfilters_iter = subfilters.iter();
                let Some(first_subfilter) = subfilters_iter.next() else {
                    return Ok(RoaringBitmap::new());
                };

                let mut bitmap = Self::inner_evaluate(
                    &(first_subfilter.clone()).into(),
                    rtxn,
                    index,
                    field_ids_map,
                    filterable_attribute_rules,
                    universe_hint,
                )?;
                for f in subfilters_iter {
                    if bitmap.is_empty() {
                        return Ok(bitmap);
                    }
                    // TODO We are doing the intersections two times,
                    //      it could be more efficient
                    //      Can't I just replace this `&=` by an `=`?
                    bitmap &= Self::inner_evaluate(
                        &(f.clone()).into(),
                        rtxn,
                        index,
                        field_ids_map,
                        filterable_attribute_rules,
                        Some(&bitmap),
                    )?;
                }
                Ok(bitmap)
            }
            IndexFilterCondition::VectorExists { fid: _, embedder, filter } => {
                super::vector::evaluate(rtxn, index, universe_hint, embedder.clone(), filter)
            }
            IndexFilterCondition::GeoLowerThan { point, radius, resolution: res_token } => {
                let base_point: [f64; 2] =
                    [point[0].parse_finite_float()?, point[1].parse_finite_float()?];
                if !(-90.0..=90.0).contains(&base_point[0]) {
                    return Err(point[0].to_external_error(BadGeoError::Lat(base_point[0])))?;
                }
                if !(-180.0..=180.0).contains(&base_point[1]) {
                    return Err(point[1].to_external_error(BadGeoError::Lng(base_point[1])))?;
                }
                let radius = radius.parse_finite_float()?;
                let mut resolution = 125;
                if let Some(res_token) = res_token {
                    resolution = res_token.parse_finite_float()? as usize;
                    if !(3..=1000).contains(&resolution) {
                        return Err(
                            res_token.to_external_error(BadGeoError::InvalidResolution(resolution))
                        )?;
                    }
                }

                let mut r1 = None;
                if index.is_geo_filtering_enabled(rtxn)? {
                    let rtree = match index.geo_rtree(rtxn)? {
                        Some(rtree) => rtree,
                        None => return Ok(RoaringBitmap::new()),
                    };

                    let xyz_base_point = lat_lng_to_xyz(&base_point);

                    let result = rtree
                        .nearest_neighbor_iter(&xyz_base_point)
                        .take_while(|point| {
                            distance_between_two_points(&base_point, &point.data.1)
                                <= radius + f64::EPSILON
                        })
                        .map(|point| point.data.0)
                        .collect();
                    r1 = Some(result);
                }

                let mut r2 = None;
                if index.is_geojson_filtering_enabled(rtxn)? {
                    let point = geo_types::Point::new(base_point[1], base_point[0]);

                    let result = index.cellulite.in_circle(rtxn, point, radius, resolution)?;

                    r2 = Some(RoaringBitmap::from_iter(result)); // TODO: Remove once we update roaring in meilisearch
                }

                match (r1, r2) {
                    (Some(r1), Some(r2)) => Ok(r1 | r2),
                    (Some(r1), None) => Ok(r1),
                    (None, Some(r2)) => Ok(r2),
                    (None, None) => {
                        Err(point[0].to_external_error(FilterError::AttributeNotFilterable {
                            attribute: &format!(
                                "{RESERVED_GEO_FIELD_NAME}/{RESERVED_GEOJSON_FIELD_NAME}"
                            ),
                            filterable_patterns: filtered_matching_patterns(
                                filterable_attribute_rules,
                                &|features| features.is_filterable(),
                            ),
                        }))?
                    }
                }
            }
            IndexFilterCondition::GeoBoundingBox { top_right_point, bottom_left_point } => {
                let top_right: [f64; 2] = [
                    top_right_point[0].parse_finite_float()?,
                    top_right_point[1].parse_finite_float()?,
                ];
                let bottom_left: [f64; 2] = [
                    bottom_left_point[0].parse_finite_float()?,
                    bottom_left_point[1].parse_finite_float()?,
                ];
                if !(-90.0..=90.0).contains(&top_right[0]) {
                    return Err(
                        top_right_point[0].to_external_error(BadGeoError::Lat(top_right[0]))
                    )?;
                }
                if !(-180.0..=180.0).contains(&top_right[1]) {
                    return Err(
                        top_right_point[1].to_external_error(BadGeoError::Lng(top_right[1]))
                    )?;
                }
                if !(-90.0..=90.0).contains(&bottom_left[0]) {
                    return Err(
                        bottom_left_point[0].to_external_error(BadGeoError::Lat(bottom_left[0]))
                    )?;
                }
                if !(-180.0..=180.0).contains(&bottom_left[1]) {
                    return Err(
                        bottom_left_point[1].to_external_error(BadGeoError::Lng(bottom_left[1]))
                    )?;
                }
                if top_right[0] < bottom_left[0] {
                    return Err(bottom_left_point[1].to_external_error(
                        BadGeoError::BoundingBoxTopIsBelowBottom(top_right[0], bottom_left[0]),
                    ))?;
                }

                let mut r1 = None;
                if index.is_geo_filtering_enabled(rtxn)? {
                    // Instead of writing a custom `GeoBoundingBox` filter we're simply going to re-use the range
                    // filter to create the following filter;
                    // `_geo.lat {top_right[0]} TO {bottom_left[0]} AND _geo.lng {top_right[1]} TO {bottom_left[1]}`
                    // As we can see, we need to use a bunch of tokens that don't exist in the original filter,
                    // thus we're going to create tokens that point to a random span but contain our text.

                    let geo_lat_token = top_right_point[0]
                        .clone()
                        .with_modified_fragment(Some("_geo.lat".to_string()));

                    let condition_lat = IndexFilterCondition::Condition {
                        fid: geo_lat_token,
                        op: Condition::Between {
                            from: bottom_left_point[0].clone(),
                            to: top_right_point[0].clone(),
                        },
                    };

                    let selected_lat = IndexFilter { condition: condition_lat }.inner_evaluate(
                        rtxn,
                        index,
                        field_ids_map,
                        filterable_attribute_rules,
                        universe_hint,
                    )?;

                    let geo_lng_token = top_right_point[1]
                        .clone()
                        .with_modified_fragment(Some("_geo.lng".to_string()));

                    let selected_lng = if top_right[1] < bottom_left[1] {
                        // In this case the bounding box is wrapping around the earth (going from 180 to -180).
                        // We need to update the lng part of the filter from;
                        // `_geo.lng {top_right[1]} TO {bottom_left[1]}` to
                        // `_geo.lng {bottom_left[1]} TO 180 AND _geo.lng -180 TO {top_right[1]}`

                        // TODO: Shouldn't it be `bottom_left_point[1]` instead of `top_right_point[1]`?
                        let min_lng_token = top_right_point[1]
                            .clone()
                            .with_modified_fragment(Some("-180.0".to_string()));
                        let max_lng_token = top_right_point[1]
                            .clone()
                            .with_modified_fragment(Some("180.0".to_string()));

                        let condition_left = IndexFilterCondition::Condition {
                            fid: geo_lng_token.clone(),
                            op: Condition::Between {
                                from: bottom_left_point[1].clone(),
                                to: max_lng_token,
                            },
                        };
                        let left = IndexFilter { condition: condition_left }.inner_evaluate(
                            rtxn,
                            index,
                            field_ids_map,
                            filterable_attribute_rules,
                            universe_hint,
                        )?;

                        let condition_right = IndexFilterCondition::Condition {
                            fid: geo_lng_token,
                            op: Condition::Between {
                                from: min_lng_token,
                                to: top_right_point[1].clone(),
                            },
                        };
                        let right = IndexFilter { condition: condition_right }.inner_evaluate(
                            rtxn,
                            index,
                            field_ids_map,
                            filterable_attribute_rules,
                            universe_hint,
                        )?;

                        left | right
                    } else {
                        let condition_lng = IndexFilterCondition::Condition {
                            fid: geo_lng_token,
                            op: Condition::Between {
                                from: bottom_left_point[1].clone(),
                                to: top_right_point[1].clone(),
                            },
                        };
                        IndexFilter { condition: condition_lng }.inner_evaluate(
                            rtxn,
                            index,
                            field_ids_map,
                            filterable_attribute_rules,
                            universe_hint,
                        )?
                    };

                    r1 = Some(selected_lat & selected_lng);
                }

                let mut r2 = None;
                if index.is_geojson_filtering_enabled(rtxn)? {
                    let polygon = geo_types::Polygon::new(
                        geo_types::LineString(vec![
                            geo_types::Coord { x: top_right[1], y: top_right[0] },
                            geo_types::Coord { x: bottom_left[1], y: top_right[0] },
                            geo_types::Coord { x: bottom_left[1], y: bottom_left[0] },
                            geo_types::Coord { x: top_right[1], y: bottom_left[0] },
                        ]),
                        Vec::new(),
                    );

                    let result = index.cellulite.in_shape(rtxn, &polygon)?;

                    r2 = Some(RoaringBitmap::from_iter(result)); // TODO: Remove once we update roaring in meilisearch
                }

                match (r1, r2) {
                    (Some(r1), Some(r2)) => Ok(r1 | r2),
                    (Some(r1), None) => Ok(r1),
                    (None, Some(r2)) => Ok(r2),
                    (None, None) => Err(top_right_point[0].to_external_error(
                        FilterError::AttributeNotFilterable {
                            attribute: &format!(
                                "{RESERVED_GEO_FIELD_NAME}/{RESERVED_GEOJSON_FIELD_NAME}"
                            ),
                            filterable_patterns: filtered_matching_patterns(
                                filterable_attribute_rules,
                                &|features| features.is_filterable(),
                            ),
                        },
                    ))?,
                }
            }
            IndexFilterCondition::GeoPolygon { points } => {
                if !index.is_geojson_filtering_enabled(rtxn)? {
                    return Err(points[0][0].to_external_error(
                        FilterError::AttributeNotFilterable {
                            attribute: RESERVED_GEOJSON_FIELD_NAME,
                            filterable_patterns: filtered_matching_patterns(
                                filterable_attribute_rules,
                                &|features| features.is_filterable(),
                            ),
                        },
                    ))?;
                }

                let mut coords = Vec::new();
                for [lat_token, lng_token] in points {
                    let lat = lat_token.parse_finite_float()?;
                    let lng = lng_token.parse_finite_float()?;
                    if !(-90.0..=90.0).contains(&lat) {
                        return Err(lat_token.to_external_error(BadGeoError::Lat(lat)))?;
                    }
                    if !(-180.0..=180.0).contains(&lng) {
                        return Err(lng_token.to_external_error(BadGeoError::Lng(lng)))?;
                    }
                    coords.push(geo_types::Coord { x: lng, y: lat });
                }

                let polygon = geo_types::Polygon::new(geo_types::LineString(coords), Vec::new());
                let result = index.cellulite.in_shape(rtxn, &polygon)?;

                let result = roaring::RoaringBitmap::from_iter(result); // TODO: Remove once we update roaring in meilisearch

                Ok(result)
            }
        }
    }
}

fn generate_filter_error(
    rtxn: &heed::RoTxn<'_>,
    index: &Index,
    field_id: FieldId,
    operator: &Condition<'_>,
    features: &FilterableAttributesFeatures,
    rule_index: usize,
) -> Error {
    match index.fields_ids_map(rtxn) {
        Ok(fields_ids_map) => {
            let field = fields_ids_map.name(field_id).unwrap_or_default();
            Error::UserError(UserError::FilterOperatorNotAllowed {
                field: field.to_string(),
                allowed_operators: features.allowed_filter_operators(),
                operator: operator.operator().to_string(),
                rule_index,
            })
        }
        Err(e) => e.into(),
    }
}

pub fn serialize_index_filter_to_filter_string(filter: &IndexFilter<'_>) -> Result<String> {
    let mut s = String::new();
    serialize_index_filter_condition(&mut s, &filter.condition)
        .map_err(|_| SerializationError::FailedToSerializeFilter)?;
    Ok(s)
}

fn serialize_index_filter_condition(
    f: &mut impl FmtWrite,
    condition: &IndexFilterCondition<'_>,
) -> std::fmt::Result {
    match condition {
        IndexFilterCondition::Not(filter) => {
            write!(f, "NOT (")?;
            serialize_index_filter_condition(f, filter)?;
            write!(f, ")")?;
        }
        IndexFilterCondition::Condition { fid, op } => {
            write!(f, "'{}' ", fid.fragment())?;
            serialize_condition(f, op)?;
        }
        IndexFilterCondition::In { fid, els } => {
            write!(f, "'{}' IN [", fid.fragment())?;
            for (i, el) in els.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "'{}'", el.fragment())?;
            }
            write!(f, "]")?;
        }
        IndexFilterCondition::Or(els) => {
            for (i, el) in els.iter().enumerate() {
                if i > 0 {
                    write!(f, " OR ")?;
                }
                write!(f, "(")?;
                serialize_index_filter_condition(f, el)?;
                write!(f, ")")?;
            }
        }
        IndexFilterCondition::And(els) => {
            for (i, el) in els.iter().enumerate() {
                if i > 0 {
                    write!(f, " AND ")?;
                }
                write!(f, "(")?;
                serialize_index_filter_condition(f, el)?;
                write!(f, ")")?;
            }
        }
        IndexFilterCondition::VectorExists { fid: _, embedder, filter: inner } => {
            write!(f, "_vectors")?;
            if let Some(embedder) = embedder {
                write!(f, ".{:?}", embedder.fragment())?;
            }
            match inner {
                VectorFilter::Fragment(fragment) => {
                    write!(f, ".fragments.{:?}", fragment.fragment())?
                }
                VectorFilter::DocumentTemplate => write!(f, ".documentTemplate")?,
                VectorFilter::UserProvided => write!(f, ".userProvided")?,
                VectorFilter::Regenerate => write!(f, ".regenerate")?,
                VectorFilter::None => (),
            }
            write!(f, " EXISTS")?;
        }
        IndexFilterCondition::GeoLowerThan { point, radius, resolution: None } => {
            write!(
                f,
                "_geoRadius({}, {}, {})",
                point[0].fragment(),
                point[1].fragment(),
                radius.fragment()
            )?;
        }
        IndexFilterCondition::GeoLowerThan { point, radius, resolution: Some(resolution) } => {
            write!(
                f,
                "_geoRadius({}, {}, {}, {})",
                point[0].fragment(),
                point[1].fragment(),
                radius.fragment(),
                resolution.fragment()
            )?;
        }
        IndexFilterCondition::GeoBoundingBox {
            top_right_point: top_left_point,
            bottom_left_point: bottom_right_point,
        } => {
            write!(
                f,
                "_geoBoundingBox([{}, {}], [{}, {}])",
                top_left_point[0].fragment(),
                top_left_point[1].fragment(),
                bottom_right_point[0].fragment(),
                bottom_right_point[1].fragment()
            )?;
        }
        IndexFilterCondition::GeoPolygon { points } => {
            write!(f, "_geoPolygon(")?;
            for (i, point) in points.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "[{}, {}]", point[0].fragment(), point[1].fragment())?;
            }
            write!(f, ")")?;
        }
    }

    Ok(())
}

fn serialize_condition(f: &mut impl FmtWrite, condition: &Condition<'_>) -> std::fmt::Result {
    match condition {
        Condition::GreaterThan(token) => write!(f, "> '{}'", token.fragment()),
        Condition::GreaterThanOrEqual(token) => write!(f, ">= '{}'", token.fragment()),
        Condition::Equal(token) => write!(f, "= '{}'", token.fragment()),
        Condition::NotEqual(token) => write!(f, "!= '{}'", token.fragment()),
        Condition::Null => write!(f, "IS NULL"),
        Condition::Empty => write!(f, "IS EMPTY"),
        Condition::Exists => write!(f, "EXISTS"),
        Condition::LowerThan(token) => write!(f, "< '{}'", token.fragment()),
        Condition::LowerThanOrEqual(token) => write!(f, "<= '{}'", token.fragment()),
        Condition::Between { from, to } => {
            write!(f, "'{}' TO '{}'", from.fragment(), to.fragment())
        }
        Condition::Contains { word, keyword: _ } => write!(f, "CONTAINS '{}'", word.fragment()),
        Condition::StartsWith { word, keyword: _ } => {
            write!(f, "STARTS WITH '{}'", word.fragment())
        }
    }
}
