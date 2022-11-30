use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::ops::Bound::{self, Excluded, Included};

use either::Either;
pub use filter_parser::{Condition, Error as FPError, FilterCondition, Span, Token};
use heed::types::DecodeIgnore;
use roaring::RoaringBitmap;

use super::facet_range_search;
use crate::error::{Error, UserError};
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec, OrderedF64Codec,
};
use crate::{distance_between_two_points, lat_lng_to_xyz, FieldId, Index, Result};

/// The maximum number of filters the filter AST can process.
const MAX_FILTER_DEPTH: usize = 2000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter<'a> {
    condition: FilterCondition<'a>,
}

#[derive(Debug)]
enum FilterError<'a> {
    AttributeNotFilterable { attribute: &'a str, filterable_fields: HashSet<String> },
    BadGeo(&'a str),
    BadGeoLat(f64),
    BadGeoLng(f64),
    Reserved(&'a str),
    TooDeep,
}
impl<'a> std::error::Error for FilterError<'a> {}

impl<'a> Display for FilterError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AttributeNotFilterable { attribute, filterable_fields } => {
                if filterable_fields.is_empty() {
                    write!(
                        f,
                        "Attribute `{}` is not filterable. This index does not have configured filterable attributes.",
                        attribute,
                    )
                } else {
                    let filterables_list = filterable_fields.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(" ");

                    write!(
                        f,
                        "Attribute `{}` is not filterable. Available filterable attributes are: `{}`.",
                        attribute,
                        filterables_list,
                    )
                }
            },
            Self::TooDeep => write!(f,
                "Too many filter conditions, can't process more than {} filters.",
                MAX_FILTER_DEPTH
            ),
            Self::Reserved(keyword) => write!(
                f,
                "`{}` is a reserved keyword and thus can't be used as a filter expression.",
                keyword
            ),
            Self::BadGeo(keyword) => write!(f, "`{}` is a reserved keyword and thus can't be used as a filter expression. Use the _geoRadius(latitude, longitude, distance) built-in rule to filter on _geo field coordinates.", keyword),
            Self::BadGeoLat(lat) => write!(f, "Bad latitude `{}`. Latitude must be contained between -90 and 90 degrees. ", lat),
            Self::BadGeoLng(lng) => write!(f, "Bad longitude `{}`. Longitude must be contained between -180 and 180 degrees. ", lng),
        }
    }
}

impl<'a> From<FPError<'a>> for Error {
    fn from(error: FPError<'a>) -> Self {
        Self::UserError(UserError::InvalidFilter(error.to_string()))
    }
}

impl<'a> From<Filter<'a>> for FilterCondition<'a> {
    fn from(f: Filter<'a>) -> Self {
        f.condition
    }
}

impl<'a> Filter<'a> {
    pub fn from_array<I, J>(array: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = Either<J, &'a str>>,
        J: IntoIterator<Item = &'a str>,
    {
        let mut ands = vec![];

        for either in array {
            match either {
                Either::Left(array) => {
                    let mut ors = vec![];
                    for rule in array {
                        if let Some(filter) = Self::from_str(rule)? {
                            ors.push(filter.condition);
                        }
                    }

                    match ors.len() {
                        0 => (),
                        1 => ands.push(ors.pop().unwrap()),
                        _ => ands.push(FilterCondition::Or(ors)),
                    }
                }
                Either::Right(rule) => {
                    if let Some(filter) = Self::from_str(rule)? {
                        ands.push(filter.condition);
                    }
                }
            }
        }
        let and = if ands.is_empty() {
            return Ok(None);
        } else if ands.len() == 1 {
            ands.pop().unwrap()
        } else {
            FilterCondition::And(ands)
        };

        if let Some(token) = and.token_at_depth(MAX_FILTER_DEPTH) {
            return Err(token.as_external_error(FilterError::TooDeep).into());
        }

        Ok(Some(Self { condition: and }))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expression: &'a str) -> Result<Option<Self>> {
        let condition = match FilterCondition::parse(expression) {
            Ok(Some(fc)) => Ok(fc),
            Ok(None) => return Ok(None),
            Err(e) => Err(Error::UserError(UserError::InvalidFilter(e.to_string()))),
        }?;

        if let Some(token) = condition.token_at_depth(MAX_FILTER_DEPTH) {
            return Err(token.as_external_error(FilterError::TooDeep).into());
        }

        Ok(Some(Self { condition }))
    }
}

impl<'a> Filter<'a> {
    pub fn evaluate(&self, rtxn: &heed::RoTxn, index: &Index) -> Result<RoaringBitmap> {
        // to avoid doing this for each recursive call we're going to do it ONCE ahead of time
        let soft_deleted_documents = index.soft_deleted_documents_ids(rtxn)?;
        let filterable_fields = index.filterable_fields(rtxn)?;

        // and finally we delete all the soft_deleted_documents, again, only once at the very end
        self.inner_evaluate(rtxn, index, &filterable_fields)
            .map(|result| result - soft_deleted_documents)
    }

    fn evaluate_operator(
        rtxn: &heed::RoTxn,
        index: &Index,
        field_id: FieldId,
        operator: &Condition<'a>,
    ) -> Result<RoaringBitmap> {
        let numbers_db = index.facet_id_f64_docids;
        let strings_db = index.facet_id_string_docids;

        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.

        let (left, right) = match operator {
            Condition::GreaterThan(val) => {
                (Excluded(val.parse_finite_float()?), Included(f64::MAX))
            }
            Condition::GreaterThanOrEqual(val) => {
                (Included(val.parse_finite_float()?), Included(f64::MAX))
            }
            Condition::LowerThan(val) => (Included(f64::MIN), Excluded(val.parse_finite_float()?)),
            Condition::LowerThanOrEqual(val) => {
                (Included(f64::MIN), Included(val.parse_finite_float()?))
            }
            Condition::Between { from, to } => {
                (Included(from.parse_finite_float()?), Included(to.parse_finite_float()?))
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
                            left_bound: &val.value().to_lowercase(),
                        },
                    )?
                    .map(|v| v.bitmap)
                    .unwrap_or_default();
                let number = val.parse_finite_float().ok();
                let number_docids = match number {
                    Some(n) => {
                        let n = Included(n);
                        let mut output = RoaringBitmap::new();
                        Self::explore_facet_number_levels(
                            rtxn,
                            numbers_db,
                            field_id,
                            0,
                            n,
                            n,
                            &mut output,
                        )?;
                        output
                    }
                    None => RoaringBitmap::new(),
                };
                return Ok(string_docids | number_docids);
            }
            Condition::NotEqual(val) => {
                let operator = Condition::Equal(val.clone());
                let docids = Self::evaluate_operator(rtxn, index, field_id, &operator)?;
                let all_ids = index.documents_ids(rtxn)?;
                return Ok(all_ids - docids);
            }
        };

        // Ask for the biggest value that can exist for this specific field, if it exists
        // that's fine if it don't, the value just before will be returned instead.
        let biggest_level = numbers_db
            .remap_data_type::<DecodeIgnore>()
            .get_lower_than_or_equal_to(
                rtxn,
                &FacetGroupKey { field_id, level: u8::MAX, left_bound: f64::MAX },
            )?
            .and_then(
                |(FacetGroupKey { field_id: id, level, .. }, _)| {
                    if id == field_id {
                        Some(level)
                    } else {
                        None
                    }
                },
            );

        match biggest_level {
            Some(level) => {
                let mut output = RoaringBitmap::new();
                Self::explore_facet_number_levels(
                    rtxn,
                    numbers_db,
                    field_id,
                    level,
                    left,
                    right,
                    &mut output,
                )?;
                Ok(output)
            }
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_number_levels(
        rtxn: &heed::RoTxn,
        db: heed::Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<f64>,
        right: Bound<f64>,
        output: &mut RoaringBitmap,
    ) -> Result<()> {
        match (left, right) {
            // If the request is an exact value we must go directly to the deepest level.
            (Included(l), Included(r)) if l == r && level > 0 => {
                return Self::explore_facet_number_levels(
                    rtxn, db, field_id, 0, left, right, output,
                );
            }
            // lower TO upper when lower > upper must return no result
            (Included(l), Included(r)) if l > r => return Ok(()),
            (Included(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Included(r)) if l >= r => return Ok(()),
            (_, _) => (),
        }
        facet_range_search::find_docids_of_facet_within_bounds::<OrderedF64Codec>(
            rtxn, db, field_id, &left, &right, output,
        )?;

        Ok(())
    }

    fn inner_evaluate(
        &self,
        rtxn: &heed::RoTxn,
        index: &Index,
        filterable_fields: &HashSet<String>,
    ) -> Result<RoaringBitmap> {
        match &self.condition {
            FilterCondition::Not(f) => {
                let all_ids = index.documents_ids(rtxn)?;
                let selected = Self::inner_evaluate(
                    &(f.as_ref().clone()).into(),
                    rtxn,
                    index,
                    filterable_fields,
                )?;
                Ok(all_ids - selected)
            }
            FilterCondition::In { fid, els } => {
                if crate::is_faceted(fid.value(), filterable_fields) {
                    let field_ids_map = index.fields_ids_map(rtxn)?;

                    if let Some(fid) = field_ids_map.id(fid.value()) {
                        let mut bitmap = RoaringBitmap::new();

                        for el in els {
                            let op = Condition::Equal(el.clone());
                            let el_bitmap = Self::evaluate_operator(rtxn, index, fid, &op)?;
                            bitmap |= el_bitmap;
                        }
                        Ok(bitmap)
                    } else {
                        Ok(RoaringBitmap::new())
                    }
                } else {
                    Err(fid.as_external_error(FilterError::AttributeNotFilterable {
                        attribute: fid.value(),
                        filterable_fields: filterable_fields.clone(),
                    }))?
                }
            }
            FilterCondition::Condition { fid, op } => {
                if crate::is_faceted(fid.value(), filterable_fields) {
                    let field_ids_map = index.fields_ids_map(rtxn)?;
                    if let Some(fid) = field_ids_map.id(fid.value()) {
                        Self::evaluate_operator(rtxn, index, fid, op)
                    } else {
                        Ok(RoaringBitmap::new())
                    }
                } else {
                    match fid.lexeme() {
                        attribute @ "_geo" => {
                            Err(fid.as_external_error(FilterError::BadGeo(attribute)))?
                        }
                        attribute if attribute.starts_with("_geoPoint(") => {
                            Err(fid.as_external_error(FilterError::BadGeo("_geoPoint")))?
                        }
                        attribute @ "_geoDistance" => {
                            Err(fid.as_external_error(FilterError::Reserved(attribute)))?
                        }
                        attribute => {
                            Err(fid.as_external_error(FilterError::AttributeNotFilterable {
                                attribute,
                                filterable_fields: filterable_fields.clone(),
                            }))?
                        }
                    }
                }
            }
            FilterCondition::Or(subfilters) => {
                let mut bitmap = RoaringBitmap::new();
                for f in subfilters {
                    bitmap |=
                        Self::inner_evaluate(&(f.clone()).into(), rtxn, index, filterable_fields)?;
                }
                Ok(bitmap)
            }
            FilterCondition::And(subfilters) => {
                let mut subfilters_iter = subfilters.iter();
                if let Some(first_subfilter) = subfilters_iter.next() {
                    let mut bitmap = Self::inner_evaluate(
                        &(first_subfilter.clone()).into(),
                        rtxn,
                        index,
                        filterable_fields,
                    )?;
                    for f in subfilters_iter {
                        if bitmap.is_empty() {
                            return Ok(bitmap);
                        }
                        bitmap &= Self::inner_evaluate(
                            &(f.clone()).into(),
                            rtxn,
                            index,
                            filterable_fields,
                        )?;
                    }
                    Ok(bitmap)
                } else {
                    Ok(RoaringBitmap::new())
                }
            }
            FilterCondition::GeoLowerThan { point, radius } => {
                if filterable_fields.contains("_geo") {
                    let base_point: [f64; 2] =
                        [point[0].parse_finite_float()?, point[1].parse_finite_float()?];
                    if !(-90.0..=90.0).contains(&base_point[0]) {
                        return Err(
                            point[0].as_external_error(FilterError::BadGeoLat(base_point[0]))
                        )?;
                    }
                    if !(-180.0..=180.0).contains(&base_point[1]) {
                        return Err(
                            point[1].as_external_error(FilterError::BadGeoLng(base_point[1]))
                        )?;
                    }
                    let radius = radius.parse_finite_float()?;
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

                    Ok(result)
                } else {
                    Err(point[0].as_external_error(FilterError::AttributeNotFilterable {
                        attribute: "_geo",
                        filterable_fields: filterable_fields.clone(),
                    }))?
                }
            }
        }
    }
}

impl<'a> From<FilterCondition<'a>> for Filter<'a> {
    fn from(fc: FilterCondition<'a>) -> Self {
        Self { condition: fc }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use big_s::S;
    use either::Either;
    use maplit::hashset;

    use crate::index::tests::TempIndex;
    use crate::Filter;

    #[test]
    fn empty_db() {
        let index = TempIndex::new();
        //Set the filterable fields to be the channel.
        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset! { S("PrIcE") });
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        let filter = Filter::from_str("PrIcE < 1000").unwrap().unwrap();
        let bitmap = filter.evaluate(&rtxn, &index).unwrap();
        assert!(bitmap.is_empty());

        let filter = Filter::from_str("NOT PrIcE >= 1000").unwrap().unwrap();
        let bitmap = filter.evaluate(&rtxn, &index).unwrap();
        assert!(bitmap.is_empty());
    }

    #[test]
    fn from_array() {
        // Simple array with Left
        let condition = Filter::from_array(vec![Either::Left(["channel = mv"])]).unwrap().unwrap();
        let expected = Filter::from_str("channel = mv").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Simple array with Right
        let condition = Filter::from_array::<_, Option<&str>>(vec![Either::Right("channel = mv")])
            .unwrap()
            .unwrap();
        let expected = Filter::from_str("channel = mv").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Array with Left and escaped quote
        let condition =
            Filter::from_array(vec![Either::Left(["channel = \"Mister Mv\""])]).unwrap().unwrap();
        let expected = Filter::from_str("channel = \"Mister Mv\"").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Array with Right and escaped quote
        let condition =
            Filter::from_array::<_, Option<&str>>(vec![Either::Right("channel = \"Mister Mv\"")])
                .unwrap()
                .unwrap();
        let expected = Filter::from_str("channel = \"Mister Mv\"").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Array with Left and escaped simple quote
        let condition =
            Filter::from_array(vec![Either::Left(["channel = 'Mister Mv'"])]).unwrap().unwrap();
        let expected = Filter::from_str("channel = 'Mister Mv'").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Array with Right and escaped simple quote
        let condition =
            Filter::from_array::<_, Option<&str>>(vec![Either::Right("channel = 'Mister Mv'")])
                .unwrap()
                .unwrap();
        let expected = Filter::from_str("channel = 'Mister Mv'").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Simple with parenthesis
        let condition =
            Filter::from_array(vec![Either::Left(["(channel = mv)"])]).unwrap().unwrap();
        let expected = Filter::from_str("(channel = mv)").unwrap().unwrap();
        assert_eq!(condition, expected);

        // Test that the facet condition is correctly generated.
        let condition = Filter::from_array(vec![
            Either::Right("channel = gotaga"),
            Either::Left(vec!["timestamp = 44", "channel != ponce"]),
        ])
        .unwrap()
        .unwrap();
        let expected =
            Filter::from_str("channel = gotaga AND (timestamp = 44 OR channel != ponce)")
                .unwrap()
                .unwrap();
        assert_eq!(condition, expected);
    }

    #[test]
    fn not_filterable() {
        let index = TempIndex::new();

        let rtxn = index.read_txn().unwrap();
        let filter = Filter::from_str("_geoRadius(42, 150, 10)").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(error.to_string().starts_with(
            "Attribute `_geo` is not filterable. This index does not have configured filterable attributes."
        ));

        let filter = Filter::from_str("dog = \"bernese mountain\"").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(error.to_string().starts_with(
            "Attribute `dog` is not filterable. This index does not have configured filterable attributes."
        ));
        drop(rtxn);

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("title")]);
                settings.set_filterable_fields(hashset! { S("title") });
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        let filter = Filter::from_str("_geoRadius(-100, 150, 10)").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(error.to_string().starts_with(
            "Attribute `_geo` is not filterable. Available filterable attributes are: `title`."
        ));

        let filter = Filter::from_str("name = 12").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(error.to_string().starts_with(
            "Attribute `name` is not filterable. Available filterable attributes are: `title`."
        ));
    }

    #[test]
    fn escaped_quote_in_filter_value_2380() {
        let index = TempIndex::new();

        index
            .add_documents(documents!([
                {
                    "id": "test_1",
                    "monitor_diagonal": "27' to 30'"
                },
                {
                    "id": "test_2",
                    "monitor_diagonal": "27\" to 30\""
                },
                {
                    "id": "test_3",
                    "monitor_diagonal": "27\" to 30'"
                },
            ]))
            .unwrap();

        index
            .update_settings(|settings| {
                settings.set_filterable_fields(hashset!(S("monitor_diagonal")));
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        let mut search = crate::Search::new(&rtxn, &index);
        // this filter is copy pasted from #2380 with the exact same espace sequence
        search.filter(Filter::from_str("monitor_diagonal = '27\" to 30\\''").unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);

        search.filter(Filter::from_str(r#"monitor_diagonal = "27' to 30'" "#).unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![0]);

        search.filter(Filter::from_str(r#"monitor_diagonal = "27\" to 30\"" "#).unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1]);

        search.filter(Filter::from_str(r#"monitor_diagonal = "27\" to 30'" "#).unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);
    }

    #[test]
    fn zero_radius() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("_geo")]);
                settings.set_filterable_fields(hashset! { S("_geo") });
            })
            .unwrap();

        index
            .add_documents(documents!([
              {
                "id": 1,
                "name": "Nàpiz' Milano",
                "address": "Viale Vittorio Veneto, 30, 20124, Milan, Italy",
                "type": "pizza",
                "rating": 9,
                "_geo": {
                  "lat": 45.4777599,
                  "lng": 9.1967508
                }
              },
              {
                "id": 2,
                "name": "Artico Gelateria Tradizionale",
                "address": "Via Dogana, 1, 20123 Milan, Italy",
                "type": "ice cream",
                "rating": 10,
                "_geo": {
                  "lat": 45.4632046,
                  "lng": 9.1719421
                }
              },
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        let mut search = crate::Search::new(&rtxn, &index);

        search.filter(Filter::from_str("_geoRadius(45.4777599, 9.1967508, 0)").unwrap().unwrap());
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![0]);
    }

    #[test]
    fn geo_radius_error() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("_geo"), S("price")]); // to keep the fields order
                settings.set_filterable_fields(hashset! { S("_geo"), S("price") });
            })
            .unwrap();

        let rtxn = index.read_txn().unwrap();

        // georadius have a bad latitude
        let filter = Filter::from_str("_geoRadius(-100, 150, 10)").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(
            error.to_string().starts_with(
                "Bad latitude `-100`. Latitude must be contained between -90 and 90 degrees."
            ),
            "{}",
            error.to_string()
        );

        // georadius have a bad latitude
        let filter = Filter::from_str("_geoRadius(-90.0000001, 150, 10)").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(error.to_string().contains(
            "Bad latitude `-90.0000001`. Latitude must be contained between -90 and 90 degrees."
        ));

        // georadius have a bad longitude
        let filter = Filter::from_str("_geoRadius(-10, 250, 10)").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(
            error.to_string().contains(
                "Bad longitude `250`. Longitude must be contained between -180 and 180 degrees."
            ),
            "{}",
            error.to_string(),
        );

        // georadius have a bad longitude
        let filter = Filter::from_str("_geoRadius(-10, 180.000001, 10)").unwrap().unwrap();
        let error = filter.evaluate(&rtxn, &index).unwrap_err();
        assert!(error.to_string().contains(
            "Bad longitude `180.000001`. Longitude must be contained between -180 and 180 degrees."
        ));
    }

    #[test]
    fn filter_depth() {
        // generates a big (2 MiB) filter with too much of ORs.
        let tipic_filter = "account_ids=14361 OR ";
        let mut filter_string = String::with_capacity(tipic_filter.len() * 14360);
        for i in 1..=14361 {
            let _ = write!(&mut filter_string, "account_ids={}", i);
            if i != 14361 {
                let _ = write!(&mut filter_string, " OR ");
            }
        }

        // Note: the filter used to be rejected for being too deep, but that is
        // no longer the case
        let filter = Filter::from_str(&filter_string).unwrap();
        assert!(filter.is_some());
    }

    #[test]
    fn empty_filter() {
        let option = Filter::from_str("     ").unwrap();
        assert_eq!(option, None);
    }

    #[test]
    fn non_finite_float() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("price")]); // to keep the fields order
                settings.set_filterable_fields(hashset! { S("price") });
            })
            .unwrap();
        index
            .add_documents(documents!([
                {
                    "id": "test_1",
                    "price": "inf"
                },
                {
                    "id": "test_2",
                    "price": "2000"
                },
                {
                    "id": "test_3",
                    "price": "infinity"
                },
            ]))
            .unwrap();

        let rtxn = index.read_txn().unwrap();
        let filter = Filter::from_str("price = inf").unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert!(result.contains(0));
        let filter = Filter::from_str("price < inf").unwrap().unwrap();
        assert!(matches!(
            filter.evaluate(&rtxn, &index),
            Err(crate::Error::UserError(crate::error::UserError::InvalidFilter(_)))
        ));

        let filter = Filter::from_str("price = NaN").unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert!(result.is_empty());
        let filter = Filter::from_str("price < NaN").unwrap().unwrap();
        assert!(matches!(
            filter.evaluate(&rtxn, &index),
            Err(crate::Error::UserError(crate::error::UserError::InvalidFilter(_)))
        ));

        let filter = Filter::from_str("price = infinity").unwrap().unwrap();
        let result = filter.evaluate(&rtxn, &index).unwrap();
        assert!(result.contains(2));
        let filter = Filter::from_str("price < infinity").unwrap().unwrap();
        assert!(matches!(
            filter.evaluate(&rtxn, &index),
            Err(crate::Error::UserError(crate::error::UserError::InvalidFilter(_)))
        ));
    }
}
