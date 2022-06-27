use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::ops::Bound::{self, Excluded, Included};

use either::Either;
pub use filter_parser::{Condition, Error as FPError, FilterCondition, Span, Token};
use heed::types::DecodeIgnore;
use log::debug;
use roaring::RoaringBitmap;

use super::FacetNumberRange;
use crate::error::{Error, UserError};
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec,
};
use crate::{
    distance_between_two_points, lat_lng_to_xyz, CboRoaringBitmapCodec, FieldId, Index, Result,
};

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
                    let filterables_list = filterable_fields.iter().map(AsRef::as_ref).collect::<Vec<_>>().join(" ");

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
        let mut ands: Option<FilterCondition> = None;

        for either in array {
            match either {
                Either::Left(array) => {
                    let mut ors = None;
                    for rule in array {
                        if let Some(filter) = Self::from_str(rule.as_ref())? {
                            let condition = filter.condition;
                            ors = match ors.take() {
                                Some(ors) => {
                                    Some(FilterCondition::Or(Box::new(ors), Box::new(condition)))
                                }
                                None => Some(condition),
                            };
                        }
                    }

                    if let Some(rule) = ors {
                        ands = match ands.take() {
                            Some(ands) => {
                                Some(FilterCondition::And(Box::new(ands), Box::new(rule)))
                            }
                            None => Some(rule),
                        };
                    }
                }
                Either::Right(rule) => {
                    if let Some(filter) = Self::from_str(rule.as_ref())? {
                        let condition = filter.condition;
                        ands = match ands.take() {
                            Some(ands) => {
                                Some(FilterCondition::And(Box::new(ands), Box::new(condition)))
                            }
                            None => Some(condition),
                        };
                    }
                }
            }
        }

        if let Some(token) = ands.as_ref().and_then(|fc| fc.token_at_depth(MAX_FILTER_DEPTH)) {
            return Err(token.as_external_error(FilterError::TooDeep).into());
        }

        Ok(ands.map(|ands| Self { condition: ands }))
    }

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
    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_number_levels(
        rtxn: &heed::RoTxn,
        db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
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

        let mut left_found = None;
        let mut right_found = None;

        // We must create a custom iterator to be able to iterate over the
        // requested range as the range iterator cannot express some conditions.
        let iter = FacetNumberRange::new(rtxn, db, field_id, level, left, right)?;

        debug!("Iterating between {:?} and {:?} (level {})", left, right, level);

        for (i, result) in iter.enumerate() {
            let ((_fid, level, l, r), docids) = result?;
            debug!("{:?} to {:?} (level {}) found {} documents", l, r, level, docids.len());
            *output |= docids;
            // We save the leftest and rightest bounds we actually found at this level.
            if i == 0 {
                left_found = Some(l);
            }
            right_found = Some(r);
        }

        // Can we go deeper?
        let deeper_level = match level.checked_sub(1) {
            Some(level) => level,
            None => return Ok(()),
        };

        // We must refine the left and right bounds of this range by retrieving the
        // missing part in a deeper level.
        match left_found.zip(right_found) {
            Some((left_found, right_found)) => {
                // If the bound is satisfied we avoid calling this function again.
                if !matches!(left, Included(l) if l == left_found) {
                    let sub_right = Excluded(left_found);
                    debug!(
                        "calling left with {:?} to {:?} (level {})",
                        left, sub_right, deeper_level
                    );
                    Self::explore_facet_number_levels(
                        rtxn,
                        db,
                        field_id,
                        deeper_level,
                        left,
                        sub_right,
                        output,
                    )?;
                }
                if !matches!(right, Included(r) if r == right_found) {
                    let sub_left = Excluded(right_found);
                    debug!(
                        "calling right with {:?} to {:?} (level {})",
                        sub_left, right, deeper_level
                    );
                    Self::explore_facet_number_levels(
                        rtxn,
                        db,
                        field_id,
                        deeper_level,
                        sub_left,
                        right,
                        output,
                    )?;
                }
            }
            None => {
                // If we found nothing at this level it means that we must find
                // the same bounds but at a deeper, more precise level.
                Self::explore_facet_number_levels(
                    rtxn,
                    db,
                    field_id,
                    deeper_level,
                    left,
                    right,
                    output,
                )?;
            }
        }

        Ok(())
    }

    fn evaluate_operator(
        rtxn: &heed::RoTxn,
        index: &Index,
        numbers_db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
        strings_db: heed::Database<FacetStringLevelZeroCodec, FacetStringLevelZeroValueCodec>,
        field_id: FieldId,
        operator: &Condition<'a>,
    ) -> Result<RoaringBitmap> {
        // Make sure we always bound the ranges with the field id and the level,
        // as the facets values are all in the same database and prefixed by the
        // field id and the level.

        let (left, right) = match operator {
            Condition::GreaterThan(val) => (Excluded(val.parse()?), Included(f64::MAX)),
            Condition::GreaterThanOrEqual(val) => (Included(val.parse()?), Included(f64::MAX)),
            Condition::LowerThan(val) => (Included(f64::MIN), Excluded(val.parse()?)),
            Condition::LowerThanOrEqual(val) => (Included(f64::MIN), Included(val.parse()?)),
            Condition::Between { from, to } => (Included(from.parse()?), Included(to.parse()?)),
            Condition::Equal(val) => {
                let (_original_value, string_docids) = strings_db
                    .get(rtxn, &(field_id, &val.value().to_lowercase()))?
                    .unwrap_or_default();
                let number = val.parse::<f64>().ok();
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
                let all_numbers_ids = index.number_faceted_documents_ids(rtxn, field_id)?;
                let all_strings_ids = index.string_faceted_documents_ids(rtxn, field_id)?;
                let operator = Condition::Equal(val.clone());
                let docids = Self::evaluate_operator(
                    rtxn, index, numbers_db, strings_db, field_id, &operator,
                )?;
                return Ok((all_numbers_ids | all_strings_ids) - docids);
            }
        };

        // Ask for the biggest value that can exist for this specific field, if it exists
        // that's fine if it don't, the value just before will be returned instead.
        let biggest_level = numbers_db
            .remap_data_type::<DecodeIgnore>()
            .get_lower_than_or_equal_to(rtxn, &(field_id, u8::MAX, f64::MAX, f64::MAX))?
            .and_then(|((id, level, _, _), _)| if id == field_id { Some(level) } else { None });

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

    pub fn evaluate(&self, rtxn: &heed::RoTxn, index: &Index) -> Result<RoaringBitmap> {
        let numbers_db = index.facet_id_f64_docids;
        let strings_db = index.facet_id_string_docids;

        match &self.condition {
            FilterCondition::Condition { fid, op } => {
                let filterable_fields = index.filterable_fields(rtxn)?;

                if crate::is_faceted(fid.value(), &filterable_fields) {
                    let field_ids_map = index.fields_ids_map(rtxn)?;
                    if let Some(fid) = field_ids_map.id(fid.value()) {
                        Self::evaluate_operator(rtxn, index, numbers_db, strings_db, fid, &op)
                    } else {
                        return Ok(RoaringBitmap::new());
                    }
                } else {
                    match fid.lexeme() {
                        attribute @ "_geo" => {
                            return Err(fid.as_external_error(FilterError::BadGeo(attribute)))?;
                        }
                        attribute if attribute.starts_with("_geoPoint(") => {
                            return Err(fid.as_external_error(FilterError::BadGeo("_geoPoint")))?;
                        }
                        attribute @ "_geoDistance" => {
                            return Err(fid.as_external_error(FilterError::Reserved(attribute)))?;
                        }
                        attribute => {
                            return Err(fid.as_external_error(
                                FilterError::AttributeNotFilterable {
                                    attribute,
                                    filterable_fields,
                                },
                            ))?;
                        }
                    }
                }
            }
            FilterCondition::Or(lhs, rhs) => {
                let lhs = Self::evaluate(&(lhs.as_ref().clone()).into(), rtxn, index)?;
                let rhs = Self::evaluate(&(rhs.as_ref().clone()).into(), rtxn, index)?;
                Ok(lhs | rhs)
            }
            FilterCondition::And(lhs, rhs) => {
                let lhs = Self::evaluate(&(lhs.as_ref().clone()).into(), rtxn, index)?;
                let rhs = Self::evaluate(&(rhs.as_ref().clone()).into(), rtxn, index)?;
                Ok(lhs & rhs)
            }
            FilterCondition::GeoLowerThan { point, radius } => {
                let filterable_fields = index.filterable_fields(rtxn)?;
                if filterable_fields.contains("_geo") {
                    let base_point: [f64; 2] = [point[0].parse()?, point[1].parse()?];
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
                    let radius = radius.parse()?;
                    let rtree = match index.geo_rtree(rtxn)? {
                        Some(rtree) => rtree,
                        None => return Ok(RoaringBitmap::new()),
                    };

                    let xyz_base_point = lat_lng_to_xyz(&base_point);

                    let result = rtree
                        .nearest_neighbor_iter(&xyz_base_point)
                        .take_while(|point| {
                            distance_between_two_points(&base_point, &point.data.1) < radius
                        })
                        .map(|point| point.data.0)
                        .collect();

                    Ok(result)
                } else {
                    return Err(point[0].as_external_error(FilterError::AttributeNotFilterable {
                        attribute: "_geo",
                        filterable_fields,
                    }))?;
                }
            }
            FilterCondition::GeoGreaterThan { point, radius } => {
                let result = Self::evaluate(
                    &FilterCondition::GeoLowerThan { point: point.clone(), radius: radius.clone() }
                        .into(),
                    rtxn,
                    index,
                )?;
                let geo_faceted_doc_ids = index.geo_faceted_documents_ids(rtxn)?;
                Ok(geo_faceted_doc_ids - result)
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
    use heed::EnvOpenOptions;
    use maplit::hashset;

    use super::*;
    use crate::update::{self, IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
    use crate::Index;

    #[test]
    fn empty_db() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the filterable fields to be the channel.
        let config = IndexerConfig::default();
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_searchable_fields(vec![S("PrIcE")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("PrIcE") });
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

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
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

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

        let config = IndexerConfig::default();
        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_searchable_fields(vec![S("title")]);
        builder.set_filterable_fields(hashset! { S("title") });
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

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
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = documents!([
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
        ]);

        let config = IndexerConfig::default();
        let indexing_config = IndexDocumentsConfig::default();
        let mut builder =
            IndexDocuments::new(&mut wtxn, &index, &config, indexing_config.clone(), |_| ())
                .unwrap();
        builder.add_documents(content).unwrap();
        builder.execute().unwrap();

        wtxn.commit().unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let mut builder = update::Settings::new(&mut wtxn, &index, &config);

        builder.set_filterable_fields(hashset!(S("monitor_diagonal")));
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        let mut search = crate::Search::new(&rtxn, &index);
        // this filter is copy pasted from #2380 with the exact same espace sequence
        search.filter(
            crate::Filter::from_str("monitor_diagonal = '27\" to 30\\''").unwrap().unwrap(),
        );
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);

        search.filter(
            crate::Filter::from_str(r#"monitor_diagonal = "27' to 30'" "#).unwrap().unwrap(),
        );
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![0]);

        search.filter(
            crate::Filter::from_str(r#"monitor_diagonal = "27\" to 30\"" "#).unwrap().unwrap(),
        );
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![1]);

        search.filter(
            crate::Filter::from_str(r#"monitor_diagonal = "27\" to 30'" "#).unwrap().unwrap(),
        );
        let crate::SearchResult { documents_ids, .. } = search.execute().unwrap();
        assert_eq!(documents_ids, vec![2]);
    }

    #[test]
    fn geo_radius_error() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let config = IndexerConfig::default();
        // Set the filterable fields to be the channel.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index, &config);
        builder.set_searchable_fields(vec![S("_geo"), S("price")]); // to keep the fields order
        builder.set_filterable_fields(hashset! { S("_geo"), S("price") });
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

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

        let error = Filter::from_str(&filter_string).unwrap_err();
        assert!(
            error.to_string().starts_with("Too many filter conditions"),
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn empty_filter() {
        let option = Filter::from_str("     ").unwrap();
        assert_eq!(option, None);
    }
}
